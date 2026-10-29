//! HTTP routes of the sync API (spec/sync-api.openapi.yaml). Every rule lives in `hub.rs`; this maps requests and
//! responses, enforces body limits and the local-network check.

use std::convert::Infallible;
use std::net::SocketAddr;

use axum::body::{Body, Bytes};
use axum::extract::rejection::BytesRejection;
use axum::extract::{ConnectInfo, DefaultBodyLimit, FromRequestParts, Path, Query, Request, State};
use axum::http::header::{self, HeaderMap, HeaderName, HeaderValue};
use axum::http::request::Parts;
use axum::http::StatusCode;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::Router;
use serde::Deserialize;
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tower::limit::GlobalConcurrencyLimitLayer;
use tower_http::decompression::RequestDecompressionLayer;
use tower_http::timeout::RequestBodyTimeoutLayer;

use crate::error::{ApiError, ApiResult};
use crate::hub::{audio_etag, Caller, Hub, Precondition, Write, MAX_UPLOAD_CHUNK};

const SMALL_BODY: usize = 64 * 1024;
const LARGE_BODIES_AT_ONCE: usize = 4;
const BODY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);
const TRANSCRIPT_BODY: usize = 20 * 1024 * 1024;
const UPLOAD_OFFSET: HeaderName = HeaderName::from_static("upload-offset");
const UPLOAD_COMPLETE: HeaderName = HeaderName::from_static("upload-complete");

pub fn router(hub: Hub) -> Router {
    let small = DefaultBodyLimit::max(SMALL_BODY);
    // Large bodies are buffered and parsed in memory: only a few at a time.
    let large = GlobalConcurrencyLimitLayer::new(LARGE_BODIES_AT_ONCE);
    Router::new()
        .route("/v1/hello", get(hello))
        .route("/v1/pair", post(pair).layer(small))
        .route(
            "/v1/devices/me",
            get(device_me)
                .patch(device_me_update)
                .delete(device_me_delete)
                .layer(small),
        )
        .route("/v1/changes", get(changes))
        .route(
            "/v1/meetings/{id}",
            get(meeting_get)
                .put(meeting_put)
                .delete(meeting_delete)
                .layer(small),
        )
        .route(
            "/v1/meetings/{id}/transcript",
            get(transcript_get)
                .put(transcript_put)
                .layer::<_, Infallible>(DefaultBodyLimit::max(TRANSCRIPT_BODY))
                .layer::<_, Infallible>(large.clone()),
        )
        .route("/v1/meetings/{id}/audio", get(audio_get))
        .route(
            "/v1/meetings/{id}/audio/uploads",
            post(upload_start).layer(small),
        )
        .route(
            "/v1/uploads/{id}",
            axum::routing::head(upload_head)
                .patch(upload_append)
                .layer::<_, Infallible>(DefaultBodyLimit::max(MAX_UPLOAD_CHUNK))
                .layer::<_, Infallible>(large),
        )
        .fallback(|| async { ApiError::not_found() })
        // Limits apply to the decompressed body (the decompression layer runs first).
        .layer(RequestDecompressionLayer::new())
        // A body must arrive within this time, however slowly it trickles in.
        .layer(RequestBodyTimeoutLayer::new(BODY_TIMEOUT))
        .layer(middleware::from_fn_with_state(
            hub.clone(),
            local_network_only,
        ))
        .with_state(hub)
}

async fn local_network_only(State(hub): State<Hub>, request: Request, next: Next) -> Response {
    let peer = request
        .extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|c| c.0.ip());
    match peer {
        Some(ip) if hub.netfilter().allows(ip) => next.run(request).await,
        _ => ApiError::forbidden_network().into_response(),
    }
}

/// The caller, from `Authorization: Bearer <token>`.
pub struct Authed(pub Caller);

fn bearer(headers: &HeaderMap) -> Option<&str> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.strip_prefix("Bearer ")
                .or_else(|| v.strip_prefix("bearer "))
        })
        .map(str::trim)
        .filter(|t| !t.is_empty() && t.len() <= 128)
}

impl FromRequestParts<Hub> for Authed {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, hub: &Hub) -> Result<Self, Self::Rejection> {
        let token = bearer(&parts.headers).ok_or_else(ApiError::unauthorized)?;
        Ok(Authed(hub.authenticate(token).await?))
    }
}

fn json_body(body: Result<Bytes, BytesRejection>) -> ApiResult<Value> {
    let bytes = body.map_err(|e| {
        if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
            ApiError::payload_too_large()
        } else {
            ApiError::bad_request(e.body_text())
        }
    })?;
    serde_json::from_slice(&bytes).map_err(|e| ApiError::bad_request(format!("invalid JSON: {e}")))
}

fn json_response(status: StatusCode, value: &Value, etag: Option<&str>) -> Response {
    let mut response = (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        serde_json::to_vec(value).unwrap_or_default(),
    )
        .into_response();
    if let Some(etag) = etag.and_then(|e| HeaderValue::from_str(e).ok()) {
        response.headers_mut().insert(header::ETAG, etag);
    }
    response
}

fn write_response(write: Write) -> Response {
    match write {
        Write::Created { doc, etag } => json_response(StatusCode::CREATED, &doc, Some(&etag)),
        Write::Updated { doc, etag } => json_response(StatusCode::OK, &doc, Some(&etag)),
        Write::Conflict { doc, etag } => {
            json_response(StatusCode::PRECONDITION_FAILED, &doc, Some(&etag))
        }
    }
}

fn precondition(headers: &HeaderMap) -> ApiResult<Precondition> {
    let get = |name: HeaderName| headers.get(name).and_then(|v| v.to_str().ok());
    Precondition::from_headers(get(header::IF_MATCH), get(header::IF_NONE_MATCH))
}

async fn hello(State(hub): State<Hub>) -> Response {
    json_response(StatusCode::OK, &hub.hello(), None)
}

async fn pair(
    State(hub): State<Hub>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult<Response> {
    let body = json_body(body)?;
    // Pairing an already-paired device again needs that device's current token.
    let paired = hub.pair(peer.ip(), &body, bearer(&headers)).await?;
    Ok(json_response(StatusCode::CREATED, &paired, None))
}

async fn device_me(State(hub): State<Hub>, Authed(caller): Authed) -> ApiResult<Response> {
    Ok(json_response(
        StatusCode::OK,
        &hub.own_device(&caller).await?,
        None,
    ))
}

async fn device_me_update(
    State(hub): State<Hub>,
    Authed(caller): Authed,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult<Response> {
    let body = json_body(body)?;
    Ok(json_response(
        StatusCode::OK,
        &hub.update_own_device(&caller, &body).await?,
        None,
    ))
}

async fn device_me_delete(State(hub): State<Hub>, Authed(caller): Authed) -> ApiResult<StatusCode> {
    hub.unpair(&caller).await?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize)]
struct ChangesQuery {
    since: Option<String>,
    limit: Option<i64>,
    wait: Option<u64>,
}

async fn changes(
    State(hub): State<Hub>,
    Authed(caller): Authed,
    query: Result<Query<ChangesQuery>, axum::extract::rejection::QueryRejection>,
) -> ApiResult<Response> {
    let Query(q) = query.map_err(|e| ApiError::bad_request(e.body_text()))?;
    let page = hub
        .changes(&caller, q.since.as_deref(), q.limit, q.wait)
        .await?;
    Ok(json_response(StatusCode::OK, &page, None))
}

async fn meeting_get(
    State(hub): State<Hub>,
    Authed(caller): Authed,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    let (doc, etag) = hub.get_meeting(&caller, &id).await?;
    Ok(json_response(StatusCode::OK, &doc, Some(&etag)))
}

async fn meeting_put(
    State(hub): State<Hub>,
    Authed(caller): Authed,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult<Response> {
    let pre = precondition(&headers)?;
    let body = json_body(body)?;
    Ok(write_response(
        hub.put_meeting(&caller, &id, body, pre).await?,
    ))
}

async fn meeting_delete(
    State(hub): State<Hub>,
    Authed(caller): Authed,
    Path(id): Path<String>,
) -> ApiResult<StatusCode> {
    hub.delete_meeting(&caller, &id).await?;
    Ok(StatusCode::NO_CONTENT)
}

async fn transcript_get(
    State(hub): State<Hub>,
    Authed(caller): Authed,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    let (doc, etag) = hub.get_transcript(&caller, &id).await?;
    Ok(json_response(StatusCode::OK, &doc, Some(&etag)))
}

async fn transcript_put(
    State(hub): State<Hub>,
    Authed(caller): Authed,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult<Response> {
    let pre = precondition(&headers)?;
    let body = json_body(body)?;
    Ok(write_response(
        hub.put_transcript(&caller, &id, body, pre).await?,
    ))
}

async fn upload_start(
    State(hub): State<Hub>,
    Authed(caller): Authed,
    Path(id): Path<String>,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult<Response> {
    let body = json_body(body)?;
    let (status, state) = hub.start_upload(&caller, &id, &body).await?;
    Ok(json_response(status, &state, None))
}

fn upload_headers(offset: i64, complete: bool) -> [(HeaderName, HeaderValue); 2] {
    [
        (UPLOAD_OFFSET, HeaderValue::from(offset)),
        (
            UPLOAD_COMPLETE,
            HeaderValue::from_static(if complete { "true" } else { "false" }),
        ),
    ]
}

async fn upload_head(
    State(hub): State<Hub>,
    Authed(caller): Authed,
    Path(id): Path<String>,
) -> ApiResult<Response> {
    let state = hub.upload_state(&caller, &id).await?;
    Ok((
        StatusCode::NO_CONTENT,
        upload_headers(state.offset, state.complete),
    )
        .into_response())
}

async fn upload_append(
    State(hub): State<Hub>,
    Authed(caller): Authed,
    Path(id): Path<String>,
    headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> ApiResult<Response> {
    let offset = headers
        .get(&UPLOAD_OFFSET)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|o| *o >= 0)
        .ok_or_else(|| ApiError::bad_request("Upload-Offset header is required"))?;
    let bytes = body.map_err(|e| {
        if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
            ApiError::payload_too_large().detail("at most 16 MiB per request")
        } else {
            ApiError::bad_request(e.body_text())
        }
    })?;
    let state = hub.append_upload(&caller, &id, offset, &bytes).await?;
    Ok((
        StatusCode::NO_CONTENT,
        upload_headers(state.offset, state.complete),
    )
        .into_response())
}

/// `bytes=a-b`, `bytes=a-` or `bytes=-n`; one range only.
fn parse_range(value: &str, size: u64) -> Option<(u64, u64)> {
    let spec = value.trim().strip_prefix("bytes=")?;
    if spec.contains(',') || size == 0 {
        return None;
    }
    let (a, b) = spec.split_once('-')?;
    let (start, end) = if a.is_empty() {
        let n: u64 = b.parse().ok()?;
        (size.saturating_sub(n), size - 1)
    } else {
        let start: u64 = a.parse().ok()?;
        let end = if b.is_empty() {
            size - 1
        } else {
            b.parse::<u64>().ok()?.min(size - 1)
        };
        (start, end)
    };
    (start <= end && start < size).then_some((start, end))
}

async fn audio_get(
    State(hub): State<Hub>,
    Authed(caller): Authed,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> ApiResult<Response> {
    let file = hub.audio_file(&caller, &id).await?;
    let etag = audio_etag(&file.sha256);
    let if_range_ok = headers
        .get(header::IF_RANGE)
        .and_then(|v| v.to_str().ok())
        .is_none_or(|v| v.trim() == etag);
    let range = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .filter(|_| if_range_ok)
        .map(|r| parse_range(r, file.size));
    let mut f = tokio::fs::File::open(&file.path).await?;
    let (status, start, len) = match range {
        None => (StatusCode::OK, 0, file.size),
        Some(Some((start, end))) => (StatusCode::PARTIAL_CONTENT, start, end - start + 1),
        Some(None) => {
            let mut r = StatusCode::RANGE_NOT_SATISFIABLE.into_response();
            r.headers_mut().insert(
                header::CONTENT_RANGE,
                HeaderValue::from_str(&format!("bytes */{}", file.size)).unwrap(),
            );
            return Ok(r);
        }
    };
    f.seek(std::io::SeekFrom::Start(start)).await?;
    let body = Body::from_stream(tokio_util::io::ReaderStream::new(f.take(len)));
    let mut response = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, &file.mime_type)
        .header(header::CONTENT_LENGTH, len)
        .header(header::ACCEPT_RANGES, "bytes")
        .header(header::ETAG, &etag)
        .body(body)
        .map_err(ApiError::internal)?;
    if status == StatusCode::PARTIAL_CONTENT {
        response.headers_mut().insert(
            header::CONTENT_RANGE,
            HeaderValue::from_str(&format!(
                "bytes {}-{}/{}",
                start,
                start + len - 1,
                file.size
            ))
            .unwrap(),
        );
    }
    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::parse_range;

    #[test]
    fn ranges() {
        assert_eq!(parse_range("bytes=0-99", 1000), Some((0, 99)));
        assert_eq!(parse_range("bytes=900-", 1000), Some((900, 999)));
        assert_eq!(parse_range("bytes=-100", 1000), Some((900, 999)));
        assert_eq!(parse_range("bytes=990-2000", 1000), Some((990, 999)));
        assert_eq!(parse_range("bytes=1000-", 1000), None);
        assert_eq!(parse_range("bytes=0-1,5-6", 1000), None);
    }
}
