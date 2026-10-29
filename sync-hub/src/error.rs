//! Errors as RFC 9457 problem details with the stable codes from spec/sync-protocol.md ("Errors").

use axum::http::{HeaderName, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::json;

#[derive(Debug)]
pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub detail: Option<String>,
    pub headers: Vec<(HeaderName, HeaderValue)>,
}

impl ApiError {
    pub fn new(status: StatusCode, code: &'static str) -> Self {
        Self {
            status,
            code,
            detail: None,
            headers: Vec::new(),
        }
    }

    pub fn detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn header(mut self, name: HeaderName, value: impl ToString) -> Self {
        if let Ok(v) = HeaderValue::from_str(&value.to_string()) {
            self.headers.push((name, v));
        }
        self
    }

    pub fn bad_request(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "bad_request").detail(detail)
    }
    pub fn unauthorized() -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "unauthorized")
    }
    pub fn device_revoked() -> Self {
        Self::new(StatusCode::UNAUTHORIZED, "device_revoked")
    }
    pub fn forbidden_network() -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden_network")
    }
    pub fn forbidden(detail: impl Into<String>) -> Self {
        Self::new(StatusCode::FORBIDDEN, "forbidden").detail(detail)
    }
    pub fn pairing_code_invalid() -> Self {
        Self::new(StatusCode::FORBIDDEN, "pairing_code_invalid")
    }
    pub fn not_found() -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found")
    }
    pub fn conflict(code: &'static str) -> Self {
        Self::new(StatusCode::CONFLICT, code)
    }
    pub fn cursor_expired() -> Self {
        Self::new(StatusCode::GONE, "cursor_expired")
    }
    pub fn meeting_deleted() -> Self {
        Self::new(StatusCode::GONE, "meeting_deleted")
    }
    pub fn payload_too_large() -> Self {
        Self::new(StatusCode::PAYLOAD_TOO_LARGE, "payload_too_large")
    }
    pub fn checksum_mismatch() -> Self {
        Self::new(StatusCode::UNPROCESSABLE_ENTITY, "checksum_mismatch")
    }
    pub fn precondition_required() -> Self {
        Self::new(StatusCode::PRECONDITION_REQUIRED, "precondition_required")
            .detail("send If-None-Match: * to create or If-Match: <ETag> to update")
    }
    pub fn rate_limited(retry_after_secs: u64) -> Self {
        Self::new(StatusCode::TOO_MANY_REQUESTS, "rate_limited")
            .header(axum::http::header::RETRY_AFTER, retry_after_secs)
    }
    pub fn internal(err: impl std::fmt::Display) -> Self {
        tracing::error!(target: "sync_hub", "internal error: {err}");
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal_error")
    }
}

impl From<sqlx::Error> for ApiError {
    fn from(e: sqlx::Error) -> Self {
        ApiError::internal(e)
    }
}

impl From<std::io::Error> for ApiError {
    fn from(e: std::io::Error) -> Self {
        ApiError::internal(e)
    }
}

impl From<serde_json::Error> for ApiError {
    fn from(e: serde_json::Error) -> Self {
        ApiError::internal(e)
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        ApiError::internal(e)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let title = self.status.canonical_reason().unwrap_or("Error");
        let mut body = json!({ "title": title, "status": self.status.as_u16(), "code": self.code });
        if let Some(detail) = &self.detail {
            body["detail"] = json!(detail);
        }
        let mut response =
            (self.status, serde_json::to_vec(&body).unwrap_or_default()).into_response();
        let headers = response.headers_mut();
        headers.insert(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/problem+json"),
        );
        for (name, value) in self.headers {
            headers.insert(name, value);
        }
        response
    }
}

pub type ApiResult<T> = Result<T, ApiError>;
