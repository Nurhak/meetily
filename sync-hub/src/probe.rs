//! Reads duration, sample rate and channels from an uploaded recording, to fill meeting.json → `audio`.
//! The desktop app can plug in its own prober (it already decodes audio with Symphonia); [`BasicProber`] covers
//! WAV and MP4/M4A without decoding.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use anyhow::{bail, Context, Result};

#[derive(Debug, Clone, PartialEq)]
pub struct AudioDetails {
    pub duration_sec: f64,
    pub sample_rate: Option<u32>,
    pub channels: Option<u16>,
    pub codec: Option<String>,
}

pub trait AudioProber: Send + Sync {
    fn probe(&self, path: &Path, mime_type: &str) -> Result<AudioDetails>;
}

pub struct BasicProber;

impl AudioProber for BasicProber {
    fn probe(&self, path: &Path, mime_type: &str) -> Result<AudioDetails> {
        match mime_type {
            "audio/wav" => probe_wav(path),
            "audio/mp4" => probe_mp4(path),
            other => bail!("no prober for {other}"),
        }
    }
}

fn read_u16le(b: &[u8]) -> u16 {
    u16::from_le_bytes([b[0], b[1]])
}
fn read_u32le(b: &[u8]) -> u32 {
    u32::from_le_bytes([b[0], b[1], b[2], b[3]])
}
fn read_u32be(b: &[u8]) -> u32 {
    u32::from_be_bytes([b[0], b[1], b[2], b[3]])
}
fn read_u64be(b: &[u8]) -> u64 {
    u64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]])
}

fn probe_wav(path: &Path) -> Result<AudioDetails> {
    let mut f = File::open(path)?;
    let mut header = [0u8; 12];
    f.read_exact(&mut header)?;
    if &header[0..4] != b"RIFF" || &header[8..12] != b"WAVE" {
        bail!("not a WAV file");
    }
    let (mut channels, mut sample_rate, mut byte_rate, mut format) = (0u16, 0u32, 0u32, 0u16);
    loop {
        let mut chunk = [0u8; 8];
        if f.read_exact(&mut chunk).is_err() {
            bail!("WAV without data chunk");
        }
        let size = read_u32le(&chunk[4..8]) as u64;
        match &chunk[0..4] {
            b"fmt " => {
                let mut fmt = vec![0u8; size.min(64) as usize];
                f.read_exact(&mut fmt)?;
                format = read_u16le(&fmt[0..2]);
                channels = read_u16le(&fmt[2..4]);
                sample_rate = read_u32le(&fmt[4..8]);
                byte_rate = read_u32le(&fmt[8..12]);
                f.seek(SeekFrom::Current(
                    size as i64 - fmt.len() as i64 + (size % 2) as i64,
                ))?;
            }
            b"data" => {
                if byte_rate == 0 {
                    bail!("WAV data before fmt");
                }
                let codec = match format {
                    1 => "pcm",
                    3 => "pcm_float",
                    _ => "wav",
                };
                return Ok(AudioDetails {
                    duration_sec: size as f64 / byte_rate as f64,
                    sample_rate: Some(sample_rate),
                    channels: Some(channels),
                    codec: Some(codec.into()),
                });
            }
            _ => {
                f.seek(SeekFrom::Current(size as i64 + (size % 2) as i64))?;
            }
        }
    }
}

/// Walks MP4 boxes: duration from `mvhd`, and for the audio track the sample-entry codec, channel count and
/// sample rate from `stsd`.
fn probe_mp4(path: &Path) -> Result<AudioDetails> {
    let mut f = File::open(path)?;
    let len = f.metadata()?.len();
    let mut details = AudioDetails {
        duration_sec: 0.0,
        sample_rate: None,
        channels: None,
        codec: None,
    };
    let mut found_duration = false;
    walk(&mut f, 0, len, &mut |kind, body| {
        match kind {
            b"mvhd" if body.len() >= 32 => {
                let (timescale, duration) = if body[0] == 1 {
                    (read_u32be(&body[20..24]), read_u64be(&body[24..32]))
                } else {
                    (read_u32be(&body[12..16]), read_u32be(&body[16..20]) as u64)
                };
                if timescale > 0 {
                    details.duration_sec = duration as f64 / timescale as f64;
                    found_duration = true;
                }
            }
            b"stsd" if body.len() >= 16 + 28 && details.codec.is_none() => {
                // full box (4) + entry count (4) + entry: size (4) + format (4) + reserved (6) + data ref (2)
                // + audio sample entry: reserved (8), channels (2), sample size (2), pre-defined (2),
                // reserved (2), sample rate 16.16 (4)
                let entry = &body[8..];
                let format = &entry[4..8];
                let audio = &entry[16..];
                details.codec = Some(match format {
                    b"mp4a" => "aac".to_string(),
                    b"Opus" => "opus".to_string(),
                    b"alac" => "alac".to_string(),
                    other => String::from_utf8_lossy(other).trim().to_lowercase(),
                });
                details.channels = Some(u16::from_be_bytes([audio[8], audio[9]]));
                details.sample_rate = Some(read_u32be(&audio[16..20]) >> 16);
            }
            _ => {}
        }
    })
    .context("reading MP4 boxes")?;
    if !found_duration {
        bail!("MP4 without mvhd");
    }
    Ok(details)
}

const CONTAINERS: &[&[u8; 4]] = &[b"moov", b"trak", b"mdia", b"minf", b"stbl"];
const LEAVES: &[&[u8; 4]] = &[b"mvhd", b"stsd"];

fn walk(f: &mut File, start: u64, end: u64, visit: &mut dyn FnMut(&[u8; 4], &[u8])) -> Result<()> {
    let mut pos = start;
    while pos + 8 <= end {
        f.seek(SeekFrom::Start(pos))?;
        let mut head = [0u8; 8];
        f.read_exact(&mut head)?;
        let kind: [u8; 4] = head[4..8].try_into().unwrap();
        let (size, header_len) = match read_u32be(&head[0..4]) {
            1 => {
                let mut large = [0u8; 8];
                f.read_exact(&mut large)?;
                (read_u64be(&large), 16)
            }
            0 => (end - pos, 8),
            n => (n as u64, 8),
        };
        if size < header_len || pos + size > end {
            break;
        }
        let body_start = pos + header_len;
        if CONTAINERS.contains(&&kind) {
            walk(f, body_start, pos + size, visit)?;
        } else if LEAVES.contains(&&kind) {
            let mut body = vec![0u8; (size - header_len).min(4096) as usize];
            f.read_exact(&mut body)?;
            visit(&kind, &body);
        }
        pos += size;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn wav(seconds: u32, rate: u32, channels: u16) -> Vec<u8> {
        let byte_rate = rate * channels as u32 * 2;
        let data = byte_rate * seconds;
        let mut v = Vec::new();
        v.extend(b"RIFF");
        v.extend((36 + data).to_le_bytes());
        v.extend(b"WAVEfmt ");
        v.extend(16u32.to_le_bytes());
        v.extend(1u16.to_le_bytes());
        v.extend(channels.to_le_bytes());
        v.extend(rate.to_le_bytes());
        v.extend(byte_rate.to_le_bytes());
        v.extend((channels * 2).to_le_bytes());
        v.extend(16u16.to_le_bytes());
        v.extend(b"data");
        v.extend(data.to_le_bytes());
        v.extend(vec![0u8; data as usize]);
        v
    }

    fn mp4_box(kind: &[u8; 4], body: &[u8]) -> Vec<u8> {
        let mut v = ((body.len() + 8) as u32).to_be_bytes().to_vec();
        v.extend(kind);
        v.extend(body);
        v
    }

    /// A minimal M4A: ftyp + moov(mvhd, trak(mdia(minf(stbl(stsd(mp4a)))))) + mdat.
    pub(crate) fn mp4(rate: u32, channels: u16, duration_in_timescale: u32) -> Vec<u8> {
        let mut mvhd = vec![0u8; 100];
        mvhd[12..16].copy_from_slice(&rate.to_be_bytes());
        mvhd[16..20].copy_from_slice(&duration_in_timescale.to_be_bytes());
        let mut entry = Vec::new();
        entry.extend([0u8; 6]); // reserved
        entry.extend(1u16.to_be_bytes()); // data reference index
        entry.extend([0u8; 8]); // reserved
        entry.extend(channels.to_be_bytes());
        entry.extend(16u16.to_be_bytes());
        entry.extend([0u8; 4]);
        entry.extend((rate << 16).to_be_bytes());
        let mp4a = mp4_box(b"mp4a", &entry);
        let mut stsd = vec![0u8; 4];
        stsd.extend(1u32.to_be_bytes());
        stsd.extend(mp4a);
        let stbl = mp4_box(b"stbl", &mp4_box(b"stsd", &stsd));
        let trak = mp4_box(b"trak", &mp4_box(b"mdia", &mp4_box(b"minf", &stbl)));
        let mut moov_body = mp4_box(b"mvhd", &mvhd);
        moov_body.extend(trak);
        let mut file = mp4_box(b"ftyp", b"M4A \0\0\0\0M4A isom");
        file.extend(mp4_box(b"moov", &moov_body));
        file.extend(mp4_box(b"mdat", &[0u8; 64]));
        file
    }

    #[test]
    fn wav_duration() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&wav(3, 16000, 1)).unwrap();
        let d = BasicProber.probe(f.path(), "audio/wav").unwrap();
        assert_eq!(
            d,
            AudioDetails {
                duration_sec: 3.0,
                sample_rate: Some(16000),
                channels: Some(1),
                codec: Some("pcm".into())
            }
        );
    }

    #[test]
    fn mp4_duration_and_audio_entry() {
        let mut f = tempfile::NamedTempFile::new().unwrap();
        f.write_all(&mp4(16000, 1, 16000 * 125 / 10)).unwrap();
        let d = BasicProber.probe(f.path(), "audio/mp4").unwrap();
        assert_eq!(d.duration_sec, 12.5);
        assert_eq!(d.sample_rate, Some(16000));
        assert_eq!(d.channels, Some(1));
        assert_eq!(d.codec.as_deref(), Some("aac"));
    }
}
