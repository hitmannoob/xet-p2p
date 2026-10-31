//! Length-prefixed CBOR frames with an enforced size limit, over any byte stream.
//!
//! Wire format: `u32` big-endian body length, then a CBOR body. Raw object payloads are never
//! wrapped in CBOR; they follow a header frame as plain bytes (see [`read_payload`]).

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::{Error, Result};

/// Writes one frame.
pub async fn write_frame<T: Serialize>(
    send: &mut (impl AsyncWrite + Unpin + ?Sized),
    msg: &T,
) -> Result<()> {
    let mut body = Vec::new();
    ciborium::into_writer(msg, &mut body).map_err(|e| Error::Encode(e.to_string()))?;
    let len = u32::try_from(body.len()).map_err(|_| Error::FrameTooLarge {
        len: body.len() as u64,
        max: u32::MAX as u64,
    })?;
    let mut buf = Vec::with_capacity(4 + body.len());
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&body);
    send.write_all(&buf).await?;
    send.flush().await?;
    Ok(())
}

/// Reads one frame of at most `max_len` body bytes. Returns `None` if the stream finished cleanly
/// before a new frame began.
pub async fn read_frame<T: DeserializeOwned>(
    recv: &mut (impl AsyncRead + Unpin + ?Sized),
    max_len: u32,
) -> Result<Option<T>> {
    let mut len = [0u8; 4];
    let mut got = 0;
    while got < 4 {
        let n = recv.read(&mut len[got..]).await?;
        if n == 0 {
            return if got == 0 {
                Ok(None)
            } else {
                Err(Error::Decode("stream ended inside a frame header".into()))
            };
        }
        got += n;
    }
    let len = u32::from_be_bytes(len);
    if len > max_len {
        return Err(Error::FrameTooLarge {
            len: len as u64,
            max: max_len as u64,
        });
    }
    let mut body = vec![0u8; len as usize];
    recv.read_exact(&mut body).await?;
    ciborium::from_reader(body.as_slice())
        .map(Some)
        .map_err(|e| Error::Decode(e.to_string()))
}

/// Reads exactly `len` raw bytes (bounded by `max_len`) and requires the stream to end there.
pub async fn read_payload(
    recv: &mut (impl AsyncRead + Unpin + ?Sized),
    len: u64,
    max_len: u64,
) -> Result<Vec<u8>> {
    if len > max_len {
        return Err(Error::FrameTooLarge { len, max: max_len });
    }
    let mut buf = vec![0u8; len as usize];
    recv.read_exact(&mut buf).await?;
    let mut extra = [0u8; 1];
    match recv.read(&mut extra).await? {
        0 => Ok(buf),
        _ => Err(Error::Decode("trailing bytes after payload".into())),
    }
}
