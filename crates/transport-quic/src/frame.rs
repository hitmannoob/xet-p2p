//! Length-prefixed CBOR frames with an enforced size limit.
//!
//! Wire format: `u32` big-endian body length, then a CBOR body. Raw object payloads are never
//! wrapped in CBOR; they follow a header frame as plain bytes (see [`read_payload`]).

use quinn::{ReadExactError, RecvStream, SendStream};
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::error::{Error, Result};

/// Writes one frame.
pub async fn write_frame<T: Serialize>(send: &mut SendStream, msg: &T) -> Result<()> {
    let mut body = Vec::new();
    ciborium::into_writer(msg, &mut body).map_err(|e| Error::Encode(e.to_string()))?;
    let len = u32::try_from(body.len()).map_err(|_| Error::FrameTooLarge {
        len: body.len() as u64,
        max: u32::MAX as u64,
    })?;
    send.write_all(&len.to_be_bytes()).await?;
    send.write_all(&body).await?;
    Ok(())
}

/// Reads one frame of at most `max_len` body bytes. Returns `None` if the stream finished cleanly
/// before a new frame began.
pub async fn read_frame<T: DeserializeOwned>(
    recv: &mut RecvStream,
    max_len: u32,
) -> Result<Option<T>> {
    let mut len = [0u8; 4];
    match recv.read_exact(&mut len).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(0)) => return Ok(None),
        Err(e) => return Err(e.into()),
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
pub async fn read_payload(recv: &mut RecvStream, len: u64, max_len: u64) -> Result<Vec<u8>> {
    if len > max_len {
        return Err(Error::FrameTooLarge { len, max: max_len });
    }
    let mut buf = vec![0u8; len as usize];
    recv.read_exact(&mut buf).await?;
    let mut extra = [0u8; 1];
    match recv
        .read(&mut extra)
        .await
        .map_err(|e| Error::Stream(e.to_string()))?
    {
        None => Ok(buf),
        Some(_) => Err(Error::Decode("trailing bytes after payload".into())),
    }
}
