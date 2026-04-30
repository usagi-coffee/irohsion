#![allow(unused_parens)]

use bytes::{Buf, Bytes, BytesMut};
use modular_bitfield::prelude::*;

pub const HEADER_LEN: usize = 4;
pub const MAX_SEQUENCE: u64 = (1 << 26) - 1;
pub const MAX_FRAGMENTS: usize = 7;
const BUNDLE_MAGIC: [u8; 4] = *b"IRBM";
const BUNDLE_PREFIX_LEN: usize = 5;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PacketHeader {
    pub sequence: u64,
    pub fragment: u8,
    pub fragments: u8,
}

#[bitfield(bits = 32)]
#[derive(Debug, Clone, Copy)]
struct PackedHeaderTail {
    sequence: B26,
    fragment: B3,
    fragments: B3,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedPacket {
    pub header: PacketHeader,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    TooShort { actual: usize },
    InvalidFragment { fragment: u8, fragments: u8 },
    EmptyBundle,
    BundleFrameTooLarge { actual: usize },
    BundleTooShort { actual: usize, expected: usize },
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::TooShort { actual } => {
                write!(
                    f,
                    "packet too short: expected at least {HEADER_LEN} bytes, got {actual}"
                )
            }
            DecodeError::InvalidFragment {
                fragment,
                fragments,
            } => {
                write!(
                    f,
                    "invalid packet fragment metadata: fragment={fragment} fragments={fragments}"
                )
            }
            DecodeError::EmptyBundle => write!(f, "bundle must contain at least one frame"),
            DecodeError::BundleFrameTooLarge { actual } => {
                write!(f, "bundle frame too large to encode: {actual} bytes")
            }
            DecodeError::BundleTooShort { actual, expected } => {
                write!(
                    f,
                    "bundle payload too short: expected at least {expected} bytes, got {actual}"
                )
            }
        }
    }
}

impl std::error::Error for DecodeError {}

pub fn encode_packet(header: PacketHeader, payload: &[u8]) -> Bytes {
    assert!(
        header.sequence <= MAX_SEQUENCE,
        "sequence exceeds {MAX_SEQUENCE}"
    );
    assert!(
        header.fragments > 0 && header.fragment < header.fragments,
        "invalid fragment metadata"
    );

    let tail = PackedHeaderTail::new()
        .with_sequence(header.sequence as u32)
        .with_fragment(header.fragment)
        .with_fragments(header.fragments);
    let mut buf = BytesMut::with_capacity(HEADER_LEN + payload.len());
    buf.extend_from_slice(&tail.into_bytes());
    buf.extend_from_slice(payload);
    buf.freeze()
}

pub fn decode_packet(data: &[u8]) -> Result<DecodedPacket, DecodeError> {
    if data.len() < HEADER_LEN {
        return Err(DecodeError::TooShort { actual: data.len() });
    }

    let mut buf = data;
    let tail = PackedHeaderTail::from_bytes(buf[..4].try_into().expect("header tail has 4 bytes"));
    buf.advance(4);
    let sequence = u64::from(tail.sequence());
    let fragment = tail.fragment();
    let fragments = tail.fragments();
    if fragments == 0 || fragment >= fragments {
        return Err(DecodeError::InvalidFragment {
            fragment,
            fragments,
        });
    }

    Ok(DecodedPacket {
        header: PacketHeader {
            sequence,
            fragment,
            fragments,
        },
        payload: Bytes::copy_from_slice(buf),
    })
}

pub fn encode_bundle<'a, I>(frames: I) -> Result<Bytes, DecodeError>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    let frames = frames.into_iter().collect::<Vec<_>>();
    if frames.is_empty() {
        return Err(DecodeError::EmptyBundle);
    }
    if frames.len() > u8::MAX as usize {
        return Err(DecodeError::BundleFrameTooLarge {
            actual: frames.len(),
        });
    }

    let table_len = frames.len() * 2;
    let payload_len = frames.iter().map(|frame| frame.len()).sum::<usize>();
    let mut buf = BytesMut::with_capacity(BUNDLE_PREFIX_LEN + table_len + payload_len);
    buf.extend_from_slice(&BUNDLE_MAGIC);
    buf.extend_from_slice(&[frames.len() as u8]);
    for frame in &frames {
        let len = u16::try_from(frame.len())
            .map_err(|_| DecodeError::BundleFrameTooLarge { actual: frame.len() })?;
        buf.extend_from_slice(&len.to_le_bytes());
    }
    for frame in frames {
        buf.extend_from_slice(frame);
    }
    Ok(buf.freeze())
}

pub fn decode_bundle(data: &[u8]) -> Result<Option<Vec<Bytes>>, DecodeError> {
    if data.len() < BUNDLE_PREFIX_LEN || data[..4] != BUNDLE_MAGIC {
        return Ok(None);
    }

    let frame_count = data[4] as usize;
    if frame_count == 0 {
        return Err(DecodeError::EmptyBundle);
    }

    let table_len = frame_count * 2;
    let expected_prefix = BUNDLE_PREFIX_LEN + table_len;
    if data.len() < expected_prefix {
        return Err(DecodeError::BundleTooShort {
            actual: data.len(),
            expected: expected_prefix,
        });
    }

    let mut lengths = Vec::with_capacity(frame_count);
    let mut offset = BUNDLE_PREFIX_LEN;
    for _ in 0..frame_count {
        lengths.push(u16::from_le_bytes([data[offset], data[offset + 1]]) as usize);
        offset += 2;
    }

    let total_payload = lengths.iter().sum::<usize>();
    let expected_total = expected_prefix + total_payload;
    if data.len() < expected_total {
        return Err(DecodeError::BundleTooShort {
            actual: data.len(),
            expected: expected_total,
        });
    }

    let mut cursor = expected_prefix;
    let mut frames = Vec::with_capacity(frame_count);
    for len in lengths {
        frames.push(Bytes::copy_from_slice(&data[cursor..cursor + len]));
        cursor += len;
    }
    Ok(Some(frames))
}

#[cfg(test)]
mod tests {
    use super::{
        HEADER_LEN, PackedHeaderTail, PacketHeader, decode_bundle, decode_packet, encode_bundle,
        encode_packet,
    };

    #[test]
    fn roundtrip() {
        let header = PacketHeader {
            sequence: 42,
            fragment: 0,
            fragments: 1,
        };
        let payload = b"abc123";

        let encoded = encode_packet(header, payload);
        assert_eq!(encoded.len(), HEADER_LEN + payload.len());

        let decoded = decode_packet(&encoded).expect("packet decodes");
        assert_eq!(decoded.header, header);
        assert_eq!(&decoded.payload[..], payload);
    }

    #[test]
    fn rejects_invalid_fragment_metadata() {
        let packet = PackedHeaderTail::new()
            .with_sequence(42)
            .with_fragment(1)
            .with_fragments(1)
            .into_bytes();

        assert!(decode_packet(&packet).is_err());
    }

    #[test]
    fn bundle_roundtrip() {
        let encoded = encode_bundle([b"abc".as_slice(), b"defg".as_slice()]).expect("bundle");
        let decoded = decode_bundle(&encoded).expect("bundle decodes");
        assert_eq!(
            decoded.expect("is bundle"),
            vec![Bytes::from_static(b"abc"), Bytes::from_static(b"defg")]
        );
    }
}
