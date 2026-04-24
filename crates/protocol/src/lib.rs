#![allow(unused_parens)]

use bytes::{Buf, Bytes, BytesMut};
use modular_bitfield::prelude::*;

pub const HEADER_LEN: usize = 4;
pub const MAX_SEQUENCE: u64 = (1 << 26) - 1;
pub const MAX_FRAGMENTS: usize = 7;

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

#[cfg(test)]
mod tests {
    use super::{HEADER_LEN, PackedHeaderTail, PacketHeader, decode_packet, encode_packet};

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
}
