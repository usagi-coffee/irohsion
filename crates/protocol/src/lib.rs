use bytes::{Buf, BufMut, Bytes, BytesMut};

pub const HEADER_LEN: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PacketHeader {
    pub session_id: u32,
    pub seq: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedPacket {
    pub header: PacketHeader,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    TooShort { actual: usize },
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
        }
    }
}

impl std::error::Error for DecodeError {}

pub fn encode_packet(header: PacketHeader, payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(HEADER_LEN + payload.len());
    buf.put_u32(header.session_id);
    buf.put_u64(header.seq);
    buf.extend_from_slice(payload);
    buf.freeze()
}

pub fn decode_packet(data: &[u8]) -> Result<DecodedPacket, DecodeError> {
    if data.len() < HEADER_LEN {
        return Err(DecodeError::TooShort { actual: data.len() });
    }

    let mut buf = data;
    let session_id = buf.get_u32();
    let seq = buf.get_u64();

    Ok(DecodedPacket {
        header: PacketHeader { session_id, seq },
        payload: Bytes::copy_from_slice(buf),
    })
}

#[cfg(test)]
mod tests {
    use super::{HEADER_LEN, PacketHeader, decode_packet, encode_packet};

    #[test]
    fn roundtrip() {
        let header = PacketHeader {
            session_id: 7,
            seq: 42,
        };
        let payload = b"abc123";

        let encoded = encode_packet(header, payload);
        assert_eq!(encoded.len(), HEADER_LEN + payload.len());

        let decoded = decode_packet(&encoded).expect("packet decodes");
        assert_eq!(decoded.header, header);
        assert_eq!(&decoded.payload[..], payload);
    }
}
