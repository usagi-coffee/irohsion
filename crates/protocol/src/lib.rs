#![allow(unused_parens)]

use bytes::{Buf, Bytes, BytesMut};
use modular_bitfield::prelude::*;

pub const HEADER_LEN: usize = 4;
pub const MAX_SEQUENCE: u64 = (1 << 26) - 1;
pub const FEC_SEQUENCE: u64 = MAX_SEQUENCE;
pub const MAX_MEDIA_SEQUENCE: u64 = FEC_SEQUENCE - 1;
pub const MAX_FRAGMENTS: usize = 7;
pub const REPAIR_REQUEST_LEN: usize = 5;
pub const REPAIR_ALL_FRAGMENTS_MASK: u8 = (1 << MAX_FRAGMENTS) - 1;
pub const MAX_FEC_GROUP_PACKETS: usize = 32;
const FEC_MAGIC: &[u8; 5] = b"IFEC1";

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RepairRequest {
    pub sequence: u64,
    pub missing_mask: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FecFrame {
    pub base_sequence: u64,
    pub payload_lengths: Vec<usize>,
    pub parity: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    TooShort { actual: usize },
    InvalidFragment { fragment: u8, fragments: u8 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepairDecodeError {
    InvalidLength { actual: usize },
    InvalidSequence { sequence: u64 },
    InvalidMissingMask { missing_mask: u8 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FecDecodeError {
    InvalidPacket(DecodeError),
    InvalidHeader(PacketHeader),
    TooShort { actual: usize },
    InvalidMagic,
    InvalidBaseSequence { sequence: u64 },
    InvalidGroupSize { count: usize },
    InvalidPayloadLength { length: usize },
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

impl std::fmt::Display for RepairDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RepairDecodeError::InvalidLength { actual } => {
                write!(
                    f,
                    "repair request has invalid length: expected {REPAIR_REQUEST_LEN} bytes, got {actual}"
                )
            }
            RepairDecodeError::InvalidSequence { sequence } => {
                write!(
                    f,
                    "repair request sequence exceeds {MAX_SEQUENCE}: {sequence}"
                )
            }
            RepairDecodeError::InvalidMissingMask { missing_mask } => {
                write!(
                    f,
                    "repair request has invalid missing mask: {missing_mask:#04x}"
                )
            }
        }
    }
}

impl std::error::Error for RepairDecodeError {}

impl std::fmt::Display for FecDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FecDecodeError::InvalidPacket(err) => write!(f, "invalid fec packet: {err}"),
            FecDecodeError::InvalidHeader(header) => write!(
                f,
                "invalid fec packet header: sequence={} fragment={} fragments={}",
                header.sequence, header.fragment, header.fragments
            ),
            FecDecodeError::TooShort { actual } => {
                write!(f, "fec frame too short: got {actual} bytes")
            }
            FecDecodeError::InvalidMagic => write!(f, "invalid fec frame magic"),
            FecDecodeError::InvalidBaseSequence { sequence } => {
                write!(
                    f,
                    "fec base sequence exceeds {MAX_MEDIA_SEQUENCE}: {sequence}"
                )
            }
            FecDecodeError::InvalidGroupSize { count } => {
                write!(f, "invalid fec group size: {count}")
            }
            FecDecodeError::InvalidPayloadLength { length } => {
                write!(f, "invalid fec payload length: {length}")
            }
        }
    }
}

impl std::error::Error for FecDecodeError {}

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

pub fn encode_repair_request(request: RepairRequest) -> Bytes {
    assert!(
        request.sequence <= MAX_SEQUENCE,
        "repair sequence exceeds {MAX_SEQUENCE}"
    );
    assert!(
        request.missing_mask != 0 && request.missing_mask & !REPAIR_ALL_FRAGMENTS_MASK == 0,
        "invalid repair missing mask"
    );

    let mut buf = BytesMut::with_capacity(REPAIR_REQUEST_LEN);
    buf.extend_from_slice(&(request.sequence as u32).to_le_bytes());
    buf.extend_from_slice(&[request.missing_mask]);
    buf.freeze()
}

pub fn decode_repair_request(data: &[u8]) -> Result<RepairRequest, RepairDecodeError> {
    if data.len() != REPAIR_REQUEST_LEN {
        return Err(RepairDecodeError::InvalidLength { actual: data.len() });
    }

    let sequence = u64::from(u32::from_le_bytes(
        data[..4]
            .try_into()
            .expect("repair sequence has exactly 4 bytes"),
    ));
    if sequence > MAX_SEQUENCE {
        return Err(RepairDecodeError::InvalidSequence { sequence });
    }

    let missing_mask = data[4];
    if missing_mask == 0 || missing_mask & !REPAIR_ALL_FRAGMENTS_MASK != 0 {
        return Err(RepairDecodeError::InvalidMissingMask { missing_mask });
    }

    Ok(RepairRequest {
        sequence,
        missing_mask,
    })
}

pub fn encode_fec_frame(frame: &FecFrame) -> Bytes {
    assert!(
        frame.base_sequence <= MAX_MEDIA_SEQUENCE,
        "fec base sequence exceeds {MAX_MEDIA_SEQUENCE}"
    );
    assert!(
        !frame.payload_lengths.is_empty() && frame.payload_lengths.len() <= MAX_FEC_GROUP_PACKETS,
        "invalid fec group size"
    );
    assert!(
        frame
            .payload_lengths
            .iter()
            .all(|length| *length <= u16::MAX as usize && *length <= frame.parity.len()),
        "invalid fec payload lengths"
    );

    let payload_len =
        FEC_MAGIC.len() + 4 + 1 + frame.payload_lengths.len() * 2 + frame.parity.len();
    let mut payload = BytesMut::with_capacity(payload_len);
    payload.extend_from_slice(FEC_MAGIC);
    payload.extend_from_slice(&(frame.base_sequence as u32).to_le_bytes());
    payload.extend_from_slice(&[
        u8::try_from(frame.payload_lengths.len()).expect("fec group size fits in u8")
    ]);
    for length in &frame.payload_lengths {
        payload.extend_from_slice(
            &u16::try_from(*length)
                .expect("length fits in u16")
                .to_le_bytes(),
        );
    }
    payload.extend_from_slice(&frame.parity);

    encode_packet(
        PacketHeader {
            sequence: FEC_SEQUENCE,
            fragment: 0,
            fragments: 1,
        },
        &payload,
    )
}

pub fn decode_fec_frame(data: &[u8]) -> Result<FecFrame, FecDecodeError> {
    let DecodedPacket { header, payload } =
        decode_packet(data).map_err(FecDecodeError::InvalidPacket)?;
    if header.sequence != FEC_SEQUENCE || header.fragment != 0 || header.fragments != 1 {
        return Err(FecDecodeError::InvalidHeader(header));
    }

    let min_len = FEC_MAGIC.len() + 4 + 1;
    if payload.len() < min_len {
        return Err(FecDecodeError::TooShort {
            actual: payload.len(),
        });
    }
    if &payload[..FEC_MAGIC.len()] != FEC_MAGIC {
        return Err(FecDecodeError::InvalidMagic);
    }

    let mut offset = FEC_MAGIC.len();
    let base_sequence = u64::from(u32::from_le_bytes(
        payload[offset..offset + 4]
            .try_into()
            .expect("fec base sequence has exactly 4 bytes"),
    ));
    offset += 4;
    if base_sequence > MAX_MEDIA_SEQUENCE {
        return Err(FecDecodeError::InvalidBaseSequence {
            sequence: base_sequence,
        });
    }

    let count = payload[offset] as usize;
    offset += 1;
    if count == 0 || count > MAX_FEC_GROUP_PACKETS {
        return Err(FecDecodeError::InvalidGroupSize { count });
    }

    let lengths_len = count * 2;
    if payload.len() < offset + lengths_len {
        return Err(FecDecodeError::TooShort {
            actual: payload.len(),
        });
    }

    let mut payload_lengths = Vec::with_capacity(count);
    for _ in 0..count {
        let length = u16::from_le_bytes(
            payload[offset..offset + 2]
                .try_into()
                .expect("fec payload length has exactly 2 bytes"),
        ) as usize;
        offset += 2;
        payload_lengths.push(length);
    }

    let parity = payload.slice(offset..);
    if payload_lengths.iter().any(|length| *length > parity.len()) {
        return Err(FecDecodeError::InvalidPayloadLength {
            length: payload_lengths
                .into_iter()
                .max()
                .expect("fec payload lengths are non-empty"),
        });
    }

    Ok(FecFrame {
        base_sequence,
        payload_lengths,
        parity,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        FecFrame, HEADER_LEN, PackedHeaderTail, PacketHeader, REPAIR_ALL_FRAGMENTS_MASK,
        RepairDecodeError, RepairRequest, decode_fec_frame, decode_packet, decode_repair_request,
        encode_fec_frame, encode_packet, encode_repair_request,
    };
    use bytes::Bytes;

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
    fn repair_request_roundtrip() {
        let request = RepairRequest {
            sequence: 123,
            missing_mask: 0b0000_0101,
        };

        let encoded = encode_repair_request(request);
        assert_eq!(decode_repair_request(&encoded), Ok(request));
    }

    #[test]
    fn repair_request_rejects_invalid_masks() {
        let zero_mask = [0, 0, 0, 0, 0];
        assert_eq!(
            decode_repair_request(&zero_mask),
            Err(RepairDecodeError::InvalidMissingMask { missing_mask: 0 })
        );

        let invalid_mask = [0, 0, 0, 0, REPAIR_ALL_FRAGMENTS_MASK << 1];
        assert!(matches!(
            decode_repair_request(&invalid_mask),
            Err(RepairDecodeError::InvalidMissingMask { .. })
        ));
    }

    #[test]
    fn fec_frame_roundtrip() {
        let frame = FecFrame {
            base_sequence: 44,
            payload_lengths: vec![3, 5],
            parity: Bytes::from_static(b"abcde"),
        };

        let encoded = encode_fec_frame(&frame);
        assert_eq!(decode_fec_frame(&encoded), Ok(frame));
    }
}
