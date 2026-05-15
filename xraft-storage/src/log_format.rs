// -----------------------------------------------------------------------
// <copyright file="log_format.rs" company="Microsoft Corp.">
//     Copyright (c) Microsoft Corp. All rights reserved.
// </copyright>
// -----------------------------------------------------------------------

//! Pure encode / decode for the Write-Ahead Log on-disk format.
//!
//! This module performs **no I/O**. Everything here is byte-in / byte-out so
//! it can be exhaustively unit-tested and (later) fuzzed.
//!
//! # Segment header (28 bytes, written once at offset 0 when a segment is
//! created)
//!
//! ```text
//! ┌─────────┬─────────┬─────────┬───────────┬───────────────────┬───────┐
//! │ magic   │ version │ flags   │ base_idx  │ created_at_unix_ms│ crc32 │
//! │ 4 bytes │ u16 LE  │ u16 LE  │ u64 LE    │ u64 LE            │ u32 LE│
//! └─────────┴─────────┴─────────┴───────────┴───────────────────┴───────┘
//!     "XRWL"     2         2         8              8                 4
//! ```
//!
//! `crc32` covers the first 24 bytes (everything before the CRC itself).
//! Validation order on `decode_segment_header`:
//!
//! 1. Length check (must be ≥ 28).
//! 2. Magic check ("XRWL"). Wrong magic ⇒ [`HeaderDecodeError::ForeignFile`].
//! 3. Header CRC check. Mismatch ⇒ [`HeaderDecodeError::Corrupt`].
//! 4. Version check (must equal `1`). Other ⇒
//!    [`HeaderDecodeError::UnsupportedVersion`].
//!
//! # Frame layout (each entry, written sequentially after the header)
//!
//! ```text
//! ┌────────┬────────┬────────┬────────┬────────────┬────────┐
//! │ length │ term   │ index  │ e_type │ data       │ crc32  │
//! │ u32 LE │ u64 LE │ u64 LE │ u8     │ length-17  │ u32 LE │
//! └────────┴────────┴────────┴────────┴────────────┴────────┘
//!     4        8        8       1     length - 17     4
//! ```
//!
//! * `length` = `17 + data.len()` — the size of the body that follows the
//!   length field, **excluding** the trailing CRC.
//! * `crc32` is computed over `length_bytes ++ body` — the length is
//!   **inside** the CRC envelope. A corrupted `length` field can therefore
//!   never be silently classified as a torn tail (see
//!   `decode_frame` below).
//! * Total frame on disk = `length + 8` bytes.
//!
//! # Decode classification rules
//!
//! * Short read of the 4-byte length, of the body, or of the trailing CRC:
//!   [`FrameDecodeError::Truncated`]. Recovery treats this as a torn tail
//!   on the **last** segment only and trims it.
//! * Length out of range (`< 17` or `> max_frame_body`):
//!   [`FrameDecodeError::Corrupt`]. Never truncated.
//! * Length is in range but the trailing CRC mismatches:
//!   [`FrameDecodeError::Corrupt`]. Never truncated.
//! * Unknown `entry_type`: [`FrameDecodeError::Corrupt`].

use bytes::Bytes;
use crc32fast::Hasher as Crc32Hasher;

use xraft_core::message::{Entry, EntryPayload};
use xraft_core::storage::SnapshotMeta;
use xraft_core::types::{LogIndex, Term, VoterSet};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Magic bytes identifying an XRAFT Write-ahead Log segment file.
pub(crate) const SEGMENT_MAGIC: [u8; 4] = *b"XRWL";

/// Current on-disk segment format version. v1 is frozen by Stage 2.1.
pub(crate) const SEGMENT_VERSION: u16 = 1;

/// Total size of the segment header in bytes (24 bytes of fields + 4 byte CRC).
pub(crate) const SEGMENT_HEADER_LEN: u64 = 28;

/// Number of header bytes that the header CRC covers.
const SEGMENT_HEADER_CRC_COVER: usize = 24;

/// Length of the per-frame `length` field.
const FRAME_LENGTH_FIELD: usize = 4;

/// Length of the per-frame trailing CRC field.
const FRAME_CRC_FIELD: usize = 4;

/// Fixed body header inside a frame body: term(8) + index(8) + entry_type(1).
pub(crate) const FRAME_BODY_HEADER: u32 = 17;

/// Default sanity cap on a single frame body length. Frames larger than
/// this cap are treated as corruption rather than as plausible torn tails.
/// 64 MiB matches the default segment size; callers may pass a smaller cap.
pub(crate) const DEFAULT_MAX_FRAME_BODY: u32 = 64 * 1024 * 1024;

const PAYLOAD_TAG_NOOP: u8 = 0;
const PAYLOAD_TAG_COMMAND: u8 = 1;
const PAYLOAD_TAG_CONFIG_CHANGE: u8 = 2;
const PAYLOAD_TAG_SNAPSHOT: u8 = 3;

// ---------------------------------------------------------------------------
// Error enums
// ---------------------------------------------------------------------------

/// Decode failures for the 28-byte segment header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum HeaderDecodeError {
    /// Fewer than 28 bytes were available — file does not contain a header.
    ShortHeader,
    /// Magic bytes do not match `"XRWL"`. The file is not an XRAFT WAL
    /// segment.
    ForeignFile,
    /// Magic matched but the header CRC failed.
    Corrupt(String),
    /// Header decoded cleanly but its declared `version` is not understood
    /// by this build.
    UnsupportedVersion(u16),
}

/// Decode failures for a single on-disk frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FrameDecodeError {
    /// The frame was cut short — recovery may legally trim this on the
    /// last segment of the WAL after the prior frame's CRC verified.
    Truncated(String),
    /// The frame is structurally invalid (bad CRC, out-of-range length,
    /// unknown payload tag, etc.). Recovery never silently swallows this.
    Corrupt(String),
}

impl std::fmt::Display for FrameDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameDecodeError::Truncated(s) => write!(f, "truncated frame: {s}"),
            FrameDecodeError::Corrupt(s) => write!(f, "corrupt frame: {s}"),
        }
    }
}

impl std::fmt::Display for HeaderDecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HeaderDecodeError::ShortHeader => write!(f, "short segment header"),
            HeaderDecodeError::ForeignFile => write!(f, "not an XRAFT WAL segment"),
            HeaderDecodeError::Corrupt(s) => write!(f, "corrupt segment header: {s}"),
            HeaderDecodeError::UnsupportedVersion(v) => {
                write!(f, "unsupported segment version: {v}")
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Segment header
// ---------------------------------------------------------------------------

/// In-memory representation of the 28-byte segment header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SegmentHeader {
    pub base_index: LogIndex,
    pub created_at_unix_ms: u64,
}

/// Encode a 28-byte segment header.
pub(crate) fn encode_segment_header(h: &SegmentHeader) -> [u8; 28] {
    let mut buf = [0u8; 28];
    buf[0..4].copy_from_slice(&SEGMENT_MAGIC);
    buf[4..6].copy_from_slice(&SEGMENT_VERSION.to_le_bytes());
    buf[6..8].copy_from_slice(&0u16.to_le_bytes()); // flags reserved
    buf[8..16].copy_from_slice(&h.base_index.0.to_le_bytes());
    buf[16..24].copy_from_slice(&h.created_at_unix_ms.to_le_bytes());
    let crc = crc32_of(&buf[0..SEGMENT_HEADER_CRC_COVER]);
    buf[24..28].copy_from_slice(&crc.to_le_bytes());
    buf
}

/// Decode a 28-byte segment header. Validation order: length → magic →
/// CRC → version. This ordering matters for diagnostics: a foreign file
/// reports `ForeignFile` rather than a misleading "bad CRC" error.
pub(crate) fn decode_segment_header(buf: &[u8]) -> Result<SegmentHeader, HeaderDecodeError> {
    if buf.len() < SEGMENT_HEADER_LEN as usize {
        return Err(HeaderDecodeError::ShortHeader);
    }
    if buf[0..4] != SEGMENT_MAGIC {
        return Err(HeaderDecodeError::ForeignFile);
    }
    let stored_crc = u32::from_le_bytes(buf[24..28].try_into().unwrap());
    let computed_crc = crc32_of(&buf[0..SEGMENT_HEADER_CRC_COVER]);
    if stored_crc != computed_crc {
        return Err(HeaderDecodeError::Corrupt(format!(
            "header crc mismatch: stored={stored_crc:#010x} computed={computed_crc:#010x}"
        )));
    }
    let version = u16::from_le_bytes(buf[4..6].try_into().unwrap());
    if version != SEGMENT_VERSION {
        return Err(HeaderDecodeError::UnsupportedVersion(version));
    }
    let base_index = LogIndex(u64::from_le_bytes(buf[8..16].try_into().unwrap()));
    let created_at_unix_ms = u64::from_le_bytes(buf[16..24].try_into().unwrap());
    Ok(SegmentHeader {
        base_index,
        created_at_unix_ms,
    })
}

// ---------------------------------------------------------------------------
// Frame codec
// ---------------------------------------------------------------------------

/// Encode an [`Entry`] to the on-disk frame format described in the module
/// docs. Returns the bytes ready to be appended to a segment file.
pub(crate) fn encode_frame(entry: &Entry) -> Vec<u8> {
    let payload_bytes = encode_payload(&entry.payload);
    // length covers term(8) + index(8) + tag(1) + payload bytes.
    let length = (FRAME_BODY_HEADER as usize + payload_bytes.len()) as u32;

    let mut frame = Vec::with_capacity(FRAME_LENGTH_FIELD + length as usize + FRAME_CRC_FIELD);
    frame.extend_from_slice(&length.to_le_bytes());
    frame.extend_from_slice(&entry.term.0.to_le_bytes());
    frame.extend_from_slice(&entry.index.0.to_le_bytes());
    frame.push(payload_tag(&entry.payload));
    frame.extend_from_slice(&payload_bytes);

    // CRC covers length_bytes ++ body. Length is INSIDE the CRC envelope
    // so a corrupted length cannot pass as a torn tail when decoded.
    let crc = crc32_of(&frame[..FRAME_LENGTH_FIELD + length as usize]);
    frame.extend_from_slice(&crc.to_le_bytes());
    frame
}

/// Total byte length of the on-disk frame for `entry`.
pub(crate) fn frame_byte_len(entry: &Entry) -> u32 {
    let payload_bytes = encode_payload(&entry.payload);
    let length = FRAME_BODY_HEADER + payload_bytes.len() as u32;
    FRAME_LENGTH_FIELD as u32 + length + FRAME_CRC_FIELD as u32
}

/// Decode a frame starting at byte `offset` in `buf`.
///
/// `max_frame_body` is the upper bound on the in-frame `length` field;
/// frames declaring a body larger than this are classified as
/// [`FrameDecodeError::Corrupt`] (never as `Truncated`) so a corrupted
/// length field cannot be mistaken for a torn write.
pub(crate) fn decode_frame(
    buf: &[u8],
    offset: usize,
    max_frame_body: u32,
) -> Result<(Entry, usize), FrameDecodeError> {
    let remaining = buf.len().saturating_sub(offset);

    if remaining < FRAME_LENGTH_FIELD {
        return Err(FrameDecodeError::Truncated("missing length field".into()));
    }

    let length =
        u32::from_le_bytes(buf[offset..offset + FRAME_LENGTH_FIELD].try_into().unwrap());

    // Length sanity. A corrupted length that falls outside this band is
    // always corruption — never a torn tail. This is the key fix that
    // closes the prior iteration's classification bug.
    if length < FRAME_BODY_HEADER {
        return Err(FrameDecodeError::Corrupt(format!(
            "length {length} below minimum body header size {FRAME_BODY_HEADER}"
        )));
    }
    if length > max_frame_body {
        return Err(FrameDecodeError::Corrupt(format!(
            "length {length} exceeds max_frame_body {max_frame_body}"
        )));
    }

    let total = FRAME_LENGTH_FIELD + length as usize + FRAME_CRC_FIELD;
    if remaining < total {
        return Err(FrameDecodeError::Truncated(format!(
            "incomplete frame body or trailing CRC \
             (need {total} bytes, have {remaining})"
        )));
    }

    let body_start = offset + FRAME_LENGTH_FIELD;
    let body_end = body_start + length as usize;
    let crc_end = body_end + FRAME_CRC_FIELD;

    let stored_crc = u32::from_le_bytes(buf[body_end..crc_end].try_into().unwrap());
    // CRC covers length_bytes ++ body. Length is INSIDE the envelope.
    let computed_crc = crc32_of(&buf[offset..body_end]);
    if stored_crc != computed_crc {
        return Err(FrameDecodeError::Corrupt(format!(
            "frame CRC mismatch at byte {offset}: \
             stored={stored_crc:#010x} computed={computed_crc:#010x}"
        )));
    }

    let body = &buf[body_start..body_end];
    let term = Term(u64::from_le_bytes(body[0..8].try_into().unwrap()));
    let index = LogIndex(u64::from_le_bytes(body[8..16].try_into().unwrap()));
    let tag = body[16];
    let payload_bytes = &body[FRAME_BODY_HEADER as usize..];

    let payload = decode_payload(tag, payload_bytes)
        .map_err(|e| FrameDecodeError::Corrupt(format!("payload decode failed: {e}")))?;

    Ok((
        Entry {
            index,
            term,
            payload,
        },
        offset + total,
    ))
}

// ---------------------------------------------------------------------------
// Payload encode / decode
// ---------------------------------------------------------------------------

fn payload_tag(p: &EntryPayload) -> u8 {
    match p {
        EntryPayload::NoOp => PAYLOAD_TAG_NOOP,
        EntryPayload::Command(_) => PAYLOAD_TAG_COMMAND,
        EntryPayload::ConfigChange(_) => PAYLOAD_TAG_CONFIG_CHANGE,
        EntryPayload::Snapshot(_) => PAYLOAD_TAG_SNAPSHOT,
    }
}

fn encode_payload(p: &EntryPayload) -> Vec<u8> {
    match p {
        EntryPayload::NoOp => Vec::new(),
        EntryPayload::Command(b) => b.to_vec(),
        EntryPayload::ConfigChange(vs) => bincode::serialize(vs)
            .expect("VoterSet bincode serialization should not fail"),
        EntryPayload::Snapshot(meta) => bincode::serialize(meta)
            .expect("SnapshotMeta bincode serialization should not fail"),
    }
}

fn decode_payload(tag: u8, bytes: &[u8]) -> Result<EntryPayload, String> {
    match tag {
        PAYLOAD_TAG_NOOP => {
            if !bytes.is_empty() {
                return Err(format!("NoOp must have empty data, got {} bytes", bytes.len()));
            }
            Ok(EntryPayload::NoOp)
        }
        PAYLOAD_TAG_COMMAND => Ok(EntryPayload::Command(Bytes::copy_from_slice(bytes))),
        PAYLOAD_TAG_CONFIG_CHANGE => {
            let vs: VoterSet = bincode::deserialize(bytes)
                .map_err(|e| format!("VoterSet decode failed: {e}"))?;
            Ok(EntryPayload::ConfigChange(vs))
        }
        PAYLOAD_TAG_SNAPSHOT => {
            let meta: SnapshotMeta = bincode::deserialize(bytes)
                .map_err(|e| format!("SnapshotMeta decode failed: {e}"))?;
            Ok(EntryPayload::Snapshot(meta))
        }
        other => Err(format!("unknown payload tag: {other}")),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn crc32_of(data: &[u8]) -> u32 {
    let mut h = Crc32Hasher::new();
    h.update(data);
    h.finalize()
}

// ---------------------------------------------------------------------------
// Unit tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn noop(index: u64, term: u64) -> Entry {
        Entry {
            index: LogIndex(index),
            term: Term(term),
            payload: EntryPayload::NoOp,
        }
    }

    fn cmd(index: u64, term: u64, data: &[u8]) -> Entry {
        Entry {
            index: LogIndex(index),
            term: Term(term),
            payload: EntryPayload::Command(Bytes::copy_from_slice(data)),
        }
    }

    // -- Segment header -----------------------------------------------------

    #[test]
    fn header_roundtrip() {
        let h = SegmentHeader {
            base_index: LogIndex(42),
            created_at_unix_ms: 1_715_000_000_000,
        };
        let bytes = encode_segment_header(&h);
        let decoded = decode_segment_header(&bytes).unwrap();
        assert_eq!(decoded, h);
    }

    #[test]
    fn header_short_header_rejected() {
        let bytes = [0u8; 10];
        assert!(matches!(
            decode_segment_header(&bytes),
            Err(HeaderDecodeError::ShortHeader)
        ));
    }

    #[test]
    fn header_foreign_magic_rejected() {
        let mut bytes = encode_segment_header(&SegmentHeader {
            base_index: LogIndex(1),
            created_at_unix_ms: 0,
        });
        bytes[0] = b'X';
        bytes[1] = b'Y';
        bytes[2] = b'Z';
        bytes[3] = b'W';
        assert!(matches!(
            decode_segment_header(&bytes),
            Err(HeaderDecodeError::ForeignFile)
        ));
    }

    #[test]
    fn header_corrupt_crc_rejected() {
        let mut bytes = encode_segment_header(&SegmentHeader {
            base_index: LogIndex(1),
            created_at_unix_ms: 0,
        });
        // Flip a byte in the base_index field; the CRC must catch it.
        bytes[8] ^= 0xFF;
        assert!(matches!(
            decode_segment_header(&bytes),
            Err(HeaderDecodeError::Corrupt(_))
        ));
    }

    #[test]
    fn header_unsupported_version_rejected() {
        // Build a header with version 99 but a valid CRC so the version
        // check (not the CRC check) is what fires.
        let mut bytes = encode_segment_header(&SegmentHeader {
            base_index: LogIndex(1),
            created_at_unix_ms: 0,
        });
        bytes[4..6].copy_from_slice(&99u16.to_le_bytes());
        let crc = crc32_of(&bytes[0..SEGMENT_HEADER_CRC_COVER]);
        bytes[24..28].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            decode_segment_header(&bytes),
            Err(HeaderDecodeError::UnsupportedVersion(99))
        ));
    }

    // -- Frame codec --------------------------------------------------------

    #[test]
    fn frame_roundtrip_noop() {
        let e = noop(1, 1);
        let bytes = encode_frame(&e);
        assert_eq!(bytes.len() as u32, frame_byte_len(&e));
        let (decoded, next) = decode_frame(&bytes, 0, DEFAULT_MAX_FRAME_BODY).unwrap();
        assert_eq!(decoded, e);
        assert_eq!(next, bytes.len());
    }

    #[test]
    fn frame_roundtrip_command() {
        let e = cmd(7, 3, b"hello world");
        let bytes = encode_frame(&e);
        let (decoded, _) = decode_frame(&bytes, 0, DEFAULT_MAX_FRAME_BODY).unwrap();
        assert_eq!(decoded, e);
    }

    #[test]
    fn frame_truncated_missing_length() {
        let buf = [0u8, 1, 2];
        assert!(matches!(
            decode_frame(&buf, 0, DEFAULT_MAX_FRAME_BODY),
            Err(FrameDecodeError::Truncated(_))
        ));
    }

    #[test]
    fn frame_truncated_missing_body() {
        let e = cmd(1, 1, b"abc");
        let bytes = encode_frame(&e);
        // Cut off the trailing CRC and half the body.
        let truncated = &bytes[..bytes.len() / 2];
        assert!(matches!(
            decode_frame(truncated, 0, DEFAULT_MAX_FRAME_BODY),
            Err(FrameDecodeError::Truncated(_))
        ));
    }

    #[test]
    fn frame_corrupt_when_length_below_min() {
        // Manually craft a frame with length=5 (< body header size 17).
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&5u32.to_le_bytes());
        bytes.extend_from_slice(&[0u8; 9]); // bogus body+crc
        assert!(matches!(
            decode_frame(&bytes, 0, DEFAULT_MAX_FRAME_BODY),
            Err(FrameDecodeError::Corrupt(_))
        ));
    }

    #[test]
    fn frame_corrupt_when_length_above_cap() {
        // Length = 100, max_frame_body = 50 → corrupt.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&100u32.to_le_bytes());
        bytes.extend_from_slice(&[0u8; 200]);
        assert!(matches!(
            decode_frame(&bytes, 0, 50),
            Err(FrameDecodeError::Corrupt(_))
        ));
    }

    #[test]
    fn frame_corrupt_when_length_field_corrupted_to_huge() {
        // Encode a real frame, then corrupt the length to 0xFFFFFFFE.
        // With the default cap this must surface as Corrupt, NOT Truncated.
        let e = cmd(1, 1, b"data");
        let mut bytes = encode_frame(&e);
        bytes[0..4].copy_from_slice(&0xFFFF_FFFEu32.to_le_bytes());
        assert!(matches!(
            decode_frame(&bytes, 0, DEFAULT_MAX_FRAME_BODY),
            Err(FrameDecodeError::Corrupt(_))
        ));
    }

    #[test]
    fn frame_corrupt_on_crc_mismatch() {
        let e = cmd(1, 1, b"data");
        let mut bytes = encode_frame(&e);
        // Flip a body byte (after length, before CRC). CRC must fire.
        bytes[FRAME_LENGTH_FIELD + 5] ^= 0xFF;
        assert!(matches!(
            decode_frame(&bytes, 0, DEFAULT_MAX_FRAME_BODY),
            Err(FrameDecodeError::Corrupt(_))
        ));
    }

    #[test]
    fn frame_corrupt_when_length_byte_flipped() {
        // The CRC envelope explicitly covers the length field, so flipping
        // a single bit inside `length` (within the legal range) must still
        // surface as a CRC failure rather than slip through. We mutate
        // the length to a SMALLER in-band value so the decoder can still
        // read body+crc within the buffer (otherwise we'd hit Truncated
        // before the CRC even runs).
        let e = cmd(10, 2, &[0xAA; 64]);
        let mut bytes = encode_frame(&e);
        // Body length is 17 + 64 = 81. Flip bit 0x01 to make it 80.
        assert_eq!(u32::from_le_bytes(bytes[0..4].try_into().unwrap()), 81);
        bytes[0] = 80;
        assert!(matches!(
            decode_frame(&bytes, 0, DEFAULT_MAX_FRAME_BODY),
            Err(FrameDecodeError::Corrupt(_))
        ));
    }

    #[test]
    fn frame_unknown_payload_tag_corrupt() {
        let e = noop(1, 1);
        let mut bytes = encode_frame(&e);
        // Body byte 16 is the tag (after term[0..8] and index[8..16]).
        bytes[FRAME_LENGTH_FIELD + 16] = 99;
        // Recompute CRC so the tag check (not CRC) is what fires.
        let body_end = FRAME_LENGTH_FIELD + (FRAME_BODY_HEADER as usize);
        let crc = crc32_of(&bytes[..body_end]);
        bytes[body_end..body_end + FRAME_CRC_FIELD].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            decode_frame(&bytes, 0, DEFAULT_MAX_FRAME_BODY),
            Err(FrameDecodeError::Corrupt(_))
        ));
    }
}
