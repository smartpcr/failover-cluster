// -----------------------------------------------------------------------
// <copyright file="log_corruption.rs" company="Microsoft Corp.">
//     Copyright (c) Microsoft Corp. All rights reserved.
// </copyright>
// -----------------------------------------------------------------------

//! Acceptance tests for WAL corruption-classification rules. These tests
//! validate the contract that closes the prior iteration's gaps:
//!
//! 1. Foreign files (no `XRWL` magic) → recovery error, NOT silent skip.
//! 2. Wrong segment version → recovery error, NOT silent skip.
//! 3. Mid-segment frame CRC corruption → recovery error.
//! 4. Length-field corruption on the *last* frame → recovery error
//!    (NOT misclassified as a torn tail).
//! 5. Trailing partial write on the last segment → recovery succeeds,
//!    last good entry intact.
//! 6. Filename `base_index` must match header `base_index`.
//! 7. Header CRC corruption → recovery error.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use bytes::Bytes;
use crc32fast::Hasher as Crc32Hasher;

use xraft_core::message::{Entry, EntryPayload};
use xraft_core::storage::LogStore;
use xraft_core::types::{LogIndex, Term};
use xraft_storage::{DEFAULT_MAX_SEGMENT_SIZE, FileLogStore};

const SEGMENT_HEADER_LEN: usize = 28;

fn cmd_entry(index: u64, term: u64, data: &[u8]) -> Entry {
    Entry {
        index: LogIndex(index),
        term: Term(term),
        payload: EntryPayload::Command(Bytes::copy_from_slice(data)),
    }
}

fn fresh_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("xraft-wal-corruption-tests")
        .join(name);
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn first_wal_path(dir: &std::path::Path) -> PathBuf {
    fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.path().extension().is_some_and(|x| x == "wal"))
        .unwrap()
        .path()
}

fn crc32(data: &[u8]) -> u32 {
    let mut h = Crc32Hasher::new();
    h.update(data);
    h.finalize()
}

fn write_known_log(dir: &std::path::Path, count: u64) {
    let mut log = FileLogStore::open(dir).unwrap();
    for i in 1..=count {
        log.append(&[cmd_entry(i, 1, b"payload")]).unwrap();
    }
}

// ---------------------------------------------------------------------------
// (a) Foreign file — bad magic
// ---------------------------------------------------------------------------

#[test]
fn corruption_a_header_magic_flipped_is_foreign_file() {
    let dir = fresh_dir("a_header_magic_flipped");
    write_known_log(&dir, 3);

    let path = first_wal_path(&dir);
    let mut bytes = fs::read(&path).unwrap();
    bytes[0] = b'F';
    bytes[1] = b'O';
    bytes[2] = b'R';
    bytes[3] = b'N';
    fs::write(&path, &bytes).unwrap();

    let err = FileLogStore::open(&dir).expect_err("foreign magic must be rejected");
    let msg = format!("{err}");
    assert!(msg.contains("foreign"), "got: {msg}");
}

// ---------------------------------------------------------------------------
// (b) Wrong version — header CRC recomputed so version check fires
// ---------------------------------------------------------------------------

#[test]
fn corruption_b_unsupported_version_is_rejected() {
    let dir = fresh_dir("b_unsupported_version");
    write_known_log(&dir, 1);

    let path = first_wal_path(&dir);
    let mut bytes = fs::read(&path).unwrap();
    bytes[4..6].copy_from_slice(&99u16.to_le_bytes());
    let new_crc = crc32(&bytes[0..24]);
    bytes[24..28].copy_from_slice(&new_crc.to_le_bytes());
    fs::write(&path, &bytes).unwrap();

    let err = FileLogStore::open(&dir).expect_err("unsupported version must be rejected");
    let msg = format!("{err}");
    assert!(msg.contains("unsupported version"), "got: {msg}");
}

// ---------------------------------------------------------------------------
// (c) Mid-segment frame CRC mismatch — must hard-fail
// ---------------------------------------------------------------------------

#[test]
fn corruption_c_mid_frame_crc_mismatch_is_rejected() {
    let dir = fresh_dir("c_mid_frame_crc_mismatch");
    write_known_log(&dir, 5);

    let path = first_wal_path(&dir);
    let mut bytes = fs::read(&path).unwrap();
    // Flip a byte well inside the file (past header, in the middle of
    // a frame). 35 lands inside frame 2 for our 7-byte payload.
    bytes[SEGMENT_HEADER_LEN + 35] ^= 0xFF;
    fs::write(&path, &bytes).unwrap();

    let err =
        FileLogStore::open(&dir).expect_err("mid-segment frame CRC mismatch must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("corrupt") || msg.contains("CRC"),
        "got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// (d) Length-field corruption — must NOT be misclassified as torn tail
// ---------------------------------------------------------------------------

#[test]
fn corruption_d_length_field_corrupted_to_huge_is_rejected() {
    let dir = fresh_dir("d_length_corrupted_to_huge");
    write_known_log(&dir, 2);

    let path = first_wal_path(&dir);
    let mut bytes = fs::read(&path).unwrap();
    // Corrupt the LAST frame's length to a huge value. This is the
    // hardest case for the prior iteration's bug: it must NOT be
    // silently classified as a "torn tail" and truncated.
    // Each frame for "payload" (7 bytes) = 4 + 17 + 7 + 4 = 32 bytes.
    let last_frame_start = bytes.len() - 32;
    bytes[last_frame_start..last_frame_start + 4]
        .copy_from_slice(&0xFFFF_FFFEu32.to_le_bytes());
    fs::write(&path, &bytes).unwrap();

    let err = FileLogStore::open(&dir)
        .expect_err("length corruption to huge value must surface as corruption");
    let msg = format!("{err}");
    assert!(msg.contains("corrupt"), "got: {msg}");
}

#[test]
fn corruption_d2_length_field_byte_flipped_within_band_is_rejected() {
    // Variant: corrupt length to ANOTHER plausible value (still in the
    // sanity band). Because CRC covers length, the CRC check must catch
    // it and surface corruption rather than silently mis-decoding into
    // garbage frames.
    let dir = fresh_dir("d2_length_byte_flipped_within_band");
    let mut log = FileLogStore::open(&dir).unwrap();
    log.append(&[
        cmd_entry(1, 1, b"good"),
        cmd_entry(2, 1, &[0xAA; 64]),
    ])
    .unwrap();
    drop(log);

    let path = first_wal_path(&dir);
    let mut bytes = fs::read(&path).unwrap();
    // Frame 1: 4 + (17 + 4) + 4 = 29 bytes. Frame 2 starts after.
    let frame2_start = SEGMENT_HEADER_LEN + 29;
    // Flip a high bit of byte 0 of frame 2's length — keeps it in-band
    // but changes the value, so CRC fires.
    bytes[frame2_start] ^= 0x40;
    fs::write(&path, &bytes).unwrap();

    let err = FileLogStore::open(&dir)
        .expect_err("in-band length flip must surface as corruption (CRC mismatch)");
    let msg = format!("{err}");
    assert!(
        msg.contains("corrupt") || msg.contains("CRC"),
        "got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// (e) Trailing partial write — recovery succeeds
// ---------------------------------------------------------------------------

#[test]
fn corruption_e_trailing_partial_write_is_trimmed() {
    let dir = fresh_dir("e_trailing_partial_write");
    write_known_log(&dir, 4);

    let path = first_wal_path(&dir);
    let mut f = OpenOptions::new().append(true).open(&path).unwrap();
    f.write_all(&[0xDE, 0xAD]).unwrap(); // < 4 bytes — not even a length field
    f.sync_all().unwrap();

    let log = FileLogStore::open(&dir).expect("torn tail must be trimmed by recovery");
    assert_eq!(log.last_index(), LogIndex(4));
    for i in 1..=4u64 {
        assert!(log.get(LogIndex(i)).unwrap().is_some());
    }
}

// ---------------------------------------------------------------------------
// (f) Header CRC corrupted (with valid magic) → recovery error
// ---------------------------------------------------------------------------

#[test]
fn corruption_f_header_crc_corrupted_is_rejected() {
    let dir = fresh_dir("f_header_crc_corrupted");
    write_known_log(&dir, 1);

    let path = first_wal_path(&dir);
    let mut bytes = fs::read(&path).unwrap();
    // Magic stays valid; flip a byte inside base_index. CRC must fire.
    bytes[8] ^= 0xFF;
    fs::write(&path, &bytes).unwrap();

    let err = FileLogStore::open(&dir).expect_err("header CRC mismatch must be rejected");
    let msg = format!("{err}");
    assert!(
        msg.contains("corrupt header") || msg.contains("header crc"),
        "got: {msg}"
    );
}

// ---------------------------------------------------------------------------
// (g) Filename base_index ≠ header base_index → recovery error
// ---------------------------------------------------------------------------

#[test]
fn corruption_g_filename_does_not_match_header_base_index() {
    let dir = fresh_dir("g_filename_mismatch");
    write_known_log(&dir, 1);

    let path = first_wal_path(&dir);
    let renamed = dir.join("00000000000000000099.wal");
    fs::rename(&path, &renamed).unwrap();

    let err = FileLogStore::open(&dir)
        .expect_err("filename / header base_index mismatch must be rejected");
    let msg = format!("{err}");
    assert!(msg.contains("does not match"), "got: {msg}");
}

// ---------------------------------------------------------------------------
// Round-trip sanity
// ---------------------------------------------------------------------------

#[test]
fn clean_log_opens_cleanly_with_default_cap() {
    let dir = fresh_dir("clean_log_default_cap");
    write_known_log(&dir, 10);
    let log = FileLogStore::open_with_max_segment_size(&dir, DEFAULT_MAX_SEGMENT_SIZE).unwrap();
    assert_eq!(log.last_index(), LogIndex(10));
    for i in 1..=10u64 {
        assert!(log.get(LogIndex(i)).unwrap().is_some());
    }
}
