//! Canonical teardown-noise allowlist for the xraft server stack.
//!
//! # Why this lives in `xraft-server` (iter-13)
//!
//! Earlier iterations parked this predicate in `xraft-test`, but the
//! shape it matches is produced by `xraft-server`'s own driver
//! (`xraft-server/src/driver.rs::run_shutdown` adds the
//! `"final hard-state persist failed"` wrapper around line 4191) and
//! BOTH `xraft-test` and `xraft-server`'s own integration tests need
//! to consume it. `xraft-server` cannot depend on `xraft-test` (the
//! reverse dep already exists), so the canonical home is here. The
//! `xraft-test::teardown` module is now a thin re-export shim so
//! every consumer reads from the same definition.
//!
//! # What this predicate matches
//!
//! Exactly one well-documented Windows-only failure mode:
//!
//! * **Allowed (logged via `tracing::warn`, not panicked):** the
//!   Windows tempdir-teardown race where the [`Driver`]'s final
//!   `quorum-state.tmp` → `quorum-state` rename fails with
//!   `os error 3` ("The system cannot find the path specified")
//!   or `os error 2` ("The system cannot find the file specified")
//!   because [`tempfile::TempDir`]'s `Drop` has already begun
//!   removing the directory out from under the still-flushing
//!   driver. Documented as cosmetic since iter 4.
//!
//! * **Fatal (panic):** every other [`XRaftError`] variant, every
//!   `JoinError::is_panic()`, and every timeout. These indicate
//!   real driver/transport bugs or shutdown deadlocks that
//!   previously passed undetected when callers used the
//!   `let _ = tokio::time::timeout(..., handle.join()).await;`
//!   anti-pattern.
//!
//! # Specificity (iter-12 + iter-13)
//!
//! The predicate is gated on FOUR conjoined conditions so a
//! regression on an unrelated code path cannot silently fall under
//! the allowlist:
//!
//! 1. Target OS is Windows. POSIX `unlink` does not invalidate
//!    already-resolved file handles the way Windows does, so any
//!    rename failure on Linux / macOS is a real bug.
//! 2. The error is [`XRaftError::Storage`].
//! 3. The payload carries the literal `"final hard-state persist
//!    failed"` wrapper — proof that the error came out of the
//!    driver's `run_shutdown` final-persist branch (see
//!    [`Driver::run_shutdown`](crate::driver::Driver)), NOT a
//!    mid-test commit-time persist on a healthy cluster.
//! 4. The payload carries the literal `quorum-state.tmp` source
//!    filename. The production driver's `HardStatePersistencer` is
//!    the only writer that uses that exact `<file>.tmp` → `<file>`
//!    source/destination pair against the `quorum-state` target,
//!    so an unrelated atomic-rename elsewhere (snapshot tempfile,
//!    log-segment rotation, ...) cannot accidentally match.
//!
//! [`Driver`]: crate::driver::Driver

use xraft_core::error::XRaftError;

/// Returns `true` iff `err` is the Windows-only tempdir-teardown
/// race documented in the module-level comment.
///
/// **All four** conditions must hold for a match — see the module
/// doc — so it is structurally impossible for an unrelated bug to
/// be silenced.
pub fn is_allowed_teardown_noise(err: &XRaftError) -> bool {
    #[cfg(target_os = "windows")]
    {
        if let XRaftError::Storage(msg) = err {
            let has_rename = msg.contains("rename");
            let has_missing_path = msg.contains("os error 3") || msg.contains("os error 2");
            // Require the SOURCE side of the atomic rename. The
            // production `HardStatePersistencer` writes
            // `quorum-state.tmp` then renames it to `quorum-state`;
            // no other code path uses that exact `<X>.tmp -> <X>`
            // shape against the `quorum-state` target, so this
            // substring uniquely identifies the documented race.
            let has_final_persist_source = msg.contains("quorum-state.tmp");
            // Require the driver-side `"final hard-state persist
            // failed"` wrapper that `xraft-server/src/driver.rs::
            // run_shutdown` (around line 4191) is the ONLY caller
            // to add. This ensures a MID-test persist failure
            // (e.g. a buggy commit-time persist on a healthy
            // cluster) is never mistaken for the shutdown-only
            // tempdir race — the wrapper is hard evidence that
            // this came out of the driver's `run_shutdown`
            // final-persist branch.
            let has_final_persist_wrapper = msg.contains("final hard-state persist failed");
            return has_rename
                && has_missing_path
                && has_final_persist_source
                && has_final_persist_wrapper;
        }
        false
    }
    #[cfg(not(target_os = "windows"))]
    {
        // Non-Windows targets do not exhibit the tempdir-teardown
        // rename race (POSIX `unlink` does not invalidate already-
        // resolved file handles the way Windows does), so any
        // rename failure on those platforms is a real bug.
        let _ = err;
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(target_os = "windows")]
    #[test]
    fn allows_windows_tempdir_race_os_error_3() {
        let err = XRaftError::Storage(
            "final hard-state persist failed: storage error: \
             rename 'C:\\Users\\foo\\Temp\\.tmp123\\state\\quorum-state.tmp' -> \
             'C:\\Users\\foo\\Temp\\.tmp123\\state\\quorum-state': The system cannot \
             find the path specified. (os error 3)"
                .into(),
        );
        assert!(is_allowed_teardown_noise(&err));
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn allows_windows_tempdir_race_os_error_2() {
        let err = XRaftError::Storage(
            "final hard-state persist failed: storage error: \
             rename 'C:\\foo\\state\\quorum-state.tmp' -> \
             'C:\\foo\\state\\quorum-state' failed: ... (os error 2)"
                .into(),
        );
        assert!(is_allowed_teardown_noise(&err));
    }

    #[test]
    fn rejects_arbitrary_storage_error() {
        let err = XRaftError::Storage("disk full".into());
        assert!(!is_allowed_teardown_noise(&err));
    }

    #[test]
    fn rejects_storage_error_without_rename() {
        // os error 3 alone (no rename context) is NOT a teardown race.
        let err =
            XRaftError::Storage("final hard-state persist failed: read failed: os error 3".into());
        assert!(!is_allowed_teardown_noise(&err));
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn rejects_rename_without_quorum_state_tmp_source() {
        // A rename failure whose source is NOT `quorum-state.tmp`
        // (e.g. a future snapshot temp file) must NOT be swallowed
        // even on Windows, because the documented race is specific
        // to the HardStatePersistencer.
        let err = XRaftError::Storage(
            "final hard-state persist failed: storage error: \
             rename 'C:\\foo\\snapshot-000123.tmp' -> 'C:\\foo\\snapshot-000123' \
             failed: ... (os error 3)"
                .into(),
        );
        assert!(!is_allowed_teardown_noise(&err));
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn rejects_rename_with_only_quorum_state_target() {
        // Matching only the destination `quorum-state` (without the
        // `.tmp` source) is NOT enough — that pattern could come
        // from a future "rename quorum-state -> backup" path; only
        // the canonical `<X>.tmp -> <X>` shape is allowlisted.
        let err = XRaftError::Storage(
            "final hard-state persist failed: storage error: \
             rename 'C:\\foo\\quorum-state' -> 'C:\\foo\\quorum-state.bak' \
             failed: ... (os error 3)"
                .into(),
        );
        assert!(!is_allowed_teardown_noise(&err));
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn rejects_mid_test_persist_failure_without_run_shutdown_wrapper() {
        // A Storage error that contains every other allowlist
        // marker but LACKS the driver's `"final hard-state persist
        // failed"` wrapper is proof that the failure came from a
        // mid-test persist code path, NOT from `run_shutdown`. It
        // must panic the test rather than be silenced.
        let err = XRaftError::Storage(
            "storage error: \
             rename 'C:\\foo\\state\\quorum-state.tmp' -> \
             'C:\\foo\\state\\quorum-state': ... (os error 3)"
                .into(),
        );
        assert!(!is_allowed_teardown_noise(&err));
    }

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn rejects_everything_on_non_windows() {
        // The predicate is Windows-only. Even a payload that LOOKS
        // like the documented Windows race is rejected on POSIX
        // targets because POSIX `unlink` does not cause the
        // underlying rename to fail in the same way; if a Linux
        // test surfaces such a message it indicates a real bug,
        // not the documented teardown race.
        let err = XRaftError::Storage(
            "final hard-state persist failed: storage error: \
             rename 'X/quorum-state.tmp' -> 'X/quorum-state': os error 2"
                .into(),
        );
        assert!(!is_allowed_teardown_noise(&err));
    }

    #[test]
    fn rejects_non_storage_errors() {
        assert!(!is_allowed_teardown_noise(&XRaftError::Transport(
            "final hard-state persist failed: rename quorum-state.tmp (os error 3)".into()
        )));
        assert!(!is_allowed_teardown_noise(&XRaftError::Shutdown));
        assert!(!is_allowed_teardown_noise(&XRaftError::ElectionTimeout));
    }
}
