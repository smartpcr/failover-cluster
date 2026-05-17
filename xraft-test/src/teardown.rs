//! Re-export shim for the canonical teardown-noise allowlist.
//!
//! # Why this is a shim
//!
//! The canonical predicate body lives in `xraft-server` because
//! `xraft-server`'s integration tests need to consume the same
//! allowlist (see `xraft-server/tests/stage_7_2_static_voter_set.rs`
//! `assert_clean_shutdown` helper). `xraft-server` cannot depend on
//! `xraft-test` (the reverse dep already exists in `Cargo.toml`), so
//! the canonical definition lives in
//! [`xraft_server::teardown`](xraft_server::teardown). This module
//! is kept as a thin re-export so the rest of `xraft-test` keeps
//! reading `crate::teardown::is_allowed_teardown_noise` without
//! source churn at the callsites.

pub use xraft_server::teardown::is_allowed_teardown_noise;

#[cfg(test)]
mod tests {
    use super::is_allowed_teardown_noise;
    use xraft_core::error::XRaftError;

    #[test]
    fn shim_delegates_to_xraft_server() {
        // Smoke test: the shim must reject an arbitrary Storage
        // error (proves it is hitting the real predicate body,
        // not e.g. returning `true` unconditionally).
        assert!(!is_allowed_teardown_noise(&XRaftError::Storage(
            "disk full".into()
        )));
        assert!(!is_allowed_teardown_noise(&XRaftError::Shutdown));
    }
}
