//! Fail-closed stub for platforms without the required descriptor-relative and
//! atomic no-replace primitives.

use std::path::Path;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecureOutputCode {
    AtomicCreateUnavailable,
}

impl SecureOutputCode {
    pub const fn as_str(self) -> &'static str {
        "atomic_create_unavailable"
    }
}

impl std::fmt::Display for SecureOutputCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Error)]
#[error("{code}: {context}")]
pub struct SecureOutputError {
    pub code: SecureOutputCode,
    pub context: &'static str,
    pub published: bool,
}

pub fn write_secure_output(_path: &Path, _bytes: &[u8]) -> Result<(), SecureOutputError> {
    Err(SecureOutputError {
        code: SecureOutputCode::AtomicCreateUnavailable,
        context: "conditional no-replace publication is unavailable on this platform",
        published: false,
    })
}
