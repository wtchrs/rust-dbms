use thiserror::Error;
use crate::buffer;

#[derive(Debug, Error)]
pub enum BTreeError {
    #[error("duplicate key")]
    DuplicateKey,
    #[error(transparent)]
    Buffer(#[from] buffer::BufferError),
}
