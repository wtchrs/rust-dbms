use crate::buffer;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BTreeError {
    #[error("duplicate key")]
    DuplicateKey,
    #[error(transparent)]
    Buffer(#[from] buffer::BufferError),
}
