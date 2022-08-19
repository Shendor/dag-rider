use storage::StoreError;

pub type VertexResult<T> = core::result::Result<T, VertexError>;

#[derive(Debug, thiserror::Error)]
pub enum VertexError {
    #[error("Not enough parents in vertex {0}. Must be at least {}")]
    VertexParentsQuorumFailed(String, usize),

    #[error("Storage failure: {0}")]
    StoreError(#[from] StoreError)
}
