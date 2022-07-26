use thiserror::Error;
pub const DEFAULT_CHANNEL_CAPACITY: usize = 1_000;
pub const MIN_QUORUM: u32 = 3;

pub type Round = u64;
pub type Wave = u64;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Serialization error: {0}")]
    SerializationError(#[from] Box<bincode::ErrorKind>),

    #[error("UnexpectedError {0}")]
    UnexpectedError(String),
}

pub mod config;
pub mod staker;
pub mod vertex;
pub mod vote;
pub mod block;
