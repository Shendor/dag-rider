use serde::{Deserialize, Serialize};

pub type Transaction = Vec<u8>;

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Block {
    pub transactions: Vec<Transaction>
}