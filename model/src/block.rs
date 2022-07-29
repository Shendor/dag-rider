use serde::{Deserialize, Serialize};

pub type Transaction = Vec<u8>;

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
pub struct Block {
    pub transactions: Vec<Transaction>
}

impl Block {
    pub fn new(transactions: Vec<Transaction>) -> Self {
        Self {
            transactions
        }
    }
}