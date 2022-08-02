use serde::{Deserialize, Serialize};

pub type Transaction = Vec<u8>;
pub type BlockHash = [u8; 32];

#[derive(Clone, Serialize, Deserialize, Default, Debug)]
pub struct Block {
    pub hash: BlockHash,
    pub transactions: Vec<Transaction>,
}

impl Block {
    pub fn new(transactions: Vec<Transaction>) -> Self {
        let encoded = bincode::serialize(&transactions).unwrap();
        let hash = blake3::hash(&encoded).as_bytes().clone();
        Self {
            hash,
            transactions
        }
    }

    pub fn hash(&self) -> BlockHash {
        self.hash
    }
}