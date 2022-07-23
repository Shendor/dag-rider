use std::fmt;
use std::fmt::Display;
use ed25519_dalek::Signature;
use serde::{Deserialize, Serialize};
use thiserror::private::DisplayAsDisplay;
use crate::Round;
use crate::staker::NodePublicKey;
use crate::vertex::{Header, VertexHash};

#[derive(Clone, Serialize, Deserialize)]
pub struct Vote {
    pub vertex_hash: VertexHash,
    pub round: Round,
    pub origin: NodePublicKey,
    pub owner: NodePublicKey,
    pub signature: Option<Signature>,
}

impl Vote {
    pub async fn new(
        header: &Header,
        owner: &NodePublicKey,
    ) -> Self {
        Self {
            vertex_hash: header.hash.clone(),
            round: header.round,
            origin: header.owner,
            owner: *owner,
            signature: None,
        }
    }
}

impl fmt::Display for Vote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: Vote - [{}, {}]",
            self.round,
            base64::encode(self.vertex_hash),
            base64::encode(self.owner),
        )
    }
}

impl fmt::Debug for Vote {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Vote: {} - {}", self.round, base64::encode(self.vertex_hash))
    }
}