use std::collections::{BTreeSet};
use ed25519_dalek::{Signature};
use std::fmt;
use std::fmt::Display;
use serde::{Deserialize, Serialize};
use thiserror::private::DisplayAsDisplay;
use crate::Round;
use crate::staker::NodePublicKey;

pub type VertexHash = [u8; 32];

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Header {
    pub hash: VertexHash,
    pub owner: NodePublicKey,
    pub parents: BTreeSet<VertexHash>,
    pub round: Round
}

impl Header {
    pub async fn new(
        owner: NodePublicKey,
        round: Round,
        parents: BTreeSet<VertexHash>,
    ) -> Self {
        let header = Self {
            owner,
            round,
            parents,
            hash: VertexHash::default(),
        };
        Self {
            hash: header.hash(),
            ..header
        }
    }

    fn hash(&self) -> VertexHash {
        self.hash
    }
}

impl fmt::Display for Header {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: Header - [{}, {}]",
            self.round,
            base64::encode(self.hash()),
            base64::encode(self.owner),
        )
    }
}

impl fmt::Debug for Header {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Header: {} - {}", self.round, base64::encode(self.hash()))
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Vertex {
    pub header: Header,
    pub votes: Vec<NodePublicKey>,
}

impl Vertex {
    pub fn genesis(nodes: Vec<NodePublicKey>) -> Vec<Self> {
        nodes.iter().map(|name| Self {
            header: Header {
                owner: *name,
                ..Header::default()
            },
            votes: vec![],
        }).collect()
    }

    pub fn round(&self) -> Round {
        self.header.round
    }

    pub fn owner(&self) -> NodePublicKey {
        self.header.owner
    }

    pub fn hash(&self) -> VertexHash {
        self.header.hash()
    }
}

impl fmt::Display for Vertex {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "{}: Vertex - [{}, {}]",
            self.round(),
            base64::encode(self.owner()),
            base64::encode(self.hash())
        )
    }
}

impl fmt::Debug for Vertex {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Vertex: {} - {}", self.round(), base64::encode(self.owner()))
    }
}