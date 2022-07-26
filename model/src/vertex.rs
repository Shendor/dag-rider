use std::collections::{BTreeMap, BTreeSet};
use ed25519_dalek::{Signature};
use std::fmt;
use std::fmt::Display;
use serde::{Deserialize, Serialize};
use thiserror::private::DisplayAsDisplay;
use crate::block::Block;
use crate::Round;
use crate::staker::NodePublicKey;

pub type VertexHash = [u8; 32];

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Header {
    /// Vertex unique identifier
    pub hash: VertexHash,
    /// source of the header (the node which created it)
    pub owner: NodePublicKey,
    pub block: Block,
    pub parents: BTreeMap<VertexHash, Round>,
    pub round: Round
}

impl Header {
    pub async fn new(
        owner: NodePublicKey,
        round: Round,
        block: Block,
        parents: BTreeMap<VertexHash, Round>
    ) -> Self {
        let header = Self {
            owner,
            round,
            block,
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

    pub fn get_strong_parents(&self) -> &BTreeMap<VertexHash, Round> {
        self.header.parents.iter()
            .filter(|(h, r)| self.is_previous_round(r))
            .collect()
    }

    pub fn get_all_parents(&self) -> &BTreeMap<VertexHash, Round> {
        &self.header.parents
    }

    pub fn is_weak_parent(&self, vertex_hash: &VertexHash) -> bool {
        match self.header.parents.get(vertex_hash) {
            // difference between round of parent vertex and current round should not be 1
            Some(r) => !self.is_previous_round(r),
            None => false
        }
    }

    fn is_previous_round(&self, previous_round: &Round) -> bool {
        self.header.round - previous_round == 1
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

impl PartialEq for Vertex {
    fn eq(&self, other: &Self) -> bool {
        self.header.hash == other.header.hash
    }
}