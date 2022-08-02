use std::collections::{BTreeMap};
use std::fmt;
use serde::{Deserialize, Serialize};
use crate::block::Block;
use crate::committee::NodePublicKey;
use crate::Round;

pub type VertexHash = [u8; 32];

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Vertex {
    /// Vertex unique identifier
    hash: VertexHash,
    /// source of the header (the node which created it)
    owner: NodePublicKey,
    block: Block,
    parents: BTreeMap<VertexHash, Round>,
    round: Round,
}

impl Vertex {
    pub fn new(owner: NodePublicKey,
               round: Round,
               block: Block,
               parents: BTreeMap<VertexHash, Round>,
    ) -> Self {
        let vertex = Self {
            owner,
            round,
            block,
            parents,
            hash: VertexHash::default(),
        };
        let encoded = bincode::serialize(&vertex).unwrap();
        let hash = blake3::hash(&encoded).as_bytes().clone();
        Self {
            hash,
            ..vertex
        }
    }

    pub fn genesis(nodes: Vec<NodePublicKey>) -> Vec<Self> {
        nodes.iter().map(|owner| Vertex::new(*owner, 1, Block::default(), BTreeMap::new())).collect()
    }

    pub fn add_parent(&mut self, parent_vertex_hash: VertexHash, round: Round) {
        self.parents.insert(parent_vertex_hash, round);
    }

    pub fn get_strong_parents(&self) -> BTreeMap<VertexHash, Round> {
        self.parents.iter()
            .filter(|(_, r)| self.is_previous_round(r))
            .map(|(h, r)| (h.clone(), r.clone()))
            .collect::<BTreeMap<VertexHash, Round>>()
    }

    pub fn get_all_parents(&self) -> BTreeMap<VertexHash, Round> {
        self.parents.clone()
    }

    pub fn is_weak_parent(&self, vertex_hash: &VertexHash) -> bool {
        match self.parents.get(vertex_hash) {
            // difference between round of parent vertex and current round should not be 1
            Some(r) => !self.is_previous_round(r),
            None => false
        }
    }

    pub fn round(&self) -> Round {
        self.round
    }

    pub fn parents(&self) -> &BTreeMap<VertexHash, Round> {
        &self.parents
    }

    pub fn owner(&self) -> NodePublicKey {
        self.owner
    }

    pub fn hash(&self) -> VertexHash {
        self.hash
    }

    fn is_previous_round(&self, previous_round: &Round) -> bool {
        self.round - previous_round == 1
    }
}

impl fmt::Display for Vertex {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "Vertex ({}, {}) [owner: {}]",
            self.round(),
            base64::encode(self.hash()),
            base64::encode(self.owner())
        )
    }
}

impl fmt::Debug for Vertex {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(
            f,
            "Vertex ({}, {}) [owner: {}]",
            self.round(),
            base64::encode(self.hash()),
            base64::encode(self.owner())
        )
    }
}

impl PartialEq for Vertex {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}