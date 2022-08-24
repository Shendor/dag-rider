use std::collections::{BTreeMap};
use std::fmt;
use std::time::SystemTime;
use serde::{Deserialize, Serialize};
use crate::block::BlockHash;
use crate::committee::NodePublicKey;
use crate::{Round, Timestamp};

pub type VertexHash = [u8; 32];

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct Vertex {
    /// Vertex unique identifier
    hash: VertexHash,
    /// source of the vertex (the node which created it)
    owner: NodePublicKey,
    blocks: Vec<BlockHash>,
    parents: BTreeMap<VertexHash, (Round, Timestamp)>,
    round: Round,
    timestamp: Timestamp,
}

impl Vertex {
    pub fn new(owner: NodePublicKey,
               round: Round,
               blocks: Vec<BlockHash>,
               parents: BTreeMap<VertexHash, (Round, Timestamp)>,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("Failed to measure time")
            .as_millis();
        let vertex = Self {
            owner,
            round,
            blocks,
            parents,
            timestamp: now,
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
        nodes.iter().map(|owner| Vertex::new(*owner, 1, vec![], BTreeMap::new())).collect()
    }

    pub fn add_parent(&mut self, parent_vertex_hash: VertexHash, round: Round, parent_vertex_time: Timestamp) {
        self.parents.insert(parent_vertex_hash, (round, parent_vertex_time));
    }

    pub fn get_strong_parents(&self) -> BTreeMap<VertexHash, (Round, Timestamp)> {
        self.parents.iter()
            .filter(|(_, (r, _))| self.is_previous_round(r))
            .map(|(h, (r, t))| (h.clone(), (r.clone(), t.clone())))
            .collect::<BTreeMap<VertexHash, (Round, Timestamp)>>()
    }

    pub fn get_all_parents(&self) -> BTreeMap<VertexHash, (Round, Timestamp)> {
        self.parents.clone()
    }

    pub fn is_weak_parent(&self, vertex_hash: &VertexHash) -> bool {
        match self.parents.get(vertex_hash) {
            // difference between round of parent vertex and current round should not be 1
            Some((r, _)) => !self.is_previous_round(r),
            None => false
        }
    }

    pub fn round(&self) -> Round {
        self.round
    }

    pub fn parents(&self) -> &BTreeMap<VertexHash, (Round, Timestamp)> {
        &self.parents
    }

    pub fn owner(&self) -> NodePublicKey {
        self.owner
    }

    pub fn encoded_owner(&self) -> String {
        base64::encode(self.owner())
    }

    pub fn hash(&self) -> VertexHash {
        self.hash
    }

    pub fn encoded_hash(&self) -> String {
        base64::encode(self.hash())
    }

    pub fn created_time(&self) -> Timestamp {
        self.timestamp
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