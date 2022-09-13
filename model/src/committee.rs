use std::collections::BTreeMap;
use std::fs;
use std::net::SocketAddr;
use ed25519_dalek::Keypair;
use serde::{Deserialize, Serialize};
use serde::de::DeserializeOwned;

pub type Id = u32;
pub type NodePublicKey = [u8; 32];

#[derive(Clone, Deserialize)]
pub struct VertexServiceAddress {
    pub vertex_address: SocketAddr,
    pub block_proposal_address: SocketAddr,
}

#[derive(Clone, Deserialize)]
pub struct BlockServiceAddress {
    pub tx_address: SocketAddr,
    pub block_address: SocketAddr,
}

#[derive(Clone, Deserialize)]
pub struct Validator {
    pub public_key: NodePublicKey,
    pub vertex_service_address: VertexServiceAddress,
    pub block_service_address: BlockServiceAddress,
}

impl Validator {
    pub fn new(keypair: &str, vertex_addr: &str, block_proposal_addr: &str, tx_addr: &str, block_addr: &str) -> Self {
        let keypair = Validator::create_keypair(String::from(keypair));
        let public_key = Validator::create_node_public_key_from(&keypair);
        Self {
            public_key,
            vertex_service_address: VertexServiceAddress {
                vertex_address: vertex_addr.parse().unwrap(),
                block_proposal_address: block_proposal_addr.parse().unwrap(),
            },
            block_service_address: BlockServiceAddress {
                tx_address: tx_addr.parse().unwrap(),
                block_address: block_addr.parse().unwrap(),
            },
        }
    }

    fn create_keypair(kps: String) -> Keypair {
        let bytes = hex::decode(kps).unwrap();
        return Keypair::from_bytes(&bytes).unwrap();
    }

    fn create_node_public_key_from(keypair: &Keypair) -> NodePublicKey {
        let encoded = bincode::serialize(&keypair.public).unwrap();
        blake3::hash(&encoded).as_bytes().clone()
    }
}

#[derive(Clone, Deserialize)]
pub struct Committee {
    pub validators: BTreeMap<Id, Validator>,
}

impl Committee {
    pub fn from_file(path: &str) -> Self {
        let data = fs::read(path).expect("Failed to open committee file");
        let json: serde_json::Value =
            serde_json::from_slice(data.as_slice()).expect("Failed to parse committee file");

        let mut validators = BTreeMap::new();

        for (key, value) in json["validators"].as_object().unwrap() {
            validators.insert(key.parse::<Id>().unwrap(),
                              Validator::new(
                                  value.get("keypair").unwrap().as_str().unwrap(),
                                  value.get("vertex_address").unwrap().as_str().unwrap(),
                                  value.get("block_proposal_address").unwrap().as_str().unwrap(),
                                  value.get("tx_address").unwrap().as_str().unwrap(),
                                  value.get("block_address").unwrap().as_str().unwrap(),
                              ));
        }

        Committee {
            validators
        }
    }

    pub fn size(&self) -> usize {
        self.validators.len()
    }

    pub fn quorum_threshold(&self) -> usize {
        2 * self.size() / 3 + 1
    }

    /// Returns the stake required to reach availability (f+1).
    pub fn validity_threshold(&self) -> usize {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (N + 2) / 3 = f + 1 + k/3 = f + 1
        ((self.size() + 2) / 3) as usize
    }

    pub fn get_node_address(&self, id: Id) -> Option<SocketAddr> {
        match self.validators.get(&id) {
            Some(v) => Some(v.vertex_service_address.vertex_address),
            None => None
        }
    }

    pub fn get_vertex_address_by_key(&self, node_key: &NodePublicKey) -> Option<SocketAddr> {
        self.validators.iter().find(|(_, v)| v.public_key == *node_key).map(|(_, v)| v.vertex_service_address.vertex_address)
    }

    pub fn get_vertex_addresses(&self) -> Vec<SocketAddr> {
        self.validators.iter().map(|v| v.1.vertex_service_address.vertex_address).collect()
    }

    pub fn get_tx_receiver_address(&self, id: Id) -> Option<SocketAddr> {
        self.validators.get(&id).map(|v| v.block_service_address.tx_address)
    }

    pub fn get_tx_receiver_address_by_key(&self, node_key: &NodePublicKey) -> Option<SocketAddr> {
        self.validators.iter().find(|(_, v)| v.public_key == *node_key).map(|(_, v)| v.block_service_address.tx_address)
    }

    pub fn get_tx_receiver_addresses(&self) -> Vec<SocketAddr> {
        self.validators.iter().map(|v| v.1.block_service_address.tx_address).collect()
    }

    pub fn get_block_receiver_address(&self, id: Id) -> Option<SocketAddr> {
        self.validators.get(&id).map(|v| v.block_service_address.block_address)
    }

    pub fn get_block_receiver_address_by_key(&self, node_key: &NodePublicKey) -> Option<SocketAddr> {
        self.validators.iter().find(|(_, v)| v.public_key == *node_key).map(|(_, v)| v.block_service_address.block_address)
    }

    pub fn get_block_proposal_address_by_key(&self, node_key: &NodePublicKey) -> Option<SocketAddr> {
        self.validators.iter().find(|(_, v)| v.public_key == *node_key).map(|(_, v)| v.vertex_service_address.block_proposal_address)
    }

    pub fn get_block_receiver_addresses(&self) -> Vec<SocketAddr> {
        self.validators.iter().map(|v| v.1.block_service_address.block_address).collect()
    }

    pub fn get_block_receiver_addresses_but_me(&self, node_key: &NodePublicKey) -> Vec<SocketAddr> {
        self.validators.iter().filter(|(_, v)| v.public_key != *node_key).map(|v| v.1.block_service_address.block_address).collect()
    }

    pub fn get_vertex_addresses_but_me(&self, node_key: &NodePublicKey) -> Vec<SocketAddr> {
        self.validators.iter().filter(|(_, v)| v.public_key != *node_key).map(|v| v.1.vertex_service_address.vertex_address).collect()
    }

    pub fn get_nodes_keys(&self) -> Vec<NodePublicKey> {
        self.validators.iter().map(|v| v.1.public_key.clone()).collect()
    }

    pub fn get_node_key(&self, id: Id) -> Option<NodePublicKey> {
        self.validators.get(&id).map(|v| v.public_key)
    }

    pub fn leader(&self, seed: usize) -> NodePublicKey {
        let mut keys: Vec<_> = self.validators.iter().map(|(_, v)| v.public_key).collect();
        keys.sort();
        keys[seed % self.size()].clone()
    }
}