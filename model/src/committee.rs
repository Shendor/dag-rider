use std::collections::BTreeMap;
use std::net::SocketAddr;
use ed25519_dalek::Keypair;
use serde::{Deserialize};

pub type Id = u32;
pub type NodePublicKey = [u8; 32];

#[derive(Clone, Deserialize)]
pub struct Validator {
    pub address: SocketAddr,
    pub tx_address: SocketAddr,
    pub block_address: SocketAddr,
    pub public_key: NodePublicKey,
}

impl Validator {
    pub fn new(keypair: &str, port: u16, tx_port: u16, block_port: u16) -> Self {
        let keypair = Validator::create_keypair(String::from(keypair));
        let public_key = Validator::create_node_public_key_from(&keypair);
        Self {
            address: SocketAddr::new("0.0.0.0".parse().unwrap(), port),
            tx_address: SocketAddr::new("0.0.0.0".parse().unwrap(), tx_port),
            block_address: SocketAddr::new("0.0.0.0".parse().unwrap(), block_port),
            public_key,
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
    pub fn default() -> Self {
        let mut validators = BTreeMap::new();
        validators.insert(1, Validator::new(
            "ad7f2ee3958a7f3fa2c84931770f5773ef7694fdd0bb217d90f29a94199c9d7307ca3851515c89344639fe6a4077923068d1d7fc6106701213c61d34ef8e9416",
            1234, 1244, 1254));
        validators.insert(2, Validator::new(
            "5a353c630d3faf8e2d333a0983c1c71d5e9b6aed8f4959578fbeb3d3f3172886393b576de0ac1fe86a4dd416cf032543ac1bd066eb82585f779f6ce21237c0cd",
            1235, 1245, 1255));
        validators.insert(3, Validator::new(
            "6f4b736b9a6894858a81696d9c96cbdacf3d49099d212213f5abce33da18716f067f8a2b9aeb602cd4163291ebbf39e0e024634f3be19bde4c490465d9095a6b",
            1236, 1246, 1256));
        validators.insert(4, Validator::new(
            "3ae38eec96146c241f6cadf01995af14f027b23b8fecbc77dbc2e3ed5fec6fc3fb4fe5534f7affc9a8f1d99e290fdb91cc26777edd6fae480cad9f735d1b3680",
            1237, 1247, 1257));

        Self {
            validators
        }
    }

    pub fn size(&self) -> usize {
        self.validators.len()
    }

    pub fn quorum_threshold(&self) -> usize {
        self.size() - 1
    }

    /// Returns the stake required to reach availability (f+1).
    pub fn validity_threshold(&self) -> usize {
        // If N = 3f + 1 + k (0 <= k < 3)
        // then (N + 2) / 3 = f + 1 + k/3 = f + 1
        ((self.validators.len() + 2) / 3) as usize
    }

    pub fn get_node_address(&self, id: Id) -> Option<SocketAddr> {
        match self.validators.get(&id) {
            Some(v) => Some(v.address),
            None => None
        }
    }

    pub fn get_node_address_by_key(&self, node_key: &NodePublicKey) -> Option<SocketAddr> {
        self.validators.iter().find(|(_, v)| v.public_key == *node_key).map(|(_, v)| v.address)
    }

    pub fn get_node_addresses(&self) -> Vec<SocketAddr> {
        self.validators.iter().map(|v| v.1.address).collect()
    }

    pub fn get_tx_receiver_address(&self, id: Id) -> Option<SocketAddr> {
        self.validators.get(&id).map(|v| v.tx_address)
    }

    pub fn get_tx_receiver_addresses(&self) -> Vec<SocketAddr> {
        self.validators.iter().map(|v| v.1.tx_address).collect()
    }

    pub fn get_block_receiver_address(&self, id: Id) -> Option<SocketAddr> {
        self.validators.get(&id).map(|v| v.block_address)
    }

    pub fn get_block_receiver_address_by_key(&self, node_key: NodePublicKey) -> Option<SocketAddr> {
        self.validators.iter().find(|(_, v)| v.public_key == node_key).map(|(_, v)| v.block_address)
    }

    pub fn get_block_receiver_addresses(&self) -> Vec<SocketAddr> {
        self.validators.iter().map(|v| v.1.block_address).collect()
    }

    pub fn get_node_addresses_but_me(&self, node_key: &NodePublicKey) -> Vec<SocketAddr> {
        self.validators.iter().filter(|(_, v)| v.public_key != *node_key).map(|v| v.1.address).collect()
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