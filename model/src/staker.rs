use std::collections::HashMap;
use ed25519_dalek::Keypair;

pub type TokenAmount = u64;
pub type Id = u32;
pub type NodePublicKey = [u8; 32];

pub struct InitialStaker {
    pub node_id: Id,
    pub keypair: Keypair,
    pub stake: TokenAmount,
    pub public_key: NodePublicKey
}

impl InitialStaker {

    pub fn new(node_id: Id, keypair: &str, stake: TokenAmount) -> Self {
        let keypair = InitialStakers::create_keypair(String::from(keypair));
        let public_key = InitialStaker::create_node_public_key_from(&keypair);
        InitialStaker {
            node_id,
            keypair,
            stake,
            public_key
        }
    }

    fn create_node_public_key_from(keypair: &Keypair) -> NodePublicKey {
        let encoded = bincode::serialize(&keypair.public).unwrap();
        blake3::hash(&encoded).as_bytes().clone()
    }
}

impl Clone for InitialStaker {

    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id.clone(),
            keypair: Keypair::from_bytes(&self.keypair.to_bytes()).unwrap(),
            stake: self.stake.clone(),
            public_key: self.public_key.clone()
        }
    }
}

pub struct InitialStakers {
    pub stakers: HashMap<Id, InitialStaker>
}

impl InitialStakers {

    pub fn new() -> Self {
        let mut stakers = HashMap::new();
        stakers.insert(1, InitialStaker::new(1, "ad7f2ee3958a7f3fa2c84931770f5773ef7694fdd0bb217d90f29a94199c9d7307ca3851515c89344639fe6a4077923068d1d7fc6106701213c61d34ef8e9416", 1000));
        stakers.insert(2, InitialStaker::new(2, "5a353c630d3faf8e2d333a0983c1c71d5e9b6aed8f4959578fbeb3d3f3172886393b576de0ac1fe86a4dd416cf032543ac1bd066eb82585f779f6ce21237c0cd", 1000));
        stakers.insert(3, InitialStaker::new(3, "6f4b736b9a6894858a81696d9c96cbdacf3d49099d212213f5abce33da18716f067f8a2b9aeb602cd4163291ebbf39e0e024634f3be19bde4c490465d9095a6b", 1000));

        InitialStakers {
            stakers
        }
    }

    pub fn get(&self, node_id: Id) -> Option<&InitialStaker> {
        self.stakers.get(&node_id)
    }

    fn create_keypair(kps: String) -> Keypair {
        let bytes = hex::decode(kps).unwrap();
        return Keypair::from_bytes(&bytes).unwrap();
    }
}