use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use std::convert::TryInto;
use std::net::SocketAddr;
use bytes::Bytes;
use log::info;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{Receiver, Sender};
use model::block::BlockHash;
use model::committee::NodePublicKey;
use network::SimpleSender;
use storage::Storage;

#[derive(Debug, Serialize, Deserialize)]
pub enum BlockMessage {
    ProposeBlock(BlockHash, NodePublicKey),
    RegisterBlock(BlockHash, NodePublicKey),
}

pub struct BlockProcessor;

impl BlockProcessor {
    pub fn spawn(
        node_key: NodePublicKey,
        block_proposal_address: SocketAddr,
        mut storage: Storage,
        mut serialized_block_receiver: Receiver<(BlockHash, Vec<u8>, NodePublicKey)>,
    ) {
        tokio::spawn(async move {
            let mut network: SimpleSender = SimpleSender::new();
            while let Some((block_hash, serialized_block, owner)) = serialized_block_receiver.recv().await {
                // Store the block in the DB.
                storage.write(block_hash.to_vec(), serialized_block).await;

                let message = if node_key == owner {
                    // Message to send the block to the Vertex Service which will be assigned and proposed to consensus.
                    BlockMessage::ProposeBlock(block_hash, owner)
                } else {
                    BlockMessage::RegisterBlock(block_hash, owner)
                };
                let serialized_message = bincode::serialize(&message).expect("Failed to serialize BlockMessage");

                network.send(block_proposal_address, Bytes::from(serialized_message)).await;
                info!("Block has been sent to the Vertex Service for proposal");
            }
        });
    }
}
