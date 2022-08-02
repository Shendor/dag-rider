use std::error::Error;

use async_trait::async_trait;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use log::{debug, info, warn};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::{channel, Sender};

use model::block::{Block, Transaction};
use model::committee::{Committee, Id};
use model::DEFAULT_CHANNEL_CAPACITY;
use network::{MessageHandler, Receiver, Writer};

use crate::block_builder::BlockBuilder;

#[derive(Debug, Serialize, Deserialize)]
pub enum BlockMessage {
    Block(Block),
}

pub struct TransactionCoordinator;

impl TransactionCoordinator {
    pub fn spawn(
        node_id: Id,
        committee: Committee,
        block_sender: Sender<Block>,
    ) {
        let (transaction_to_block_builder_sender, transaction_receiver) = channel(DEFAULT_CHANNEL_CAPACITY);

        let tx_address = committee.get_tx_receiver_address(node_id).unwrap();
        debug!("Start listening for transactions on {:?}", tx_address);
        Receiver::spawn(
            tx_address,
            TxReceiverHandler { transaction_to_block_builder_sender },
        );

        let address = committee.get_block_receiver_address(node_id).unwrap();
        debug!("Start listening for blocks on {:?}", address);
        Receiver::spawn(
            address,
            BlockReceiverHandler { block_sender },
        );

        BlockBuilder::spawn(
            transaction_receiver,
            committee,
        );
    }
}

#[derive(Clone)]
struct TxReceiverHandler {
    transaction_to_block_builder_sender: Sender<Transaction>,
}

#[async_trait]
impl MessageHandler for TxReceiverHandler {
    async fn dispatch(&self, _writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>> {
        info!("TxReceiverHandler received transaction to process {:?}", message);
        // Send the transaction to the block builder.
        self.transaction_to_block_builder_sender
            .send(message.to_vec())
            .await
            .expect("Failed to send transaction");

        Ok(())
    }
}

#[derive(Clone)]
struct BlockReceiverHandler {
    block_sender: Sender<Block>,
}

#[async_trait]
impl MessageHandler for BlockReceiverHandler {
    async fn dispatch(&self, writer: &mut Writer, serialized: Bytes) -> Result<(), Box<dyn Error>> {
        // debug!("BlockReceiverHandler received the message and sends back the response");
        let _ = writer.send(Bytes::from("Ack")).await;

        match bincode::deserialize(&serialized) {
            Ok(BlockMessage::Block(block)) => {
                info!("BlockReceiverHandler received block to process with {} transactions and sends it to Consensus", block.transactions.len());
                self
                    .block_sender
                    .send(block)
                    .await
                    .expect("Failed to send block")
            },
            Err(e) => warn!("Serialization error: {}", e),
        }
        Ok(())
    }
}