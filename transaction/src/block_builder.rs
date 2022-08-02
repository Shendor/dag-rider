use bytes::Bytes;
use log::{debug, error, info};
use tokio::sync::mpsc::{Receiver};

use model::block::{Block, Transaction};
use model::committee::Committee;
use network::ReliableSender;

use crate::transaction_coordinator::BlockMessage;

const BATCH_SIZE: usize = 10;

pub struct BlockBuilder {
    committee: Committee,
    transaction_receiver: Receiver<Transaction>,
    current_transactions: Vec<Transaction>,
    network: ReliableSender,
}

impl BlockBuilder {
    pub fn spawn(
        transaction_receiver: Receiver<Transaction>,
        committee: Committee,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                transaction_receiver,
                current_transactions: vec![],
                network: ReliableSender::new(),
            }
                .run()
                .await;
        });
    }

    async fn run(&mut self) {
        while let Some(transaction) = self.transaction_receiver.recv().await {
            info!("BlockBuilder received transaction {:?}", transaction);
            self.current_transactions.push(transaction);

            if self.current_transactions.len() >= BATCH_SIZE {
                info!("BlockBuilder has enough transactions to make a block. Broadcast it to others");
                let message = BlockMessage::Block(Block::new(self.current_transactions.drain(..).collect()));
                let serialized = bincode::serialize(&message).expect("Failed to serialize the block");

                // Broadcast the block through the network.
                let bytes = Bytes::from(serialized.clone());
                let handlers = self.network.broadcast(self.committee.get_block_receiver_addresses(), bytes).await;
                for h in handlers {
                    if let Err(e) = h.await {
                        error!("Broadcast of the block was not successful: {:?}", e);
                    }
                }
            }
        }
    }
}
