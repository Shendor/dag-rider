use log::{debug};
use tokio::sync::mpsc::{channel, Sender};

use model::block::{Block, BlockHash};
use model::committee::{Committee, Id};
use model::DEFAULT_CHANNEL_CAPACITY;
use network::Receiver;
use storage::Storage;
use crate::block_builder::BlockBuilder;
use crate::handler::{ReceiveBlockHandler, ReceiveTxHandler};

pub struct TransactionService;

impl TransactionService {
    pub fn spawn(
        node_id: Id,
        committee: Committee,
        storage: Storage,
        block_sender: Sender<BlockHash>,
    ) {
        let (transaction_sender, transaction_receiver) = channel(DEFAULT_CHANNEL_CAPACITY);

        let tx_address = committee.get_tx_receiver_address(node_id).unwrap();
        debug!("Start listening for transactions on {:?}", tx_address);
        Receiver::spawn(
            tx_address,
            ReceiveTxHandler { transaction_sender },
        );

        let address = committee.get_block_receiver_address(node_id).unwrap();
        debug!("Start listening for blocks on {:?}", address);
        Receiver::spawn(
            address,
            ReceiveBlockHandler { block_sender, storage },
        );

        BlockBuilder::spawn(
            transaction_receiver,
            committee,
        );
    }
}