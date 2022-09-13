use log::{debug};
use tokio::sync::mpsc::{channel, Sender};

use model::block::BlockHash;
use model::committee::{Committee, Id, NodePublicKey};
use model::DEFAULT_CHANNEL_CAPACITY;
use network::Receiver;
use storage::Storage;
use crate::block_builder::BlockBuilder;
use crate::block_processor::BlockProcessor;
use crate::handler::{ReceiveBlockHandler, ReceiveTxHandler};

pub struct TransactionService;

impl TransactionService {
    pub fn spawn(
        node_key: NodePublicKey,
        committee: Committee,
        storage: Storage
    ) {
        let (transaction_sender, transaction_receiver) = channel(DEFAULT_CHANNEL_CAPACITY);
        let (serialized_block_sender, serialized_block_receiver) = channel(DEFAULT_CHANNEL_CAPACITY);

        let tx_address = committee.get_tx_receiver_address_by_key(&node_key).unwrap();
        debug!("Start listening for transactions on {:?}", tx_address);
        Receiver::spawn(
            tx_address,
            ReceiveTxHandler { transaction_sender },
        );

        let address = committee.get_block_receiver_address_by_key(&node_key).unwrap();
        debug!("Start listening for blocks on {:?}", address);
        Receiver::spawn(
            address,
            ReceiveBlockHandler {
                serialized_block_sender: serialized_block_sender.clone(),
                storage: storage.clone()
            },
        );

        let address = committee.get_block_proposal_address_by_key(&node_key).unwrap();
        BlockProcessor::spawn(
            node_key,
            address,
            storage,
            serialized_block_receiver
        );

        BlockBuilder::spawn(
            node_key,
            transaction_receiver,
            serialized_block_sender,
            committee,
        )
    }
}