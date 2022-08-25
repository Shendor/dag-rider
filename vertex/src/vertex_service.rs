use log::{debug, info};
use tokio::sync::mpsc::{channel, Receiver, Sender};

use model::committee::{Committee, NodePublicKey};
use model::{DEFAULT_CHANNEL_CAPACITY, Round};
use model::block::BlockHash;
use model::vertex::{Vertex};
use network::{Receiver as NetworkReceiver, ReliableSender};
use storage::Storage;
use crate::proposer::Proposer;
use crate::vertex_aggregator::VertexAggregator;

use crate::vertex_message_handler::{VertexMessage, VertexReceiverHandler};
use crate::vertex_synchronizer::SyncMessage;

pub struct VertexService;

impl VertexService {
    pub fn spawn(
        node_key: NodePublicKey,
        committee: Committee,
        storage: Storage,
        consensus_sender: Sender<Vertex>,
        gc_message_receiver: tokio::sync::broadcast::Receiver<Round>,
        block_receiver: Receiver<BlockHash>
    ) {
        let (vertex_sender, vertex_receiver) = channel::<Vertex>(DEFAULT_CHANNEL_CAPACITY);
        let (proposed_vertex_sender, proposed_vertex_receiver) = channel::<Vertex>(DEFAULT_CHANNEL_CAPACITY);
        let (parents_sender, parents_receiver) = channel::<(Vec<Vertex>, Round)>(DEFAULT_CHANNEL_CAPACITY);
        let (sync_message_sender, sync_message_receiver) = channel::<SyncMessage>(DEFAULT_CHANNEL_CAPACITY);
        let (vertex_sync_sender, vertex_sync_receiver) = channel::<Vertex>(DEFAULT_CHANNEL_CAPACITY);

        // Spawn the network receiver listening to vertices broadcast from the other nodes.
        let address = committee.get_node_address_by_key(&node_key)
            .expect("Node address was not found in the committee for the provided public key");
        NetworkReceiver::spawn(
            address,
            VertexReceiverHandler { vertex_sender },
        );
        info!("VertexReceiverHandler is listening to the messages on {}", address);

        VertexAggregator::spawn(
            node_key,
            committee.clone(),
            storage,
            vertex_receiver,
            parents_sender,
            proposed_vertex_receiver,
            consensus_sender,
            sync_message_sender,
            vertex_sync_receiver
        );

        Proposer::spawn(
            node_key,
            committee.clone(),
            parents_receiver,
            proposed_vertex_sender,
            block_receiver,
            ReliableSender::new()
        )

        /*VertexSynchronizer::spawn(
            node_key,
            committee.clone(),
            storage,
            sync_message_receiver,
            gc_message_receiver,
            vertex_sync_sender
        );*/
    }
}