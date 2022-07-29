use log::info;
use tokio::sync::mpsc::{Receiver, Sender};

use model::committee::{Committee, Id};
use model::vertex::{Vertex};
use network::{Receiver as NetworkReceiver, ReliableSender};

use crate::vertex_broadcaster::VertexBroadcaster;
use crate::vertex_message_handler::VertexReceiverHandler;

pub struct VertexCoordinator;

impl VertexCoordinator {
    pub fn spawn(
        node_id: Id,
        committee: Committee,
        vertex_to_consensus_sender: Sender<Vertex>,
        vertex_to_broadcast_receiver: Receiver<Vertex>
    ) {
        // Spawn the network receiver listening to vertices broadcasted from the other nodes.
        let address = committee.get_node_address(node_id).unwrap();
        NetworkReceiver::spawn(
            address,
            VertexReceiverHandler { vertex_to_consensus_sender },
        );
        info!("Vertex Coordinator listening to the messages on {}", address);

        VertexBroadcaster::spawn(
            vertex_to_broadcast_receiver,
            ReliableSender::new(),
            committee
        );
    }
}