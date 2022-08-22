use log::{debug, info};
use tokio::sync::mpsc::{Receiver, Sender};

use model::committee::{Committee, Id};
use model::vertex::{Vertex};
use network::{Receiver as NetworkReceiver, ReliableSender};

use crate::vertex_broadcaster::VertexBroadcaster;
use crate::vertex_message_handler::{VertexMessage, VertexReceiverHandler};

pub struct VertexService;

impl VertexService {
    pub fn spawn(
        node_id: Id,
        committee: Committee,
        vertex_message_sender: Sender<VertexMessage>,
        vertex_to_broadcast_receiver: Receiver<Vertex>
    ) {
        // Spawn the network receiver listening to vertices broadcasted from the other nodes.
        debug!("Start listening for vertices from other nodes");
        let address = committee.get_node_address(node_id).unwrap();
        NetworkReceiver::spawn(
            address,
            VertexReceiverHandler { vertex_message_sender },
        );
        info!("Vertex Coordinator listening to the messages on {}", address);

        VertexBroadcaster::spawn(
            vertex_to_broadcast_receiver,
            ReliableSender::new(),
            committee
        );
    }
}