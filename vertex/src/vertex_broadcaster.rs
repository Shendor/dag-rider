use bytes::Bytes;
use log::{debug, error};
use tokio::sync::mpsc::{Receiver};

use model::committee::Committee;
use model::vertex::{Vertex};
use network::ReliableSender;

pub struct VertexBroadcaster {
    vertex_to_broadcast_receiver: Receiver<Vertex>,
    network: ReliableSender,
    committee: Committee
}

impl VertexBroadcaster {
    pub fn spawn(vertex_to_broadcast_receiver: Receiver<Vertex>, network: ReliableSender, committee: Committee) {
        tokio::spawn(async move {
            Self { vertex_to_broadcast_receiver, network, committee}.run().await;
        });
    }

    pub async fn run(&mut self) {
        loop {
            match self.vertex_to_broadcast_receiver.recv().await.unwrap() {
                vertex => {
                    debug!("Vertex received for broadcast {}", vertex);
                    let addresses = self
                        .committee
                        .get_node_addresses();
                    let bytes = bincode::serialize(&vertex).expect("Failed to serialize vertex in VertexBroadcaster");

                    let handlers = self.network.broadcast(addresses, Bytes::from(bytes)).await;
                    for h in handlers {
                        if let Err(e) = h.await {
                            error!("Broadcast of vertices was not successful")
                        }
                    }
                }
            }
        }
    }

}
