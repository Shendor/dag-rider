use std::net::SocketAddr;
use ed25519_dalek::Keypair;
use log::info;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use network::{MessageHandler, Receiver as NetworkReceiver, Writer};
use model::vertex::{Vertex, VertexHash, Header};
use model::config::Parameters;
use model::DEFAULT_CHANNEL_CAPACITY;
use model::staker::{InitialStaker, NodePublicKey};
use crate::core::Core;
use crate::vertex_message_handler::VertexReceiverHandler;

pub struct VertexCoordinator;

impl VertexCoordinator {
    pub fn spawn(
        staker: InitialStaker,
        parameters: Parameters,
        vertex_sender: Sender<Vertex>,
    ) {
        let (vertex_message_sender, vertex_message_receiver) = channel(DEFAULT_CHANNEL_CAPACITY);
        let (vertex_request_sender, vertex_request_receiver) = channel(DEFAULT_CHANNEL_CAPACITY);

        // Spawn the network receiver listening to messages from the other primaries.
        let address = SocketAddr::new("0.0.0.0".parse().unwrap(), 1234);
        NetworkReceiver::spawn(
            address,
            VertexReceiverHandler {
                vertex_message_sender,
                vertex_request_sender
            },
        );
        info!("Vertex Coordinator {} listening to the messages on {}", staker.node_id, address);

        Core::spawn(
            staker.public_key,
            vertex_message_receiver,
            vertex_sender,
        );
    }
}