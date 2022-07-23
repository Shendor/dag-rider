use std::collections::{HashMap, HashSet};
use log::{debug, error, warn};
use tokio::sync::mpsc::{Receiver, Sender};
use model::vertex::{Vertex, VertexHash, Header};
use model::Round;
use model::staker::NodePublicKey;
use crate::vertex_message_handler::{VertexMessage};

pub struct Core {
    node: NodePublicKey,

    /// Receiver for dag messages (headers, votes, certificates).
    vertex_message_receiver: Receiver<VertexMessage>,
    /// Output all vertices to the consensus layer.
    vertex_sender: Sender<Vertex>,

    /// The owner of the last voted headers.
    last_voted: HashMap<Round, HashSet<NodePublicKey>>,
    /// The set of vertices we are currently processing.
    processing: HashMap<Round, HashSet<VertexHash>>,
    /// The latest proposed vector for which we are waiting votes.
    current_vertex: VertexHash,
}

impl Core {
    pub fn spawn(
        node: NodePublicKey,
        vertex_message_receiver: Receiver<VertexMessage>,
        vertex_sender: Sender<Vertex>,
    ) {
        tokio::spawn(async move {
            Self {
                node,
                vertex_message_receiver,
                vertex_sender,
                last_voted: HashMap::new(),
                processing: HashMap::new(),
                current_vertex: VertexHash::default(),
            }
                .run()
                .await;
        });
    }

    pub async fn run(&mut self) {
        loop {
            match self.vertex_message_receiver.recv().await.unwrap() {
                VertexMessage::Header(header) => {
                    debug!("Header received {}", header);
                }
                VertexMessage::Vote(vote) => {
                    debug!("Vote received {}", vote);
                }
                VertexMessage::Vertex(vertex) => {
                    debug!("Vertex received {}", vertex);
                    self.process_vertex(vertex).await;
                }
                _ => panic!("Unexpected core message")
            }
        }
    }

    async fn process_vertex(&mut self, vertex: Vertex) -> model::Result<()> {
        debug!("Processing {:?}", vertex);
        self.vertex_sender.send(vertex).await;
        Ok(())
    }
}
