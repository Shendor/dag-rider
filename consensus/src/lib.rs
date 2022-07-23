use log::{debug, log_enabled, warn};
use std::collections::{HashMap};
use tokio::sync::mpsc::{Receiver, Sender};
use model::{ Round};
use model::staker::NodePublicKey;
use model::vertex::Vertex;

pub type Dag = HashMap<Round, HashMap<NodePublicKey, Vertex>>;

/// The state that needs to be persisted for crash-recovery.
struct State {
    /// The last committed round.
    last_committed_round: Round,
    // Keeps the last committed round for each authority. This map is used to clean up the dag and
    // ensure we don't commit twice the same certificate.
    last_committed: HashMap<NodePublicKey, Round>,
    /// Keeps the latest committed certificate (and its parents) for every authority. Anything older
    /// must be regularly cleaned up through the function `update`.
    dag: Dag,
}

impl State {
    fn new(genesis_vertices: Vec<Vertex>) -> Self {
        let genesis = genesis_vertices
            .iter()
            .map(|x| (x.owner(), x.clone()))
            .collect::<HashMap<_, _>>();

        Self {
            last_committed_round: 0,
            last_committed: genesis.iter().map(|(h, b)| (*h, b.round())).collect(),
            dag: [(0, genesis)].iter().cloned().collect::<Dag>(),
        }
    }

}

pub struct Consensus {
    vertex_receiver: Receiver<Vertex>,
    vertex_output_sender: Sender<Vertex>,
    genesis: Vec<Vertex>,
}

impl Consensus {
    pub fn spawn(
        nodes: Vec<NodePublicKey>,
        vertex_receiver: Receiver<Vertex>,
        vertex_output_sender: Sender<Vertex>,
    ) {
        tokio::spawn(async move {
            Self {
                vertex_receiver,
                vertex_output_sender,
                genesis: Vertex::genesis(nodes),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        let mut state = State::new(self.genesis.clone());

        while let Some(vertex) = self.vertex_receiver.recv().await {
            debug!("Processing {}", vertex);

            self.vertex_output_sender.send(vertex).await;
        }
    }
}
