use anyhow::{Context, Result};
use clap::{App, ArgMatches, SubCommand};
use env_logger::Env;
use log::info;
use tokio::sync::mpsc::{channel, Receiver};

use consensus::Consensus;
use model::block::Block;
use model::committee::{Committee, Id};
use model::vertex::Vertex;
use storage::Storage;
use transaction::TransactionService;
use vertex::vertex_coordinator::VertexCoordinator;

pub const DEFAULT_CHANNEL_CAPACITY: usize = 1000;

#[tokio::main]
async fn main() -> Result<()> {
    let matches = App::new("DAG-Rider")
        .version("1.0")
        .about("DAG-Rider")
        .subcommand(
            SubCommand::with_name("run")
                .about("Run a node")
                .args_from_usage("--id=<INT> 'Node id'")
        )
        .get_matches();

    let mut logger = env_logger::Builder::from_env(Env::default().default_filter_or("debug"));
    logger.init();

    match matches.subcommand() {
        ("run", Some(sub_matches)) => run(sub_matches).await?,
        _ => unreachable!(),
    }
    Ok(())
}

async fn run(matches: &ArgMatches<'_>) -> Result<()> {
    let node_id = matches.value_of("id").unwrap().parse::<Id>().unwrap();

    let (vertex_output_sender, vertex_output_receiver) = channel::<Vertex>(DEFAULT_CHANNEL_CAPACITY);

    let (vertex_to_broadcast_sender, vertex_to_broadcast_receiver) = channel::<Vertex>(DEFAULT_CHANNEL_CAPACITY);
    let (vertex_to_consensus_sender, vertex_to_consensus_receiver) = channel::<Vertex>(DEFAULT_CHANNEL_CAPACITY);
    let (block_sender, block_receiver) = channel::<Block>(DEFAULT_CHANNEL_CAPACITY);

    let storage = Storage::new(matches.value_of("store").unwrap()).context("Failed to create the storage")?;

    VertexCoordinator::spawn(
        node_id,
        Committee::default(),
        vertex_to_consensus_sender,
        vertex_to_broadcast_receiver
    );

    TransactionService::spawn(
        node_id,
        Committee::default(),
        storage,
        block_sender
    );

    Consensus::spawn(
        node_id,
        Committee::default(),
        vertex_to_consensus_receiver,
        vertex_to_broadcast_sender,
        vertex_output_sender,
        block_receiver
    );

    wait_and_print_vertexs(vertex_output_receiver).await;
    unreachable!();
}

async fn wait_and_print_vertexs(mut vertex_output_receiver: Receiver<Vertex>) {
    while let Some(vertex) = vertex_output_receiver.recv().await {
        info!("Vertex committed: {}", vertex)
    }
}
