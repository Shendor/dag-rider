use std::collections::HashMap;
use anyhow::{Context, Result};
use clap::{App, ArgMatches, SubCommand};
use env_logger::Env;
use log::info;
use tokio::sync::mpsc::{channel, Receiver};
use consensus::consensus::Consensus;
use consensus::garbage_collector::GarbageCollector;

use model::block::{Block, BlockHash};
use model::committee::{Committee, Id};
use model::{Round, Timestamp};
use model::vertex::Vertex;
use storage::Storage;
use transaction::TransactionService;
use vertex::vertex_service::VertexService;
use vertex::vertex_message_handler::VertexMessage;

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

    let (consensus_sender, consensus_receiver) = channel::<Vertex>(DEFAULT_CHANNEL_CAPACITY);
    let (gc_round_sender, gc_round_receiver) = tokio::sync::broadcast::channel::<Round>(DEFAULT_CHANNEL_CAPACITY);
    let (block_sender, block_receiver) = channel::<BlockHash>(DEFAULT_CHANNEL_CAPACITY);
    let (ordered_vertex_timestamps_sender, ordered_vertex_timestamps_receiver) =
        channel::<(Vertex, Vec<(Round, Timestamp)>)>(DEFAULT_CHANNEL_CAPACITY);

    let committee = Committee::default();
    let genesis = Vertex::genesis(committee.get_nodes_keys()).iter()
        .map(|v| (v.hash().to_vec(), bincode::serialize(v).expect("Failed to serialize vertex")))
        .collect::<HashMap<Vec<u8>, Vec<u8>>>();
    // let storage = Storage::new(matches.value_of("store").unwrap()).context("Failed to create the storage")?;
    let storage = Storage::new(genesis).context("Failed to create the storage")?;
    let node_key = committee.get_node_key(node_id).expect(format!("Node public key not found for the id {}", node_id).as_str());

    TransactionService::spawn(
        node_id,
        committee.clone(),
        storage.clone(),
        block_sender,
    );

    VertexService::spawn(
        node_key,
        committee.clone(),
        storage,
        consensus_sender,
        gc_round_sender.subscribe(),
        block_receiver
    );

    Consensus::spawn(
        committee,
        consensus_receiver,
        ordered_vertex_timestamps_sender
    );

    GarbageCollector::spawn(
        ordered_vertex_timestamps_receiver,
        gc_round_sender
    );

    wait_and_print_gc_rounds(gc_round_receiver).await;
    // Ok(())
    unreachable!();
}

async fn wait_and_print_gc_rounds(mut gc_round_receiver: tokio::sync::broadcast::Receiver<Round>) {
    loop {
        if let Ok(gc_round) = gc_round_receiver.recv().await {
            info!("GC round: {}", gc_round);
        }
    }
}
