use std::collections::HashMap;
use std::process::id;
use anyhow::{Context, Result};
use clap::{App, AppSettings, ArgMatches, SubCommand};
use env_logger::Env;
use log::info;
use tokio::sync::mpsc::{channel, Receiver};
use consensus::Consensus;
use model::config::Parameters;
use model::staker::{Id, InitialStakers, NodePublicKey};
use model::vertex::Vertex;
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
                .subcommand(
                    SubCommand::with_name("vertex_coordinator")
                        .about("Run vertex coordinator")
                )
                .subcommand(
                    SubCommand::with_name("worker")
                        .about("Run a single worker")
                        .args_from_usage("--id=<INT> 'The worker id'"),
                )
                .setting(AppSettings::SubcommandRequiredElseHelp),
        )
        .setting(AppSettings::SubcommandRequiredElseHelp)
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
    let stakers = InitialStakers::new();
    let stakers_keys = stakers.stakers.iter().map(|v| v.1.public_key.clone()).collect::<Vec<NodePublicKey>>();
    let id = matches.value_of("id").unwrap().parse::<Id>().unwrap();
    // let committee = Committee::new();
    let parameters = Parameters::new();
    // let storage = HashMap::new();

    let (vertex_output_sender, vertex_output_receiver) = channel::<Vertex>(DEFAULT_CHANNEL_CAPACITY);

    match matches.subcommand() {
        ("vertex_coordinator", _) => {
            let (vertex_sender, vertex_receiver) = channel::<Vertex>(DEFAULT_CHANNEL_CAPACITY);

            VertexCoordinator::spawn(
                stakers.get(id).unwrap().clone(),
                parameters.clone(),
                vertex_sender,
            );
            Consensus::spawn(
                stakers_keys,
                vertex_receiver,
                vertex_output_sender
            );
        }
        _ => unreachable!(),
    }
    wait_and_print_vertexs(vertex_output_receiver).await;
    unreachable!();
}

async fn wait_and_print_vertexs(mut rx_output: Receiver<Vertex>) {
    while let Some(vertex) = rx_output.recv().await {
        info!("{}", vertex)
    }
}
