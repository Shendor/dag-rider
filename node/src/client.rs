use std::net::SocketAddr;

use anyhow::{Context, Result};
use bytes::BufMut as _;
use bytes::BytesMut;
use clap::{App, AppSettings, crate_name, crate_version};
use env_logger::Env;
use futures::sink::SinkExt as _;
use log::{info};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[tokio::main]
async fn main() -> Result<()> {
    // --size 32 --rate 1 --nodes 127.0.0.1:1234
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .args_from_usage("<ADDR> 'The network address of the node where to send txs'")
        // .args_from_usage("--size=<INT> 'The size of each transaction in bytes'")
        // .args_from_usage("--rate=<INT> 'The rate (txs/s) at which to send the transactions'")
        // .args_from_usage("--nodes=[ADDR]... 'Network addresses that must be reachable before starting the benchmark.'")
        .setting(AppSettings::ArgRequiredElseHelp)
        .get_matches();

    env_logger::Builder::from_env(Env::default().default_filter_or("info"))
        .format_timestamp_millis()
        .init();

    let target = matches
        .value_of("ADDR")
        .unwrap()
        .parse::<SocketAddr>()
        .context("Invalid socket address format")?;

    info!("Node address: {}", target);

    let client = Client {
        target,
    };

    // Start the benchmark.
    client.send().await.context("Failed to submit transactions")
}

struct Client {
    target: SocketAddr,
}

impl Client {
    pub async fn send(&self) -> Result<()> {
        const TRANSACTION_COUNT: u64 = 10;
        const TX_SIZE: usize = 64;

        let stream = TcpStream::connect(self.target)
            .await
            .context(format!("failed to connect to {}", self.target))?;

        let mut tx = BytesMut::with_capacity(TX_SIZE);
        let mut transport = Framed::new(stream, LengthDelimitedCodec::new());

        info!("Start sending transactions");

        for c in 0..TRANSACTION_COUNT {
            info!("Sending sample transaction {}", c);

            tx.put_u8(0u8); // Sample txs start with 0.
            tx.put_u64(c); // This counter identifies the tx.
            // tx.resize(TX_SIZE, 0u8);
            let bytes = tx.split().freeze();

            transport.send(bytes).await?;
        }

        Ok(())
    }
}
