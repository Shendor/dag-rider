# dag-rider
Implementation of DAG-Rider protocol for demonstration purpose.

Original paper: https://arxiv.org/pdf/2102.08325v2.pdf

Some code was taken from https://github.com/asonnino/narwhal with modifications.

## How to run
There are 4 hardcoded nodes, each having id from 1 to 4. To run a node with id 1: `cargo run --package node --bin node -- run --id 1`

To run a client for sending transactions: `cargo run --package node --bin client -- 127.0.0.1:1244`, where last parameter is an IP address of a node
service which accepts transactions. The available endpoints are:
* Node 1: 127.0.0.1:1244
* Node 2: 127.0.0.1:1245
* Node 3: 127.0.0.1:1246
* Node 4: 127.0.0.1:1247