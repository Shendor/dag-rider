# Local Benchmarks
When running benchmarks, the codebase is automatically compiled with the feature flag `benchmark`. This enables the node to print some special log entries that are then read by the python scripts and used to compute performance. These special log entries are clearly indicated with comments in the code: make sure to not alter them (otherwise the benchmark scripts will fail to interpret the logs).

### Parametrize the benchmark
After cloning the repo, you can use [Fabric](http://www.fabfile.org/) to run benchmarks on your local machine.
```python
@task
def local(ctx):
    ...
```
The benchmark parameters look as follows:
```python
bench_params = {
    'rate': 50_000,
    'faults': 0,
    'duration': 20,
}
```
They specify the input rate (tx/s) at which the clients submits transactions to the system (`rate`), the size of each transaction in bytes (`tx_size`), the number of faulty nodes ('faults), and the duration of the benchmark in seconds (`duration`). The minimum transaction size is 9 bytes, this ensure that the transactions of a client are all different. The benchmarking script will deploy as many clients as workers and divide the input rate equally amongst each client. For instance, if you configure the testbed with 4 nodes, 1 worker per node, and an input rate of 1,000 tx/s (as in the example above), the scripts will deploy 4 clients each submitting transactions to one node at a rate of 250 tx/s. When the parameters `faults` is set to `f > 0`, the last `f` nodes and clients are not booted; the system will thus run with `n-f` nodes (and `n-f` clients). 

### Run the benchmark
Once you specified both `bench_params` as desired, run:
```
$ fab local
```
This command first recompiles your code in `release` mode (and with the `benchmark` feature flag activated), thus ensuring you always benchmark the latest version of your code. This may take a long time the first time you run it. It then generates the configuration files and keys for each node, and runs the benchmarks with the specified parameters. It finally parses the logs and displays a summary of the execution similarly to the one below. All the configuration and key files are hidden JSON files; i.e., their name starts with a dot (`.`), such as `.committee.json`.
```
-----------------------------------------
 SUMMARY:
-----------------------------------------
 + CONFIG:
 Faults: 0 node(s)
 Committee size: 4 node(s)
 Worker(s) per node: 1 worker(s)
 Collocate primary and workers: True
 Input rate: 50,000 tx/s
 Transaction size: 512 B
 Execution time: 19 s

 + RESULTS:
 Consensus TPS: 46,478 tx/s
 Consensus BPS: 23,796,531 B/s
 Consensus latency: 464 ms

 End-to-end TPS: 46,149 tx/s
 End-to-end BPS: 23,628,541 B/s
 End-to-end latency: 557 ms
-----------------------------------------
```
The 'Consensus TPS' and 'Consensus latency' respectively report the average throughput and latency without considering the client. The consensus latency thus refers to the time elapsed between the block's creation and its commit. In contrast, 'End-to-end TPS' and 'End-to-end latency' report the performance of the whole system, starting from when the client submits the transaction. The end-to-end latency is often called 'client-perceived latency'. To accurately measure this value without degrading performance, the client periodically submits 'sample' transactions that are tracked across all the modules until they get committed into a block; the benchmark scripts use sample transactions to estimate the end-to-end latency.