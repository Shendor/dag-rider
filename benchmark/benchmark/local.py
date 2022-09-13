import subprocess
from math import ceil
from os.path import basename, splitext
from time import sleep

from benchmark.commands import CommandMaker
from benchmark.config import Key, Committee, BenchParameters, ConfigError
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker


class LocalBench:
    BASE_PORT = 3000

    def __init__(self, bench_parameters_dict):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _background_run(self, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'{command} 2> {log_file}'
        subprocess.run(['tmux', 'new', '-d', '-s', name, cmd], check=True)

    def _kill_nodes(self):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError('Failed to kill testbed', e)

    def run(self, committee_file, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting local benchmark')

        # Kill any previous testbed.
        self._kill_nodes()

        try:
            Print.info('Setting up testbed...')
            rate = self.rate[0]

            # Cleanup all files.
            cmd = f'{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}'
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)  # Removing the store may take time.

            # Recompile the latest code.
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            committee = Committee(committee_file)
            committee.print(PathMaker.committee_file())

            # Run the clients (they will wait for the nodes to be ready).
            tx_addresses = committee.tx_addresses(self.faults)
            rate_share = ceil(rate / len(tx_addresses))
            for (id, address) in tx_addresses:
                cmd = CommandMaker.run_client(
                    address,
                    rate_share,
                    [x for _, x in tx_addresses]
                )
                log_file = PathMaker.client_log_file(int(id), int(id))
                self._background_run(cmd, log_file)

            for id in range(1, committee.size() + 1):
                cmd = CommandMaker.run_block_service(
                    id,
                    committee_file,
                    PathMaker.db_path(id),
                )
                log_file = PathMaker.block_log_file(id)
                self._background_run(cmd, log_file)

            for id in range(1, committee.size() + 1):
                cmd = CommandMaker.run_node(id,
                                            committee_file,
                                            PathMaker.db_path(id),
                                            )
                log_file = PathMaker.node_log_file(id)
                self._background_run(cmd, log_file)

            # Wait for all transactions to be processed.
            Print.info(f'Running benchmark ({self.duration} sec)...')
            sleep(self.duration)
            self._kill_nodes()

            # Parse logs and return the parser.
            Print.info('Parsing logs...')
            return LogParser.process(PathMaker.logs_path(), faults=self.faults)

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            raise BenchError('Failed to run benchmark', e)
