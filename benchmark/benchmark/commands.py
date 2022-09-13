from os.path import join

from benchmark.utils import PathMaker


class CommandMaker:

    @staticmethod
    def cleanup():
        return (
            f'rm -r db-* ; rm .*.json ; mkdir -p {PathMaker.results_path()}'
        )

    @staticmethod
    def clean_logs():
        return f'rm -r {PathMaker.logs_path()} ; mkdir -p {PathMaker.logs_path()}'

    @staticmethod
    def compile():
        return 'cargo build --quiet --release --features benchmark'

    @staticmethod
    def run_node(id, committee, store):
        assert isinstance(committee, str)
        return (f'./node run --id {id} --committee {committee} '
                f'--store {store} consensus')

    @staticmethod
    def run_block_service(id, committee, store):
        assert isinstance(committee, str)
        return (f'./node run --id {id} --committee {committee} '
                f'--store {store} block')

    @staticmethod
    def run_client(address, rate, nodes):
        assert isinstance(address, str)
        assert isinstance(rate, int) and rate >= 0
        assert isinstance(nodes, list)
        assert all(isinstance(x, str) for x in nodes)
        nodes = f'--nodes {" ".join(nodes)}' if nodes else ''
        return f'./benchmark_client {address} --rate {rate} {nodes}'

    @staticmethod
    def kill():
        return 'tmux kill-server'

    @staticmethod
    def alias_binaries(origin):
        assert isinstance(origin, str)
        node, client = join(origin, 'node'), join(origin, 'benchmark_client')
        return f'rm node ; rm benchmark_client ; ln -s {node} . ; ln -s {client} .'
