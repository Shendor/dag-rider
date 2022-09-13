from json import dump, load


class ConfigError(Exception):
    pass


class Key:
    def __init__(self, name, secret):
        self.name = name
        self.secret = secret

    @classmethod
    def from_file(cls, filename):
        assert isinstance(filename, str)
        with open(filename, 'r') as f:
            data = load(f)
        return cls(data['name'], data['secret'])


class Committee:
    """ The committee looks as follows:
       "1": {
       "address": "0.0.0.0:3011",
       "tx_address": "0.0.0.0:3012",
       "block_address": "0.0.0.0:3013",
       "keypair": "d7fc6106701213c61d34ef8e9416"
   }
   """

    def __init__(self, filename):
        with open(filename, 'r') as f:
            self.json = load(f)

    def node_addresses(self, faults=0):
        ''' Returns an ordered list of primaries' addresses. '''
        assert faults < self.size()
        addresses = []
        good_nodes = self.size() - faults
        for validator in list(self.json['validators'].values())[:good_nodes]:
            addresses += [validator['vertex_address']]
        return addresses

    def tx_addresses(self, faults=0):
        ''' Returns an ordered list of list of workers' addresses. '''
        assert faults < self.size()
        addresses = []
        good_nodes = self.size() - faults
        counter = 0
        for id, validator in self.json['validators'].items():
            if counter < good_nodes:
                addresses += [(id, validator['tx_address'])]
            counter += 1
        print(f'Run clients for these addresses: {addresses}')
        return addresses

    def ips(self, name=None):
        ''' Returns all the ips associated with an authority (in any order). '''
        if name is None:
            names = list(self.json['validators'].keys())
        else:
            names = [name]

        ips = set()
        for name in names:
            validators = self.json['validators'][name]
            ips.add(self.ip(validators['vertex_address']))
            ips.add(self.ip(validators['tx_address']))
            ips.add(self.ip(validators['block_address']))
            ips.add(self.ip(validators['block_proposal_address']))

        return list(ips)

    def remove_nodes(self, nodes):
        ''' remove the `nodes` last nodes from the committee. '''
        assert nodes < self.size()
        for _ in range(nodes):
            self.json['validators'].popitem()

    def size(self):
        ''' Returns the number of authorities. '''
        return len(self.json['validators'])

    def workers(self):
        ''' Returns the total number of workers (all authorities altogether). '''
        # return sum(len(x['workers']) for x in self.json['validators'].values())
        # NOTE: There is only 1 transaction receiver per node
        return 1

    def print(self, filename):
        assert isinstance(filename, str)
        with open(filename, 'w') as f:
            dump(self.json, f, indent=4, sort_keys=True)

    @staticmethod
    def ip(address):
        assert isinstance(address, str)
        return address.split(':')[0]


class BenchParameters:
    def __init__(self, json):
        try:
            self.faults = int(json['faults'])

            rate = json['rate']
            rate = rate if isinstance(rate, list) else [rate]
            if not rate:
                raise ConfigError('Missing input rate')
            self.rate = [int(x) for x in rate]

            
            self.workers = int(json['workers'])

            if 'collocate' in json:
                self.collocate = bool(json['collocate'])
            else:
                self.collocate = True

            self.tx_size = int(json['tx_size'])
           
            self.duration = int(json['duration'])

            self.runs = int(json['runs']) if 'runs' in json else 1
        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')


class PlotParameters:
    def __init__(self, json):
        try:
            faults = json['faults']
            faults = faults if isinstance(faults, list) else [faults]
            self.faults = [int(x) for x in faults] if faults else [0]

            nodes = json['nodes']
            nodes = nodes if isinstance(nodes, list) else [nodes]
            if not nodes:
                raise ConfigError('Missing number of nodes')
            self.nodes = [int(x) for x in nodes]

            workers = json['workers']
            workers = workers if isinstance(workers, list) else [workers]
            if not workers:
                raise ConfigError('Missing number of workers')
            self.workers = [int(x) for x in workers]

            if 'collocate' in json:
                self.collocate = bool(json['collocate'])
            else:
                self.collocate = True

            self.tx_size = int(json['tx_size'])

            max_lat = json['max_latency']
            max_lat = max_lat if isinstance(max_lat, list) else [max_lat]
            if not max_lat:
                raise ConfigError('Missing max latency')
            self.max_latency = [int(x) for x in max_lat]

        except KeyError as e:
            raise ConfigError(f'Malformed bench parameters: missing key {e}')

        except ValueError:
            raise ConfigError('Invalid parameters type')

        if len(self.nodes) > 1 and len(self.workers) > 1:
            raise ConfigError(
                'Either the "nodes" or the "workers can be a list (not both)'
            )

    def scalability(self):
        return len(self.workers) > 1
