import argparse
import logging
import os
import sys
from network_dependency.kafka.kafka_reader import KafkaReader
from network_dependency.utils.scope_statistics import ScopeStatistic

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('timestamp', type=int)
    parser.add_argument('output_dir')
    # Logging
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S'
        )
    logging.info("Started: %s" % sys.argv)
    args = parser.parse_args()
    output_dir = args.output_dir
    if not output_dir.endswith('/'):
        output_dir += '/'
    scopes = dict()
    reader = KafkaReader(['ihr_bgp_traceroutev4_ribs'],
                         'kafka2:9092',
                         args.timestamp * 1000,
                         (args.timestamp + 1) * 1000)
    with reader:
        for msg in reader.read():
            for element in msg['elements']:
                path = element['fields']['as-path']
                scope = int(path.split()[-1])
                if scope not in scopes:
                    scopes[scope] = ScopeStatistic(scope)
                scopes[scope].add_path(path)
    os.makedirs(output_dir, exist_ok=True)
    overview_lines = ['as traceroutes unique_peer_ass\n']
    for v in scopes.values():
        overview_lines.append(' '.join(map(str,
                                           [v.as_,
                                            v.total_peer_count,
                                            len(v.peer_count)])) + '\n')
        with open(output_dir + str(v.as_) + '-peers.dat', 'w') as f:
            f.write('peer_as occurrences relative_occurrence(percent)\n')
            f.writelines(v.peer_distribution())
        with open(output_dir + str(v.as_) + '-paths.dat', 'w') as f:
            f.write('peer_as occurrences relative_occurrence(percent)\n')
            f.writelines(v.path_distribution())
    with open(output_dir + 'overview.dat', 'w') as f:
        f.writelines(overview_lines)