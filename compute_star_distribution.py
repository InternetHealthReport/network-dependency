import argparse
import logging
import sys
from collections import defaultdict

from network_dependency.kafka.kafka_reader import KafkaReader
from network_dependency.kafka.kafka_writer import KafkaWriter
from network_dependency.utils.helper_functions import \
    parse_timestamp_argument, check_key, check_keys


def parse_hop(hop: str) -> list:
    if not hop:
        return list()
    if hop.startswith('{'):
        return list(hop.strip('{}').split(','))
    return [hop]


def get_star_distribution(ip_path: str) -> tuple:
    """Parse the full-ip-path attribute and return a tuple
    (path_length, stars_in_path)"""
    path_len = 0
    star_count = 0
    for hop in ip_path.split(' '):
        for ip in parse_hop(hop):
            path_len += 1
            if ip == '*':
                star_count += 1
    return path_len, star_count


def process_msg(msg: dict, data: dict) -> None:
    if check_key('elements', msg):
        return
    for element in msg['elements']:
        if check_key('fields', element) \
                or check_keys(['as-path', 'full-ip-path'], element['fields']):
            continue
        star_distribution = \
            get_star_distribution(element['fields']['full-ip-path'])
        as_path = element['fields']['as-path'].split(' ')
        if not as_path:
            logging.error(f'Empty AS path: {msg}')
            continue
        scope = as_path[-1]
        if scope.startswith('{'):
            logging.warning(f'Skipping AS set scope: {msg}')
            continue
        for hop in as_path:
            for asn in parse_hop(hop):
                data[scope][asn].append(star_distribution)


def get_distributions(reader: KafkaReader) -> dict:
    scope_data = defaultdict(lambda: defaultdict(list))
    for msg in reader.read():
        process_msg(msg, scope_data)
    return scope_data


def write_data(writer: KafkaWriter, data: dict, timestamp: int) -> None:
    logging.info(f'Flushing {len(data)} scopes')
    for scope in data:
        for asn in data[scope]:
            msg = {'timestamp': timestamp,
                   'asn': asn,
                   'scope': scope,
                   'nb_paths': len(data[scope][asn]),
                   'stars': data[scope][asn]}
            writer.write(scope, msg, timestamp * 1000)


def main() -> None:
    log_fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        level=logging.INFO,
        filename='compute_star_distribution.log',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    parser = argparse.ArgumentParser()
    parser.add_argument('timestamp')
    parser.add_argument('collector')
    parser.add_argument('-s', '--server', default='localhost:9092')
    args = parser.parse_args()

    logging.info(f'Started: {sys.argv}')

    start_ts = parse_timestamp_argument(args.timestamp) * 1000
    if start_ts == 0:
        logging.error(f'Invalid timestamp specified: {args.timestamp}')
        sys.exit(1)
    end_ts = start_ts + 1

    rib_topic = 'ihr_bgp_' + args.collector + '_ribs'
    output_topic = 'ihr_hegemony_' + args.collector + '_star_distribution'
    logging.info(f'Reading from topic: {rib_topic}')
    logging.info(f'Writing to topic: {output_topic}')

    reader = KafkaReader([rib_topic], args.server, start_ts, end_ts)
    writer = KafkaWriter(output_topic,
                         args.server,
                         num_partitions=10,
                         # 2 months
                         config={'retention.ms': 5184000000})

    with reader:
        scope_data = get_distributions(reader)
    if not scope_data:
        logging.warning('No scope data.')
        sys.exit(1)

    with writer:
        write_data(writer, scope_data, start_ts // 1000)


if __name__ == '__main__':
    main()
