import argparse
import bz2
import logging
import pickle
import sys
from collections import defaultdict, namedtuple
from datetime import datetime, timezone

from network_dependency.kafka.kafka_reader import KafkaReader
from network_dependency.utils.helper_functions import parse_timestamp_argument

BOOTSTRAP_SERVERS = 'localhost:9092'
DATE_FMT = '%Y-%m-%dT%H:%M'
Record = namedtuple('Record', 'peer dst as_path')


def unprepend_path(path: str) -> tuple:
    ret = list()
    prev = None
    # Treat AS sets as a single hop for now.
    for as_ in path.split(' '):
        if prev and prev == as_:
            continue
        ret.append(as_)
        prev = as_
    return tuple(ret)


def process_msg(msg: dict) -> Record:
    for element in msg['elements']:
        # Only parse records or announcements.
        if element['type'] != 'R' and element['type'] != 'A':
            continue
        if element['peer_asn'] == 0:
            continue
        path = unprepend_path(element['fields']['as-path'])
        if path:
            yield Record(element['peer_asn'], path[-1], path)


def read_topics(topics: list, start_ts: int, end_ts: int) -> Record:
    reader = KafkaReader(topics, BOOTSTRAP_SERVERS, start_ts, end_ts)
    with reader:
        for msg in reader.read():
            for rec in process_msg(msg):
                yield rec


def main() -> None:
    desc = """Get the upstream path diversions seen at the specified collector. 
           If the first d hops after the peer AS (specified by the --depth 
           parameter) differ for a (peer AS, destination AS) pair, the paths are
           recorded."""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('collector', help='Name of collector. Value is '
                                          'inserted into template '
                                          'ihr_bgp_<collector>_ribs etc.')
    parser.add_argument('start', help='Start timestamp')
    parser.add_argument('end', help='End timestamp')
    parser.add_argument('-d', '--depth', help='Comparison depth (default: 1)',
                        type=int, default=1)

    log_fmt = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(format=log_fmt, level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')

    logging.info(f'Started: {sys.argv}')

    args = parser.parse_args()
    start_ts = parse_timestamp_argument(args.start)
    if start_ts == 0:
        logging.error(f'Invalid start timestamp: {args.start}')
        sys.exit(1)
    start_dt = datetime.fromtimestamp(start_ts, tz=timezone.utc)
    end_ts = parse_timestamp_argument(args.end)
    if end_ts == 0:
        logging.error(f'Invalid end timestamp: {args.end}')
        sys.exit(1)
    end_dt = datetime.fromtimestamp(end_ts, tz=timezone.utc)

    logging.info(f' Start timestamp: {start_dt.strftime(DATE_FMT)}')
    logging.info(f'   End timestamp: {end_dt.strftime(DATE_FMT)}')
    logging.info(f'Comparison depth: {args.depth}')

    upstream_paths = dict()
    path_counts = dict()
    diverging_paths = set()
    for rec in read_topics([f'ihr_bgp_{args.collector}_ribs',
                            f'ihr_bgp_{args.collector}_updates'],
                           start_ts * 1000,
                           end_ts * 1000):
        peer_dst = (rec.peer, rec.dst)
        # depth + 1 since the first AS is always the peer
        path_prefix = rec.as_path[:args.depth + 1]
        if peer_dst not in path_counts:
            path_counts[peer_dst] = defaultdict(int)
        path_counts[peer_dst][path_prefix] += 1
        if peer_dst not in upstream_paths:
            upstream_paths[peer_dst] = path_prefix
        elif upstream_paths[peer_dst] != path_prefix:
            logging.debug(f'Path divergence for {peer_dst}: '
                          f'{upstream_paths[peer_dst]} != {path_prefix}')
            if peer_dst not in diverging_paths:
                diverging_paths.add(peer_dst)
    logging.info(f'     Total paths: {len(path_counts)}')
    logging.info(f'Consistent paths: '
                 f'{len(path_counts.keys() - diverging_paths)}')
    logging.info(f' Diverging paths: {len(diverging_paths)}')

    output_data = {'total': len(path_counts),
                   'diverging': dict()}
    for peer_dst in diverging_paths:
        output_data['diverging'][peer_dst] = dict(path_counts[peer_dst])
    output = f'diversion.{args.collector}.{start_dt.strftime(DATE_FMT)}--' \
             f'{end_dt.strftime(DATE_FMT)}.pickle.bz2'
    logging.info(f'Writing data to file {output}')
    with bz2.open(output, 'wb') as f:
        pickle.dump(output_data, f)


if __name__ == '__main__':
    main()
    sys.exit(0)
