import argparse
import logging
import sys

from network_dependency.kafka.kafka_reader import KafkaReader
from network_dependency.utils.helper_functions import check_keys, \
                                                      parse_timestamp_argument


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('topic')
    parser.add_argument('output_file')
    parser.add_argument('timestamp',
                        help='Read timestamp (as UNIX epoch in seconds or '
                             'milliseconds, or in YYYY-MM-DDThh:mm format)')
    parser.add_argument('-n', '--num-asn', type=int,
                        help='Get first N ASes')
    parser.add_argument('-s', '--server', default='localhost:9092')
    args = parser.parse_args()

    log_fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        level=logging.INFO,
        filename='get_as_rank.log',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(f'Started: {sys.argv}')

    timestamp_arg = args.timestamp
    start_ts = parse_timestamp_argument(timestamp_arg)
    if start_ts == 0:
        logging.error(f'Invalid timestamp specified: {timestamp_arg}')
        sys.exit(1)
    end_ts = start_ts + 1

    num_asn = args.num_asn

    reader = KafkaReader([args.topic],
                         args.server,
                         start_ts * 1000,
                         end_ts * 1000)
    required_keys = ['timestamp', 'rank', 'asn']
    asns = list()
    with reader:
        for msg in reader.read():
            if check_keys(required_keys, msg):
                logging.warning(f'Missing one or more required keys '
                                f'{required_keys} in message: {msg}')
                continue
            if msg['timestamp'] != start_ts:
                logging.warning(f'Read message with unexpected timestamp. '
                                f'Expected: {start_ts} Got: {msg["timestamp"]}')
                continue
            asns.append((msg['rank'], msg['asn']))
    asns.sort()

    if num_asn:
        logging.info(f'Limiting to top {num_asn} ASes.')
        asns = asns[:num_asn]
    prev_rank = 0
    for rank, asn in asns:
        if rank != prev_rank + 1:
            logging.error(f'Gap in AS ranks. Expected: {prev_rank + 1} '
                          f'Got: {rank} (AS{asn})')
        prev_rank = rank

    output_file = args.output_file
    logging.info(f'Writing {len(asns)} ASes to file: {output_file}')
    with open(output_file, 'w') as f:
        for rank, asn in asns:
            f.write(f'{asn}\n')


if __name__ == '__main__':
    main()
    sys.exit(0)
