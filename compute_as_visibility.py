import argparse
import configparser
import logging
import sys
from collections import defaultdict
from datetime import datetime, timezone

from confluent_kafka import OFFSET_BEGINNING, OFFSET_END

from network_dependency.kafka.kafka_reader import KafkaReader
from network_dependency.kafka.kafka_writer import KafkaWriter
from network_dependency.utils.helper_functions import parse_timestamp_argument

DATE_FMT = '%Y-%m-%dT%H:%M:%S'


def verify_option(config: configparser.ConfigParser,
                  section: str,
                  option: str) -> bool:
    try:
        setting = config.get(section, option)
        if not setting:
            logging.error(f'Error in configuration file: Section [{section}] '
                          f'option "{option}" is present but empty.')
            return True
    except configparser.NoSectionError as e:
        logging.error(f'Error in configuration file: {e}')
        return True
    except configparser.NoOptionError as e:
        logging.error(f'Error in configuration file: {e}')
        return True
    return False


def verify_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(config_path)
    check_options = [('input', 'kafka_topic'),
                     ('output', 'kafka_topic'),
                     ('kafka', 'bootstrap_servers')]
    for option in check_options:
        if verify_option(config, *option):
            return configparser.ConfigParser()
    return config


def check_key(key, data: dict) -> bool:
    if key not in data or not data[key]:
        logging.error(f'Key {key} missing in message.')
        return True
    return False


def parse_asn(asn: str) -> list:
    if not asn:
        return list()
    if asn.startswith('('):
        return list(asn.strip('()').split(','))
    return [asn]


def get_as_triple(as_path: list, idx: int) -> (str, str, str):
    """Caller needs to make sure that idx is in range of as_path."""
    lneighbor = str()
    if idx - 1 >= 0:
        lneighbor = as_path[idx - 1]
    asn = as_path[idx]
    rneighbor = str()
    if idx + 1 < len(as_path):
        rneighbor = as_path[idx + 1]
    return lneighbor, asn, rneighbor


def process_msg(msg: dict, data: dict) -> int:
    if check_key('elements', msg):
        return 0
    elements = msg['elements']
    as_paths_in_msg = 0
    for element in elements:
        if check_key('fields', element) \
                or check_key('as-path', element['fields']):
            continue
        as_path = element['fields']['as-path'].split(' ')
        for hop in range(len(as_path)):
            lneighbor, asn, rneighbor = map(parse_asn,
                                            get_as_triple(as_path, hop))
            # The for loops below are necessary since every hop might
            # be an AS set...
            # Increment counter for AS
            for entry in asn:
                data[entry]['count'] += 1
            # Handle left neighbors
            for neighbor in lneighbor:
                for entry in asn:
                    data[entry]['lneighbors'][neighbor] += 1
            # Handle right neighbors
            for neighbor in rneighbor:
                for entry in asn:
                    data[entry]['rneighbors'][neighbor] += 1
        as_paths_in_msg += 1
    return as_paths_in_msg


def flush_data(data: dict,
               as_paths_in_bin: int,
               timestamp: int,
               writer: KafkaWriter):
    ts_str = datetime.fromtimestamp(timestamp, tz=timezone.utc) \
        .strftime(DATE_FMT)
    logging.info(f'Flushing bin {ts_str}: {len(data)} ASes.')
    msg = {'timestamp': timestamp,
           'total_as_paths': as_paths_in_bin}
    for asn in data:
        msg['asn'] = asn
        msg.update(data[asn])
        writer.write(asn, msg, timestamp * 1000)


def make_data_dict() -> dict:
    return {'count': 0,
            'lneighbors': defaultdict(int),
            'rneighbors': defaultdict(int)}


def process_interval(reader: KafkaReader, writer: KafkaWriter) -> None:
    bins = defaultdict(lambda: defaultdict(make_data_dict))
    as_paths_in_bins = defaultdict(int)
    for msg in reader.read():
        if check_key('rec', msg) or check_key('time', msg['rec']):
            continue
        msg_ts = msg['rec']['time']
        as_paths_in_bins[msg_ts] += process_msg(msg, bins[msg_ts])
    for ts in sorted(bins.keys()):
        flush_data(bins[ts], as_paths_in_bins[ts], ts, writer)


def main() -> None:
    log_fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        level=logging.INFO,
        filename='compute_as_visibility.log',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('-s', '--start', help='Start timestamp (as UNIX epoch '
                                              'in seconds or milliseconds, or'
                                              'in YYYY-MM-DDThh:mm format)')
    parser.add_argument('-e', '--stop', help='Stop timestamp (as UNIX epoch '
                                             'in seconds or milliseconds, or'
                                             'in YYYY-MM-DDThh:mm format)')
    args = parser.parse_args()

    logging.info(f'Started: {sys.argv}')

    config = verify_config(args.config)
    if not config.sections():
        sys.exit(1)

    start = OFFSET_BEGINNING
    if args.start:
        start = parse_timestamp_argument(args.start) * 1000
        if start == 0:
            logging.error(f'Invalid start timestamp: {args.start}')
            sys.exit(1)
    end = OFFSET_END
    if args.stop:
        end = parse_timestamp_argument(args.stop) * 1000
        if end == 0:
            logging.error(f'Invalid stop timestamp: {args.stop}')
            sys.exit(1)

    reader = KafkaReader([config.get('input', 'kafka_topic')],
                         config.get('kafka', 'bootstrap_servers'),
                         start,
                         end)
    writer = KafkaWriter(config.get('output', 'kafka_topic'),
                         config.get('kafka', 'bootstrap_servers'))

    with reader, writer:
        process_interval(reader, writer)


if __name__ == '__main__':
    main()
    sys.exit(0)
