import argparse
import configparser
import logging
import sys
from collections import defaultdict
from datetime import datetime, timezone

from confluent_kafka import OFFSET_BEGINNING, OFFSET_END

from network_dependency.kafka.kafka_reader import KafkaReader
from network_dependency.kafka.kafka_writer import KafkaWriter
from network_dependency.utils.helper_functions import parse_timestamp_argument, \
    check_key, check_keys

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
    check_options = [('input', 'collector'),
                     ('output', 'kafka_topic'),
                     ('kafka', 'bootstrap_servers')]
    for option in check_options:
        if verify_option(config, *option):
            return configparser.ConfigParser()
    return config


def parse_asn(asn: str) -> list:
    if not asn:
        return list()
    if asn.startswith('{'):
        return list(asn.strip('{}').split(','))
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
        if check_keys(['type', 'fields'], element) \
                or check_key('as-path', element['fields']):
            continue
        if element['type'] not in {'R', 'A', 'W'}:
            continue
        as_path = element['fields']['as-path'].split(' ')
        ip_path = None
        # Only for traceroute-based RIBs.
        if 'ip-path' in element['fields']:
            ip_path = element['fields']['ip-path'].split(' ')
        for hop in range(len(as_path)):
            lneighbor, asn, rneighbor = map(parse_asn,
                                            get_as_triple(as_path, hop))
            if ip_path:
                # We treat both IPs and ASNs as strings, so we can apply
                # the same function.
                lneighbor_ip, ip, rneighbor_ip = \
                    map(parse_asn, get_as_triple(ip_path, hop))
            # The for loops below are necessary since every hop might
            # be an AS set...
            # Increment counter for AS
            for idx, entry in enumerate(asn):
                if ip_path and ip[idx] == '*':
                    continue
                data[entry]['count'] += 1
                if ip_path:
                    data[entry]['unique_ips'].add(ip[idx])
            # Handle left neighbors
            for idx, neighbor in enumerate(lneighbor):
                if ip_path and lneighbor_ip[idx] == '*':
                    continue
                for as_idx, entry in enumerate(asn):
                    if ip_path and ip[as_idx] == '*':
                        continue
                    data[entry]['lneighbors'][neighbor] += 1
            # Handle right neighbors
            for idx, neighbor in enumerate(rneighbor):
                if ip_path and rneighbor_ip[idx] == '*':
                    continue
                for as_idx, entry in enumerate(asn):
                    if ip_path and ip[as_idx] == '*':
                        continue
                    data[entry]['rneighbors'][neighbor] += 1
        as_paths_in_msg += 1
    return as_paths_in_msg


def flush_data(data: dict,
               total_as_paths: int,
               start_output_ts: int,
               end_output_ts: int,
               writer: KafkaWriter):
    start_ts_str = datetime.fromtimestamp(start_output_ts, tz=timezone.utc) \
        .strftime(DATE_FMT)
    end_ts_str = datetime.fromtimestamp(end_output_ts, tz=timezone.utc) \
        .strftime(DATE_FMT)
    logging.info(f'Flushing range {start_ts_str} - {end_ts_str}: {len(data)} '
                 f'ASes.')
    msg = {'start_timestamp': start_output_ts,
           'end_timestamp': end_output_ts,
           'total_as_paths': total_as_paths}
    for asn in data:
        msg['asn'] = asn
        data[asn]['unique_ips'] = tuple(data[asn]['unique_ips'])
        msg.update(data[asn])
        writer.write(asn, msg, end_output_ts * 1000)


def make_data_dict() -> dict:
    return {'count': 0,
            'unique_ips': set(),
            'lneighbors': defaultdict(int),
            'rneighbors': defaultdict(int)}


def process_interval(start_output_ts: int,
                     end_output_ts: int,
                     reader: KafkaReader,
                     writer: KafkaWriter) -> None:
    data = defaultdict(make_data_dict)
    total_as_paths = 0
    smallest_ts = None
    largest_ts = None
    for msg in reader.read():
        if check_key('rec', msg) or check_key('time', msg['rec']):
            continue
        msg_ts = int(msg['rec']['time'])
        if not smallest_ts or msg_ts < smallest_ts:
            smallest_ts = msg_ts
        if not largest_ts or msg_ts > largest_ts:
            largest_ts = msg_ts
        total_as_paths += process_msg(msg, data)
    if start_output_ts == OFFSET_BEGINNING:
        # Neither args.start nor args.start_output were specified, so use the
        # detected timestamp, converted to milliseconds.
        start_output_ts = smallest_ts
    if end_output_ts == OFFSET_END:
        end_output_ts = largest_ts
    flush_data(data, total_as_paths, start_output_ts, end_output_ts, writer)


def main() -> None:
    log_fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        level=logging.INFO,
        filename='compute_as_visibility.log',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    desc = """Compute the AS visibility based on AS paths visible at a
           collector. Counts the occurrences of each AS number in the AS paths
           of the specified range. The --start and --stop parameters are used to
           specify the actual range that is read, whereas --start-output and
           --end-output can be used to override the timestamps that are written
           to the output topic."""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('config')
    parser.add_argument('-s', '--start', help='Start timestamp (as UNIX epoch '
                                              'in seconds or milliseconds, or '
                                              'in YYYY-MM-DDThh:mm format)')
    parser.add_argument('-e', '--stop', help='Stop timestamp (as UNIX epoch '
                                             'in seconds or milliseconds, or '
                                             'in YYYY-MM-DDThh:mm format)')
    parser.add_argument('-so', '--start-output',
                        help='Start output timestamp (as UNIX epoch in seconds '
                             'or milliseconds, or in YYYY-MM-DDThh:mm format)')
    parser.add_argument('-eo', '--end-output',
                        help='End output timestamp (as UNIX epoch in seconds '
                             'or milliseconds, or in YYYY-MM-DDThh:mm format)')
    args = parser.parse_args()

    logging.info(f'Started: {sys.argv}')

    config = verify_config(args.config)
    if not config.sections():
        sys.exit(1)

    start_ts = OFFSET_BEGINNING
    start_output_ts = start_ts
    if args.start:
        start_ts = parse_timestamp_argument(args.start) * 1000
        start_output_ts = start_ts // 1000
        if start_ts == 0:
            logging.error(f'Invalid start timestamp: {args.start}')
            sys.exit(1)
    end_ts = OFFSET_END
    end_output_ts = end_ts
    if args.stop:
        end_ts = parse_timestamp_argument(args.stop) * 1000
        end_output_ts = end_ts // 1000
        if end_ts == 0:
            logging.error(f'invalid stop timestamp: {args.stop}')
            sys.exit(1)
    if args.end_output:
        end_output_ts = parse_timestamp_argument(args.end_output)
        if end_output_ts == 0:
            logging.error(f'invalid output timestamp: {args.end_output}')
            sys.exit(1)
    if args.start_output:
        start_output_ts = parse_timestamp_argument(args.start_output)
        if start_output_ts == 0:
            logging.error(f'invalid output timestamp: {args.start_output}')
            sys.exit(1)
    logging.info(f'Timestamps: start: {start_ts} start_output: '
                 f'{start_output_ts} end: {end_ts} end_output: {end_output_ts}')

    rib_topic = 'ihr_bgp_' + config.get('input', 'collector') + '_ribs'
    update_topic = 'ihr_bgp_' + config.get('input', 'collector') + '_updates'

    reader = KafkaReader([rib_topic, update_topic],
                         config.get('kafka', 'bootstrap_servers'),
                         start_ts,
                         end_ts)
    writer = KafkaWriter(config.get('output', 'kafka_topic'),
                         config.get('kafka', 'bootstrap_servers'),
                         num_partitions=10,
                         replication_factor=2,
                         # 2 months
                         config={'retention.ms': 5184000000})

    with reader, writer:
        process_interval(start_output_ts, end_output_ts, reader, writer)


if __name__ == '__main__':
    main()
    sys.exit(0)
