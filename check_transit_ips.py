import argparse
import bz2
import configparser
import logging
import os
import pickle
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from kafka_wrapper.kafka_reader import KafkaReader
from kafka_wrapper.kafka_writer import KafkaWriter
from network_dependency.utils.helper_functions import check_key, check_keys, \
    parse_timestamp_argument

# 1. Read 'asn' from ihr_hegemony_classification_bgp_only_dependencies
#    with 'transit_ips' > 0.
# 2. Read 'scope' from ihr_hegemony with 'asn' == asn from 1.
#    Map asn to scope.
# 3. Read 'transit_ips' from ihr_bgp_traceroutev4_topology_as_visibility
#    with 'asn' == asn from 1. Use configured lookback interval.
#    Map transit_ip to asn. Check if one IP is mapped to multiple ASes.
# 4. Read 'full-ip-path', 'as-path' from
#    ihr_bgp_traceroutev4_topology_ribs within timeframe.
#    If IP is in transit_ip -> asn mapping, check if as-path[-1] is the
#    scope from asn -> scope mapping.

DATE_FMT = '%Y-%m-%dT%H:%M'
OUTPUT_EXTENSION = '.csv'
OUTPUT_DELIMITER = ','


@dataclass
class Pair:
    good: int = 0
    bad: int = 0


def check_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(config_path)
    try:
        config.get('input', 'visibility_topic')
        config.get('input', 'classification_topic')
        config.get('input', 'bgp_hegemony_topic')
        config.get('input', 'rib_topic')
        config.get('output', 'kafka_topic')
        config.getint('settings', 'lookback_days')
        config.get('kafka', 'bootstrap_servers')
    except configparser.NoSectionError as e:
        logging.error(f'Missing section in config file: {e}')
        return configparser.ConfigParser()
    except configparser.NoOptionError as e:
        logging.error(f'Missing option in config file: {e}')
        return configparser.ConfigParser()
    except ValueError as e:
        logging.error(f'Malformed option in config file: {e}')
        return configparser.ConfigParser()
    output_specified = config.get('output', 'data_directory', fallback=None) \
                       or config.get('output', 'kafka_topic', fallback=None)
    if not output_specified:
        logging.error('No output specified in config file. At least one of '
                      '[data_directory, kafka_topic] is required.')
        return configparser.ConfigParser()
    return config


def read_transit_asns(reader: KafkaReader) -> set:
    ret = set()
    for msg in reader.read():
        required_keys = ['asn', 'transit_ips']
        if check_keys(required_keys, msg):
            logging.error(f'One of {required_keys} missing in message: {msg}')
            continue
        transit_ips = int(msg['transit_ips'])
        if transit_ips > 0 and msg['asn'] not in ret:
            ret.add(msg['asn'])
    return ret


def read_asn_scopes(reader: KafkaReader, asns: set) -> dict:
    ret = defaultdict(set)
    for msg in reader.read():
        required_keys = ['asn', 'scope']
        if check_keys(required_keys, msg):
            logging.error(f'One of {required_keys} missing in message: {msg}')
            continue
        asn = msg['asn']
        scope = msg['scope']
        if scope == '-1' or asn == scope:
            continue
        if asn in asns:
            ret[asn].add(scope)
    return ret


def read_transit_ips(reader: KafkaReader, asns: set) -> dict:
    ret = defaultdict(set)
    covered_asns = set()
    for msg in reader.read():
        required_keys = ['asn', 'transit_ips']
        if check_keys(required_keys, msg):
            logging.error(f'One of {required_keys} missing in message: {msg}')
            continue
        asn = msg['asn']
        if asn not in asns:
            continue
        transit_ips = pickle.loads(bz2.decompress(msg['transit_ips']))
        for ip in transit_ips:
            if ip in ret and asn not in ret[ip]:
                logging.warning(f'IP {ip} maps to multiple ASes: {ret[ip]}, '
                                f'{asn}')
            ret[ip].add(asn)
            if asn not in covered_asns:
                covered_asns.add(asn)
    uncovered_asns = asns - covered_asns
    if uncovered_asns:
        logging.error(f'Missing transit IP coverage for ASes: {covered_asns}')
    return ret


def parse_hop(hop: str) -> set:
    if hop.startswith('{'):
        return set(hop.strip('{}').split(','))
    return {hop}


def process_traceroutes(reader: KafkaReader, ip_asn_map: dict,
                        asn_scope_map: dict) -> dict:
    ret = defaultdict(Pair)
    for msg in reader.read():
        if check_key('elements', msg):
            logging.error(f'No "elements" key in message: {msg}')
            continue
        for element in msg['elements']:
            if check_key('fields', element) \
                    or check_keys(['as-path', 'full-ip-path'],
                                  element['fields']):
                logging.error(f'Malformed element in message {msg}')
                continue
            scope = parse_hop(element['fields']['as-path'].split(' ')[-1])
            ip_set = set()
            for ip in map(parse_hop,
                          element['fields']['full-ip-path'].split(' ')):
                ip_set.update(ip)
            transit_ips = ip_set.intersection(ip_asn_map.keys())
            for transit_ip in transit_ips:
                for asn in ip_asn_map[transit_ip]:
                    if not asn_scope_map[asn].intersection(scope):
                        ret[asn].bad += 1
                    else:
                        ret[asn].good += 1
    return ret


def write_csv_output(data_dir: str,
                     input_topic: str,
                     start_ts_dt: datetime,
                     dependencies: dict) -> None:
    if not data_dir.endswith('/'):
        data_dir += '/'
    output_file = data_dir + 'transit_ips.' + input_topic + '.' + \
                  start_ts_dt.strftime(DATE_FMT) + OUTPUT_EXTENSION
    value_lines = [(asn, pair.good, pair.bad)
                   for asn, pair in dependencies.items()]
    value_lines.sort(key=lambda e: (e[1], e[2]))
    output_lines = [('asn', 'good', 'bad')] + value_lines
    os.makedirs(data_dir, exist_ok=True)
    logging.info(f'Writing {len(dependencies)} entries to file {output_file}')
    with open(output_file, 'w') as f:
        f.write('\n'.join([OUTPUT_DELIMITER.join(map(str, line))
                           for line in output_lines]))


def write_kafka_output(output_topic: str,
                       bootstrap_servers: str,
                       output_ts: int,
                       dependencies: dict) -> None:
    logging.info(f'Writing {len(dependencies)} entries to topic {output_topic}')
    writer = KafkaWriter(output_topic,
                         bootstrap_servers,
                         num_partitions=10,
                         # 2 months
                         config={'retention.ms': 5184000000})
    msg = {'timestamp': int(output_ts / 1000)}
    with writer:
        for asn, pair in dependencies.items():
            msg['asn'] = asn
            msg['good'] = pair.good
            msg['bad'] = pair.bad
            writer.write(msg['asn'], msg, output_ts)


def main() -> None:
    log_fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        level=logging.INFO,
        filename='check_transit_ips.log',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('timestamp', help='Timestamp (as UNIX epoch in seconds '
                                          'or milliseconds, or in '
                                          'YYYY-MM-DDThh:mm format)')
    args = parser.parse_args()

    logging.info(f'Started {sys.argv}')

    config = check_config(args.config)
    if not config.sections():
        sys.exit(1)

    start_ts = parse_timestamp_argument(args.timestamp) * 1000
    if start_ts == 0:
        logging.error(f'Invalid timestamp specified: {args.timestamp}')
        sys.exit(1)
    end_ts = start_ts + 1
    start_ts_dt = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc)
    lookback_days = config.getint('settings', 'lookback_days')
    lookback_start_ts_dt = start_ts_dt - timedelta(days=lookback_days)
    lookback_start_ts = int(lookback_start_ts_dt.timestamp()) * 1000

    logging.info(f'Checking timestamp {start_ts_dt.strftime(DATE_FMT)}')
    logging.info(f'Starting lookback at '
                 f'{lookback_start_ts_dt.strftime(DATE_FMT)}')

    bootstrap_servers = config.get('kafka', 'bootstrap_servers')

    classification_topic = config.get('input', 'classification_topic')
    classification_reader = KafkaReader([classification_topic],
                                        bootstrap_servers,
                                        start_ts,
                                        end_ts)
    logging.info(f'Reading dependencies with transit IPs from '
                 f'{classification_topic}')
    with classification_reader:
        transit_asns = read_transit_asns(classification_reader)
    logging.info(f'Read {len(transit_asns)} ASes')
    if not transit_asns:
        sys.exit(0)

    hegemony_topic = config.get('input', 'bgp_hegemony_topic')
    hegemony_reader = KafkaReader([hegemony_topic],
                                  bootstrap_servers,
                                  start_ts,
                                  end_ts)
    logging.info(f'Reading scopes from {hegemony_topic}')
    with hegemony_reader:
        asn_scope_map = read_asn_scopes(hegemony_reader, transit_asns)
    if transit_asns - asn_scope_map.keys():
        logging.warning(f'Failed to find scopes for '
                        f'{len(transit_asns - asn_scope_map.keys())} ASes')

    visibility_topic = config.get('input', 'visibility_topic')
    visibility_reader = KafkaReader([visibility_topic],
                                    bootstrap_servers,
                                    lookback_start_ts,
                                    end_ts)
    logging.info(f'Reading transit IPs from {visibility_topic}')
    with visibility_reader:
        ip_asn_map = read_transit_ips(visibility_reader, transit_asns)
    logging.info(f'Read {len(ip_asn_map)} transit IPs')

    rib_topic = config.get('input', 'rib_topic')
    rib_reader = KafkaReader([rib_topic],
                             bootstrap_servers,
                             start_ts,
                             end_ts)
    logging.info(f'Reading traceroutes from {rib_topic}')
    with rib_reader:
        checked_dependencies = \
            process_traceroutes(rib_reader, ip_asn_map, asn_scope_map)
    if transit_asns - checked_dependencies.keys():
        logging.warning(f'No results for ASes: '
                        f'{transit_asns - checked_dependencies.keys()}')

    data_dir = config.get('output', 'data_directory', fallback=None)
    if data_dir:
        write_csv_output(data_dir,
                         classification_topic,
                         start_ts_dt,
                         checked_dependencies)

    output_topic = config.get('output', 'kafka_topic', fallback=None)
    if output_topic:
        write_kafka_output(output_topic,
                           bootstrap_servers,
                           start_ts,
                           checked_dependencies)


if __name__ == '__main__':
    main()
    sys.exit(0)
