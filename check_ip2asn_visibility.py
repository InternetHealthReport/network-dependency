import argparse
import configparser
import logging
import os
import sys
from collections import namedtuple
from datetime import datetime, timezone

from network_dependency.kafka.kafka_reader import KafkaReader
from network_dependency.kafka.kafka_writer import KafkaWriter
from network_dependency.utils.helper_functions import \
    parse_timestamp_argument, check_keys
from network_dependency.utils.ip_lookup import IPLookup, Visibility

DATE_FMT = '%Y-%m-%dT%H:%M'
OUTPUT_EXTENSION = '.csv'
OUTPUT_DELIMITER = ','

DEPENDENCY_FIELDS = ['asn', 'unique_ips', 'transit_ips', 'last_hop_ips',
                     'rib_prefix_count', 'rib_prefix_ip_sum',
                     'ixp_prefix_count', 'ixp_prefix_ip_sum']
Dependency = namedtuple('Dependency', DEPENDENCY_FIELDS,
                        defaults=[0] * len(DEPENDENCY_FIELDS))


def check_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(config_path)
    try:
        config.get('input', 'kafka_topic')
        config.get('kafka', 'bootstrap_servers')
        config.get('ip2asn', 'path')
        config.get('ip2asn', 'db')
        config.get('ip2ixp', 'kafka_bootstrap_servers')
        config.get('ip2ixp', 'ix_kafka_topic')
        config.get('ip2ixp', 'netixlan_kafka_topic')
    except configparser.NoSectionError as e:
        logging.error(f'Missing section in config file: {e}')
        return configparser.ConfigParser()
    except configparser.NoOptionError as e:
        logging.error(f'Missing option in config file: {e}')
        return configparser.ConfigParser()
    output_specified = config.get('output', 'data_directory', fallback=None) \
                       or config.get('output', 'kafka_topic', fallback=None)
    if not output_specified:
        logging.error('No output specified in config file. At least one of '
                      '[data_directory, kafka_topic] is required.')
        return configparser.ConfigParser()
    return config


def process_msg(msg: dict, lookup: IPLookup) -> Dependency:
    keys = ['asn', 'unique_ips', 'transit_ips', 'last_hop_ips']
    if check_keys(keys, msg):
        logging.warning(f'Missing keys {keys} in message: {msg}')
        return Dependency()
    asn = msg['asn']
    unique_ips = msg['unique_ips']
    transit_ips = msg['transit_ips']
    last_hop_ips = msg['last_hop_ips']
    visibility: Visibility = lookup.asn2source(asn)
    return Dependency(asn,
                      unique_ips,
                      transit_ips,
                      last_hop_ips,
                      visibility.ip2asn_prefixes.prefix_count,
                      visibility.ip2asn_prefixes.prefix_ip_sum,
                      visibility.ip2ixp_prefixes.prefix_count,
                      visibility.ip2ixp_prefixes.prefix_ip_sum)


def write_csv_output(data_dir: str,
                     input_topic: str,
                     start_ts_dt: datetime,
                     dependencies: list) -> None:
    if not data_dir.endswith('/'):
        data_dir += '/'
    output_file = data_dir + 'ip2asn_visibility.' + input_topic + '.' + \
                  start_ts_dt.strftime(DATE_FMT) + OUTPUT_EXTENSION
    output_lines = [('asn', 'unique_ips', 'transit_ips', 'last_hop_ips',
                     'rib_prefix_count', 'rib_prefix_ip_sum',
                     'ixp_prefix_count', 'ixp_prefix_ip_sum')] + dependencies
    os.makedirs(data_dir, exist_ok=True)
    logging.info(f'Writing {len(dependencies)} entries to file {output_file}')
    with open(output_file, 'w') as f:
        f.write('\n'.join([OUTPUT_DELIMITER.join(map(str, line))
                           for line in output_lines]))


def write_kafka_output(output_topic: str,
                       bootstrap_servers: str,
                       output_ts: int,
                       dependencies: list) -> None:
    logging.info(f'Writing {len(dependencies)} entries to topic {output_topic}')
    writer = KafkaWriter(output_topic,
                         bootstrap_servers,
                         num_partitions=10,
                         # 2 months
                         config={'retention.ms': 5184000000})
    msg = {'timestamp': int(output_ts / 1000)}
    with writer:
        for dependency in dependencies:
            msg.update(dependency._asdict())
            writer.write(msg['asn'], msg, output_ts)


def main() -> None:
    log_fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        level=logging.INFO,
        filename='check_ip2asn_visibility.log',
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

    lookup = IPLookup(config)
    logging.info(f'Loaded {len(lookup.i2asn_ipv4_asns)} RIB ASes')
    logging.info(f'Loaded {len(lookup.ixp_ipv4_asns)} IXP ASes')

    start_ts = parse_timestamp_argument(args.timestamp) * 1000
    if start_ts == 0:
        logging.error(f'Invalid timestamp specified: {args.timestamp}')
        sys.exit(1)
    end_ts = start_ts + 1
    start_ts_dt = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc)
    logging.info(f'Checking timestamp {start_ts_dt.strftime(DATE_FMT)}')

    input_topic = config.get('input', 'kafka_topic')
    bootstrap_servers = config.get('kafka', 'bootstrap_servers')
    kafka_reader = KafkaReader([input_topic], bootstrap_servers, start_ts,
                               end_ts)
    dependencies = list()
    with kafka_reader:
        for msg in kafka_reader.read():
            dependency = process_msg(msg, lookup)
            if dependency.asn == '0':
                continue
            dependencies.append(dependency)
    # Sort by ascending unique_ips, rip_prefix_count, ixp_prefix_count
    dependencies.sort(key=lambda t: (t[1], t[4], t[6]))
    logging.info(f'Read {len(dependencies)} ASes from kafka topic.')

    data_dir = config.get('output', 'data_directory', fallback=None)
    if data_dir:
        write_csv_output(data_dir, input_topic, start_ts_dt, dependencies)

    output_topic = config.get('output', 'kafka_topic', fallback=None)
    if output_topic:
        write_kafka_output(output_topic,
                           bootstrap_servers,
                           start_ts,
                           dependencies)


if __name__ == '__main__':
    main()
    sys.exit(0)
