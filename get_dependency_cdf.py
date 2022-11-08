import argparse
import configparser
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone

from kafka_wrapper.kafka_reader import KafkaReader
from network_dependency.utils.helper_functions import check_keys, parse_timestamp_argument


DATE_FMT = '%Y-%m-%dT%H:%M'
OUTPUT_EXTENSION = '.csv'
OUTPUT_DELIMITER = ','


def check_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(config_path)
    try:
        config.get('input', 'bgp_topic')
        config.get('input', 'traceroute_topic')
        config.get('options', 'min_peers')
        config.get('options', 'min_dependencies')
        config.get('kafka', 'bootstrap_servers')
    except configparser.NoSectionError as e:
        logging.error(f'Missing section in config file: {e}')
        return configparser.ConfigParser()
    except configparser.NoOptionError as e:
        logging.error(f'Missing option in config file: {e}')
        return configparser.ConfigParser()
    return config


def read_dependent_scopes(reader: KafkaReader,
                          min_peers: int,
                          min_dependencies: int) -> dict:
    ret = defaultdict(list)
    required_keys = ['scope', 'asn', 'nb_peers', 'hege']
    for msg in reader.read():
        if check_keys(required_keys, msg):
            logging.info(f'Warning: Missing keys {required_keys} in ' \
            'message: {msg}')
            continue
        nb_peers = msg['nb_peers']
        if nb_peers < min_peers:
            continue
        scope = msg['scope']
        if scope == '-1':
            continue
        asn = msg['asn']
        hege = msg['hege']
        ret[asn].append((hege, scope))
    filtered_asns = list()
    for asn, dependent_scopes in ret.items():
        if len(dependent_scopes) < min_dependencies:
            filtered_asns.append(asn)
        else:
            dependent_scopes.sort()
    for asn in filtered_asns:
        ret.pop(asn)
    return ret


if __name__ == '__main__':
    log_fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        level=logging.INFO,
        filename='get_dependency_cdf.log',
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

    bgp_topic = config.get('input', 'bgp_topic')
    traceroute_topic = config.get('input', 'traceroute_topic')
    min_peers = config.getint('options', 'min_peers')
    min_dependencies = config.getint('options', 'min_dependencies')
    logging.info(f'min_peers: {min_peers} min_dependencies: {min_dependencies}')

    start_ts = parse_timestamp_argument(args.timestamp) * 1000
    if start_ts == 0:
        logging.error(f'Invalid timestamp specified: {args.timestamp}')
        sys.exit(1)
    end_ts = start_ts + 1
    start_ts_dt = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc)

    bootstrap_servers = config.get('kafka', 'bootstrap_servers')

    bgp_reader = KafkaReader([bgp_topic], bootstrap_servers, start_ts, end_ts)
    with bgp_reader:
        bgp_asns = read_dependent_scopes(bgp_reader, min_peers, min_dependencies)

    traceroute_reader = KafkaReader([traceroute_topic], bootstrap_servers, 
    start_ts, end_ts)
    with traceroute_reader:
        traceroute_asns = read_dependent_scopes(traceroute_reader, min_peers, 
        min_dependencies)
    
    logging.info(f'BGP: {len(bgp_asns)} Traceroute: {len(traceroute_asns)}')
    
    output_dir = config.get('output', 'directory')
    os.makedirs(output_dir, exist_ok=True)
    for asn in set(bgp_asns.keys()).intersection(traceroute_asns.keys()):
        out_file_bgp = f'{output_dir}/{asn}.{start_ts_dt.strftime(DATE_FMT)}.' \
            f'bgp{OUTPUT_EXTENSION}'
        logging.info(out_file_bgp)
        logging.info(bgp_asns[asn])
        with open(out_file_bgp, 'w') as f:
            f.write('\n'.join([OUTPUT_DELIMITER.join((str(hege), scope)) 
            for hege, scope in bgp_asns[asn]]))

        out_file_traceroute = f'{output_dir}/{asn}.' \
            f'{start_ts_dt.strftime(DATE_FMT)}.traceroute{OUTPUT_EXTENSION}'
        logging.info(out_file_traceroute)
        logging.info(traceroute_asns[asn])
        with open(out_file_traceroute, 'w') as f:
            f.write('\n'.join([OUTPUT_DELIMITER.join((str(hege), scope)) 
            for hege, scope in traceroute_asns[asn]]))
sys.exit(0)