import argparse
import configparser
import logging
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from network_dependency.kafka.kafka_reader import KafkaReader
from network_dependency.kafka.kafka_writer import KafkaWriter
from network_dependency.utils.helper_functions \
    import parse_timestamp_argument, check_keys

DATE_FMT = '%Y-%m-%dT%H:%M'


def check_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(config_path)
    try:
        config.get('input', 'visibility_topic')
        config.get('input', 'classification_topic')
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
    return config


def read_bgp_only_dependencies(reader: KafkaReader) -> set:
    bgp_only = set()
    others = set()
    for msg in reader.read():
        if check_keys(['equal', 'mismatched', 'tr_only', 'bgp_only'], msg):
            logging.warning(f'Skipping message with missing fields: {msg}')
            continue
        for asn, bgp_score, tr_score, rank in msg['equal']:
            if asn not in others:
                others.add(asn)
        for asn, bgp_score, bgp_rank, tr_score, tr_rank in msg['mismatched']:
            if asn not in others:
                others.add(asn)
        for asn, score, rank in msg['tr_only']:
            if asn not in others:
                others.add(asn)
        for asn, score, rank in msg['bgp_only']:
            if asn not in bgp_only:
                bgp_only.add(asn)
    return bgp_only - others


def read_visible_asns(reader: KafkaReader) -> dict:
    ret = defaultdict(set)
    for msg in reader.read():
        if check_keys(['asn', 'unique_ips'], msg):
            logging.warning(f'Skipping message with missing fields: {msg}')
            continue
        ret[msg['asn']].update(set(msg['unique_ips']))
    return ret


def main() -> None:
    log_fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        level=logging.INFO,
        filename='check_bgp_only_visibility.log',
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

    lookup_ts = parse_timestamp_argument(args.timestamp)
    if lookup_ts == 0:
        logging.error(f'Invalid timestamp specified: {args.timestamp}')
        sys.exit(1)
    lookback_days = config.getint('settings', 'lookback_days')
    lookup_ts_dt = datetime.fromtimestamp(lookup_ts, tz=timezone.utc)
    lookback_start_ts_dt = lookup_ts_dt - timedelta(days=lookback_days)
    lookback_start_ts = int(lookback_start_ts_dt.timestamp()) * 1000
    lookback_end_ts = lookup_ts * 1000 + 1

    logging.info(f'Checking timestamp {lookup_ts_dt.strftime(DATE_FMT)}')
    logging.info(f'Starting lookback at '
                 f'{lookback_start_ts_dt.strftime(DATE_FMT)}')

    visibility_topic = config.get('input', 'visibility_topic')
    classification_topic = config.get('input', 'classification_topic')
    bootstrap_servers = config.get('kafka', 'bootstrap_servers')

    visibility_reader = KafkaReader([visibility_topic], bootstrap_servers,
                                    lookback_start_ts, lookback_end_ts)
    with visibility_reader:
        visible_asns = read_visible_asns(visibility_reader)

    bgp_only_reader = KafkaReader([classification_topic], bootstrap_servers,
                                  lookup_ts * 1000, lookback_end_ts)
    with bgp_only_reader:
        bgp_only_asns = read_bgp_only_dependencies(bgp_only_reader)

    bgp_only_writer = KafkaWriter(config.get('output', 'kafka_topic'),
                                  bootstrap_servers,
                                  num_partitions=10,
                                  replication_factor=2,
                                  # 2 months
                                  config={'retention.ms': 5184000000})

    threshold_steps = list(set(map(len, visible_asns.values())))
    threshold_steps.sort(reverse=True)

    msg = {'timestamp': lookup_ts}

    logging.info(f'BGP only ASes: {len(bgp_only_asns):5d}')
    logging.info(f' Visible ASes: {len(visible_asns):5d}')
    logging.info(f'   Thresholds: {len(threshold_steps):5d}')
    remaining_visible_asns = visible_asns.keys()
    remaining_bgp_only_asns = bgp_only_asns.copy()
    with bgp_only_writer:
        for threshold in threshold_steps:
            thresh_asns = {asn for asn in remaining_visible_asns
                           if len(visible_asns[asn]) == threshold}
            remaining_visible_asns -= thresh_asns
            visible_with_threshold = \
                remaining_bgp_only_asns.intersection(thresh_asns)
            remaining_bgp_only_asns -= thresh_asns
            logging.debug(f' threshold {threshold:2d}')
            logging.debug(f'  remaining visible: '
                          f'{len(remaining_visible_asns):5d}')
            logging.debug(f'      visible w/ th: {len(thresh_asns):5d}')
            logging.debug(f'  bgp visible w/ th: '
                          f'{len(visible_with_threshold):5d}')
            logging.debug(f'remaining invisible: '
                          f'{len(remaining_bgp_only_asns):5d}')
            for asn in visible_with_threshold:
                msg['asn'] = asn
                msg['unique_ips'] = threshold
                bgp_only_writer.write(asn, msg, lookup_ts * 1000)
        for asn in remaining_bgp_only_asns:
            msg['asn'] = asn
            msg['unique_ips'] = 0
            bgp_only_writer.write(asn, msg, lookup_ts * 1000)


if __name__ == '__main__':
    main()
