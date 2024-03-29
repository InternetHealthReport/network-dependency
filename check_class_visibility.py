import argparse
import bz2
import configparser
import logging
import pickle
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from kafka_wrapper.kafka_reader import KafkaReader
from kafka_wrapper.kafka_writer import KafkaWriter
from network_dependency.utils.helper_functions \
    import parse_timestamp_argument, check_keys

DATE_FMT = '%Y-%m-%dT%H:%M'


def csv_converter(entry: str) -> list:
    return [e.strip() for e in entry.split(',')]


def check_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser(converters={'csv': csv_converter})
    config.read(config_path)
    try:
        config.get('input', 'visibility_topic')
        config.get('input', 'classification_topic')
        config.getcsv('input', 'included_classes')
        config.getcsv('input', 'excluded_classes')
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


def read_class_dependencies(reader: KafkaReader,
                            included_classes: list,
                            excluded_classes: list) -> (set, dict):
    included = set()
    excluded = set()
    included_scope_count = defaultdict(int)
    for msg in reader.read():
        if check_keys(included_classes + excluded_classes, msg):
            logging.warning(f'Skipping message with missing fields: {msg}')
            continue
        for class_name in included_classes:
            for entry in msg[class_name]:
                asn = entry[0]
                if asn not in included:
                    included.add(asn)
                included_scope_count[asn] += 1
        for class_name in excluded_classes:
            for entry in msg[class_name]:
                asn = entry[0]
                if asn not in excluded:
                    excluded.add(asn)
    return included - excluded, included_scope_count


def read_visible_asns(reader: KafkaReader) -> (dict, dict, dict):
    ret_unique = defaultdict(set)
    ret_transit = defaultdict(set)
    ret_last_hop = defaultdict(set)
    for msg in reader.read():
        if check_keys(['asn', 'transit_ips', 'last_hop_ips'], msg):
            logging.warning(f'Skipping message with missing fields: {msg}')
            continue
        transit = pickle.loads(bz2.decompress(msg['transit_ips']))
        last_hop = pickle.loads(bz2.decompress(msg['last_hop_ips']))
        ret_transit[msg['asn']].update(transit)
        ret_last_hop[msg['asn']].update(last_hop)
        ret_unique[msg['asn']].update(transit.union(last_hop))
    return ret_unique, ret_transit, ret_last_hop


def main() -> None:
    log_fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        level=logging.INFO,
        filename='check_class_visibility.log',
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
    included_classes = config.getcsv('input', 'included_classes')
    excluded_classes = config.getcsv('input', 'excluded_classes')
    bootstrap_servers = config.get('kafka', 'bootstrap_servers')

    visibility_reader = KafkaReader([visibility_topic], bootstrap_servers,
                                    lookback_start_ts, lookback_end_ts)
    with visibility_reader:
        visible_asns, visible_asns_transit, visible_asns_last_hop = \
            read_visible_asns(visibility_reader)

    class_reader = KafkaReader([classification_topic], bootstrap_servers,
                               lookup_ts * 1000, lookback_end_ts)
    with class_reader:
        included_asns, included_scope_count = \
            read_class_dependencies(class_reader, included_classes,
                                    excluded_classes)

    class_writer = KafkaWriter(config.get('output', 'kafka_topic'),
                               bootstrap_servers,
                               num_partitions=10,
                               replication_factor=2,
                               # 2 months
                               config={'retention.ms': 5184000000})

    threshold_steps = list(set(map(len, visible_asns.values())))
    threshold_steps.sort(reverse=True)

    msg = {'timestamp': lookup_ts}

    logging.info(f'Included ASes: {len(included_asns):5d}')
    logging.info(f' Visible ASes: {len(visible_asns):5d}')
    logging.info(f'   Thresholds: {len(threshold_steps):5d}')
    remaining_visible_asns = visible_asns.keys()
    remaining_included_asns = included_asns.copy()
    with class_writer:
        for threshold in threshold_steps:
            thresh_asns = {asn for asn in remaining_visible_asns
                           if len(visible_asns[asn]) == threshold}
            remaining_visible_asns -= thresh_asns
            visible_with_threshold = \
                remaining_included_asns.intersection(thresh_asns)
            remaining_included_asns -= thresh_asns
            logging.debug(f' threshold {threshold:2d}')
            logging.debug(f'  remaining visible: '
                          f'{len(remaining_visible_asns):5d}')
            logging.debug(f'      visible w/ th: {len(thresh_asns):5d}')
            logging.debug(f'  bgp visible w/ th: '
                          f'{len(visible_with_threshold):5d}')
            logging.debug(f'remaining invisible: '
                          f'{len(remaining_included_asns):5d}')
            for asn in visible_with_threshold:
                msg['asn'] = asn
                msg['scopes'] = included_scope_count[asn]
                msg['unique_ips'] = threshold
                msg['transit_ips'] = len(visible_asns_transit[asn])
                msg['last_hop_ips'] = len(visible_asns_last_hop[asn])
                class_writer.write(asn, msg, lookup_ts * 1000)
        for asn in remaining_included_asns:
            msg['asn'] = asn
            msg['scopes'] = included_scope_count[asn]
            msg['unique_ips'] = 0
            msg['transit_ips'] = 0
            msg['last_hop_ips'] = 0
            class_writer.write(asn, msg, lookup_ts * 1000)


if __name__ == '__main__':
    main()
