import argparse
import bz2
import configparser
import logging
import os
import pickle
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import psutil
from confluent_kafka import OFFSET_BEGINNING, OFFSET_END

sys.path.append('../')
from kafka_wrapper.kafka_reader import KafkaReader
from network_dependency.utils.helper_functions import parse_timestamp_argument
from shared_extract_functions import AS_HOPS_FEATURE, IP_HOPS_FEATURE, \
                                     RTT_FEATURE, VALID_FEATURES, VALID_MODES, \
                                     extract_as_hops, extract_ip_hops, \
                                     extract_rtts, process_window


DATE_FMT = '%Y-%m-%dT%H:%M'
DATE_FMT_SHORT = '%Y-%m-%d'
DATA_SUFFIX = '.candidates.pickle.bz2'
DATA_DELIMITER = ','


def parse_csv(value: str) -> list:
    return [entry.strip() for entry in value.split(',')]


def check_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser(converters={'csv': parse_csv})
    config.read(config_path)
    try:
        config.get('input', 'kafka_topic')
        config.get('output', 'path')
        enabled_features = config.getcsv('options', 'enabled_features')
        mode = config.get('options', 'mode')
        config.get('options', 'peer_id_file')
        config.get('kafka', 'bootstrap_servers')
    except configparser.NoSectionError as e:
        logging.error(f'Missing section in config file: {e}')
        return configparser.ConfigParser()
    except configparser.NoOptionError as e:
        logging.error(f'Missing option in config file: {e}')
        return configparser.ConfigParser()
    for feature in enabled_features:
        if feature not in VALID_FEATURES:
            logging.error(f'Invalid feature specified: {feature}')
            return configparser.ConfigParser()
    if mode not in VALID_MODES:
        logging.error(f'Invalid mode specified: {mode}')
        return configparser.ConfigParser()
    return config


def read_probe_asns(asn_file: str) -> set:
    with open(asn_file, 'r') as f:
        return {int(l.strip().split(',')[0]) for l in f.readlines()[1:]}


def remove_probe_peers(m: dict, peer_ids: set) -> None:
    curr_peers = set(m.keys())
    for peer in curr_peers:
        if peer not in peer_ids:
            # Remove results coming from non-probe peers.
            m.pop(peer)
            continue
        for dst in peer_ids.intersection(m[peer].keys()):
            # Remove results going to probe peers.
            m[peer].pop(dst)
        if len(m[peer]) == 0:
            # Remove peer if no results are left.
            m.pop(peer)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('-s', '--start',
                        help='Start timestamp (as UNIX epoch in seconds or '
                             'milliseconds, or in YYYY-MM-DDThh:mm format)')
    parser.add_argument('-e', '--stop',
                        help='Stop timestamp (as UNIX epoch in seconds or '
                             'milliseconds, or in YYYY-MM-DDThh:mm format)')
    window_desc = """Apply a sliding window over the specified time interval.
                     The window length is specified in days with the
                     --window-length parameter, the amount of days to slide
                     the window each iteration with --window-slide-offset."""
    window_options = parser.add_argument_group(title='Sliding Window',
                                               description=window_desc)
    window_options.add_argument('--window-length', type=int, nargs='+',
                                help='window length in days')
    window_options.add_argument('--window-slide-offset', type=int,
                                help='slide offset in days')
    args = parser.parse_args()

    log_fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        level=logging.INFO,
        filename='extract_candidates.log',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(f'Started: {sys.argv}')

    config = check_config(args.config)
    if not config.sections():
        sys.exit(1)

    if args.window_length is None and args.window_slide_offset is not None \
        or args.window_length is not None and args.window_slide_offset is None:
        logging.error('Window mode requires both --window-length and '
                      '--window-slide-offset parameters.')
        sys.exit(1)
    window_lengths = args.window_length
    window_offset = args.window_slide_offset

    peer_id_file = config.get('options', 'peer_id_file')
    logging.info(f'Reading peer identifiers from {peer_id_file}')
    peer_ids = read_probe_asns(peer_id_file)
    logging.info(f'Read {len(peer_ids)} peer identifiers')

    start_ts = OFFSET_BEGINNING
    start_ts_arg = args.start
    if start_ts_arg:
        start_ts = parse_timestamp_argument(start_ts_arg)
        if start_ts == 0:
            logging.error(f'Invalid start timestamp specified: {start_ts_arg}')
            sys.exit(1)
        logging.info(f'Starting read at timestamp: {start_ts} '
            f'{datetime.utcfromtimestamp(start_ts).strftime("%Y-%m-%dT%H:%M")}')
        start_ts *= 1000

    end_ts = OFFSET_END
    read_to_end = True
    end_ts_arg = args.stop
    if end_ts_arg:
        read_to_end = False
        end_ts = parse_timestamp_argument(end_ts_arg)
        if end_ts == 0:
            logging.error(f'Invalid end timestamp specified: {end_ts_arg}')
            sys.exit(1)
        logging.info(f'Ending read at timestamp: {end_ts} '
            f'{datetime.utcfromtimestamp(end_ts).strftime("%Y-%m-%dT%H:%M")}')
        end_ts *= 1000

    mode = config.get('options', 'mode')
    enabled_features = config.getcsv('options', 'enabled_features')
    daily_feature_values = \
      defaultdict(lambda: {feature: defaultdict(dict)
                           for feature in enabled_features})
    input_topic = config.get('input', 'kafka_topic')

    reader = KafkaReader([input_topic],
                         config.get('kafka', 'bootstrap_servers'),
                         start_ts,
                         end_ts)
    if start_ts != OFFSET_BEGINNING:
        start_ts //= 1000
    if end_ts != OFFSET_END:
        end_ts //= 1000
    # First, read the entire time interval. If no window is
    # specified, use a placeholder day value.
    logging.info('Reading entire time interval.')
    day = 0
    check_count = 0
    with reader:
        for msg in reader.read():
            if start_ts == OFFSET_BEGINNING:
                start_ts = msg['timestamp']
            if read_to_end:
                end_ts = msg['timestamp']
            if window_lengths:
                day = int(datetime.fromtimestamp(msg['timestamp'],
                                                 tz=timezone.utc) \
                                  .replace(hour=0, minute=0, second=0) \
                                  .timestamp())
            if AS_HOPS_FEATURE in enabled_features:
                extract_as_hops(msg,
                                daily_feature_values[day][AS_HOPS_FEATURE],
                                mode)
            if IP_HOPS_FEATURE in enabled_features:
                extract_ip_hops(msg,
                                daily_feature_values[day][IP_HOPS_FEATURE],
                                mode)
            if RTT_FEATURE in enabled_features:
                extract_rtts(msg,
                             daily_feature_values[day][RTT_FEATURE],
                             mode)
            check_count += 1
            if check_count >= 1000000:
                mem_pct = psutil.virtual_memory().percent
                logging.info(f'Memory usage: {mem_pct}%')
                if mem_pct > 90:
                    logging.error(f'Aborting due to too high memory usage: {psutil.virtual_memory()}')
                    sys.exit(1)
                check_count = 0

    first_window_start = datetime.fromtimestamp(start_ts, tz=timezone.utc) \
                           .replace(hour=0, minute=0, second=0)
    last_window_end = datetime.fromtimestamp(end_ts, tz=timezone.utc) \
                              .replace(hour=0, minute=0, second=0)

    output_path = config.get('output', 'path')
    if not output_path.endswith('/'):
        output_path += '/'

    if window_lengths:
        for window_length in window_lengths:
            window_start = first_window_start
            window_end = window_start + timedelta(days=window_length)
            while window_end <= last_window_end:
                logging.info(f'Processing window '
                             f'{window_start.strftime(DATE_FMT)}'
                             f' -- {window_end.strftime(DATE_FMT)}')
                window_feature_values = process_window(daily_feature_values,
                                          window_start,
                                          window_end,
                                          peer_ids=None)
                curr_output_path = \
                f'{output_path}{window_length}/' \
                f'{window_start.strftime(DATE_FMT_SHORT)}' \
                f'--{window_end.strftime(DATE_FMT_SHORT)}/'
                os.makedirs(curr_output_path, exist_ok=True)
                for feature in enabled_features:
                    logging.info(f'Processing feature: {feature}')
                    connections_sparse = window_feature_values[feature]
                    remove_probe_peers(connections_sparse, peer_ids)
                    dst_map = defaultdict(dict)
                    for peer in connections_sparse:
                        for dst, value in connections_sparse[peer].items():
                            dst_map[dst][peer] = value
                    logging.info(f'Found {len(dst_map)} new destinations.')
                    output_file = \
                    f'{curr_output_path}{input_topic}.{feature}{DATA_SUFFIX}'
                    logging.info(f'Writing output to file: {output_file}')
                    with bz2.open(output_file, 'w') as f:
                        pickle.dump(dst_map, f)
                del window_feature_values
                window_feature_values = None
                window_start += timedelta(days=window_offset)
                window_end += timedelta(days=window_offset)
    else:
        window_feature_values = daily_feature_values[day]
        curr_output_path = \
        f'{output_path}{first_window_start.strftime(DATE_FMT_SHORT)}' \
        f'--{last_window_end.strftime(DATE_FMT_SHORT)}/'
        os.makedirs(curr_output_path, exist_ok=True)
        for feature in enabled_features:
            logging.info(f'Processing feature: {feature}')
            connections_sparse = window_feature_values[feature]
            remove_probe_peers(connections_sparse, peer_ids)
            dst_map = defaultdict(dict)
            for peer in connections_sparse:
                for dst, value in connections_sparse[peer].items():
                    dst_map[dst][peer] = value
            logging.info(f'Found {len(dst_map)} new destinations.')
            output_file = \
            f'{curr_output_path}{input_topic}.{feature}{DATA_SUFFIX}'
            logging.info(f'Writing output to file: {output_file}')
            with bz2.open(output_file, 'w') as f:
                pickle.dump(dst_map, f)


if __name__ == '__main__':
    main()
    sys.exit(0)
