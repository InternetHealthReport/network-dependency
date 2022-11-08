import argparse
import configparser
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import psutil
import numpy as np
from confluent_kafka import OFFSET_BEGINNING, OFFSET_END

from kafka_wrapper.kafka_reader import KafkaReader
from network_dependency.utils.helper_functions import parse_timestamp_argument
from metis.shared_extract_functions import AS_HOPS_FEATURE, IP_HOPS_FEATURE, \
                                     RTT_FEATURE, VALID_FEATURES, VALID_MODES, \
                                     extract_as_hops, extract_ip_hops, \
                                     extract_rtts, filter_by_peers, \
                                     process_window


DATE_FMT = '%Y-%m-%dT%H:%M'
DATE_FMT_SHORT = '%Y-%m-%d'
DATA_SUFFIX = '.csv.gz'
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


def square_matrix(m: dict) -> None:
    peer_set = m.keys()
    dst_set = set()
    for peer in m:
        dst_set.update(m[peer].keys())
    dst_no_peer = dst_set - peer_set
    peer_no_dst = peer_set - dst_set
    if dst_no_peer:
        logging.info(f'Found {len(dst_no_peer)} destinations without peer entry.')
    if peer_no_dst:
        logging.warning(f'Found {len(peer_no_dst)} peers without destination entry.')
    for peer in dst_no_peer:
        m[peer] = dict()


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
        filename='extract_traceroute_features.log',
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

    peer_ids = None
    peer_id_file = config.get('options', 'peer_id_file', fallback=None)
    if peer_id_file:
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

    windows = defaultdict(list)
    first_window_start = datetime.fromtimestamp(start_ts, tz=timezone.utc) \
                           .replace(hour=0, minute=0, second=0)
    last_window_end = datetime.fromtimestamp(end_ts, tz=timezone.utc) \
                              .replace(hour=0, minute=0, second=0)
    if window_lengths:
        for window_length in window_lengths:
            window_start = first_window_start
            window_end = window_start + timedelta(days=window_length)
            while window_end <= last_window_end:
                logging.info(f'Processing window '
                             f'{window_start.strftime(DATE_FMT)}'
                             f' -- {window_end.strftime(DATE_FMT)}')
                windows[window_length]\
                  .append((window_start,
                           window_end,
                           process_window(daily_feature_values,
                                          window_start,
                                          window_end,
                                          peer_ids)))
                window_start += timedelta(days=window_offset)
                window_end += timedelta(days=window_offset)
    else:
        filter_by_peers(daily_feature_values[day], peer_ids)
        windows[None].append((first_window_start,
                              last_window_end,
                              daily_feature_values[day]))

    output_path = config.get('output', 'path')
    if not output_path.endswith('/'):
        output_path += '/'

    for curr_window_length, curr_windows in windows.items():
        for curr_window_start, curr_window_end, feature_values in curr_windows:
            if curr_window_length is None:
                curr_output_path = \
                f'{output_path}{curr_window_start.strftime(DATE_FMT_SHORT)}' \
                f'--{curr_window_end.strftime(DATE_FMT_SHORT)}/'
            else:
                curr_output_path = \
                f'{output_path}{curr_window_length}/' \
                f'{curr_window_start.strftime(DATE_FMT_SHORT)}' \
                f'--{curr_window_end.strftime(DATE_FMT_SHORT)}/'

            os.makedirs(curr_output_path, exist_ok=True)
            for feature in enabled_features:
                logging.info(f'Processing feature: {feature}')
                connections_sparse = feature_values[feature]
                square_matrix(connections_sparse)
                asns = list(connections_sparse.keys())
                asns.sort()
                asn_idx = {asn: idx for idx, asn in enumerate(asns)}
                num_asns = len(asns)
                logging.info(f'Creating {num_asns} x {num_asns} array.')
                connections_full = np.zeros((num_asns, num_asns))
                total_entries = num_asns * num_asns
                entry_count = 0
                for peer in asns:
                    for dst in connections_sparse[peer]:
                        entry_count += 1
                        connections_full[asn_idx[peer], asn_idx[dst]] = \
                            connections_sparse[peer][dst]
                fill_percentage = entry_count / total_entries * 100
                logging.info(f'Filled {entry_count}/{total_entries} '
                            f'({fill_percentage:.2f}%) entries')
                output_file = \
                f'{curr_output_path}{input_topic}.{feature}{DATA_SUFFIX}'
                logging.info(f'Writing output to file: {output_file}')
                if feature == RTT_FEATURE:
                    np.savetxt(output_file,
                               connections_full,
                               '%f',
                               delimiter=DATA_DELIMITER,
                               header=DATA_DELIMITER.join(map(str, asns)))
                else:
                    np.savetxt(output_file,
                               connections_full,
                               '%d',
                               delimiter=DATA_DELIMITER,
                               header=DATA_DELIMITER.join(map(str, asns)))


if __name__ == '__main__':
    main()
    sys.exit(0)
