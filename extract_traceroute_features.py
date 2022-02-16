import argparse
import configparser
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from itertools import permutations
from typing import Any

import numpy as np
from confluent_kafka import OFFSET_BEGINNING, OFFSET_END

from network_dependency.kafka.kafka_reader import KafkaReader
from network_dependency.utils.helper_functions import check_key, check_keys, \
                                                      parse_timestamp_argument


DATE_FMT = '%Y-%m-%dT%H:%M'
DATE_FMT_SHORT = '%Y-%m-%d'
DATA_SUFFIX = '.csv.gz'
DATA_DELIMITER = ','
AS_HOPS_FEATURE = 'as_hops'
IP_HOPS_FEATURE = 'ip_hops'
RTT_FEATURE = 'rtt'
VALID_FEATURES = {AS_HOPS_FEATURE, IP_HOPS_FEATURE, RTT_FEATURE}
AS_MODE = 'as'
PROBE_MODE = 'probe'
VALID_MODES = [AS_MODE, PROBE_MODE]


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


def get_source_identifier(msg: dict, mode: str) -> Any:
    if mode == AS_MODE:
        if check_key('src_asn', msg):
            logging.warning(f'Missing "src_asn" key in message: {msg}')
            return None
        return msg['src_asn']
    elif mode == PROBE_MODE:
        if check_key('prb_id', msg):
            logging.warning(f'Missing "prb_id" key in message: {msg}')
            return None
        return msg['prb_id']
    logging.error(f'Failed to find source identifier in message: {msg}')
    return None


def build_as_path(hops: list) -> list:
    as_path = list()
    included_ases = set()
    curr_hop = 0
    curr_as_set = set()
    for hop in hops:
        if check_keys(['hop', 'asn'], hop):
            logging.warning(f'Missing "hop" or "asn" key in hop: {hop}')
            return list()
        if hop['asn'] in included_ases or hop['asn'] <= 0:
            continue
        if hop['hop'] != curr_hop and curr_as_set:
            as_path.append((curr_hop, curr_as_set.copy()))
            curr_as_set.clear()
        curr_hop = hop['hop']
        if hop['asn'] not in curr_as_set:
            curr_as_set.add(hop['asn'])
        included_ases.add(hop['asn'])
    if curr_as_set:
        as_path.append((curr_hop, curr_as_set.copy()))
    return as_path


def process_as_path(dst_asn: int, as_path: list, hop_counts: dict) -> None:
    dst_asn_idx = -1
    for idx, (ip_hop, asns) in enumerate(as_path):
        if dst_asn in asns:
            dst_asn_idx = idx
            break
    if dst_asn_idx < 0:
        logging.debug(f'AS path did not reach destination AS {dst_asn}: '
                      f'{as_path}')
        return
    for position, (ip_hop, peer_asns) in enumerate(as_path[:dst_asn_idx]):
        hop_count = dst_asn_idx - position
        for peer_asn in peer_asns:
            if dst_asn not in hop_counts[peer_asn]:
                hop_counts[peer_asn][dst_asn] = hop_count
            if hop_count < hop_counts[peer_asn][dst_asn]:
                hop_counts[peer_asn][dst_asn] = hop_count


def process_neighor_ases(as_path: list, hop_counts: dict) -> None:
    for peer_idx, (peer_ip_hop, peer_asns) in enumerate(as_path):
        curr_ip_hop = peer_ip_hop
        next_idx = peer_idx + 1
        while next_idx < len(as_path):
            dst_ip_hop, dst_asns = as_path[next_idx]
            # Broken chain.
            if dst_ip_hop != curr_ip_hop + 1:
                break
            hop_count = dst_ip_hop - peer_ip_hop
            for peer_asn in peer_asns:
                for dst_asn in dst_asns:
                    if dst_asn not in hop_counts[peer_asn]:
                        hop_counts[peer_asn][dst_asn] = hop_count
                    if hop_count < hop_counts[peer_asn][dst_asn]:
                        hop_counts[peer_asn][dst_asn] = hop_count
            curr_ip_hop = dst_ip_hop
            next_idx += 1


def extract_as_hops(msg: dict, hop_counts: dict, mode: str) -> None:
    src_id = get_source_identifier(msg, mode)
    if src_id is None:
        return
    if check_key('hops', msg):
        logging.warning(f'Missing "hops" key in message: {msg}')
        return
    as_path = build_as_path(msg['hops'])
    if not as_path:
        return
    if check_key('dst_asn', msg):
        logging.debug('Skipping AS path check since "dst_asn" key is missing')
    else:
        process_as_path(msg['dst_asn'], as_path, hop_counts)
    # process_neighor_ases(as_path, hop_counts)


def extract_ip_hops(msg: dict, hop_counts: dict, mode: str) -> None:
    src_id = get_source_identifier(msg, mode)
    if src_id is None:
        return
    if check_key('hops', msg):
        logging.warning(f'Missing "hops" key in message: {msg}')
        return
    hops = msg['hops']
    for idx, peer_hop in enumerate(hops):
        if check_keys(['hop', 'asn'], peer_hop):
            logging.warning(f'Missing "hop" or "asn" key in hop: {peer_hop}')
            continue
        peer_asn = peer_hop['asn']
        for dst_hop in hops[idx + 1:]:
            if check_keys(['hop', 'asn'], dst_hop):
                logging.warning(f'Missing "hop" or "asn" key in hop: {dst_hop}')
                continue
            dst_asn = dst_hop['asn']
            if dst_asn <= 0 or peer_asn == dst_asn:
                continue
            hop_count = dst_hop['hop'] - peer_hop['hop']
            if dst_asn not in hop_counts[peer_asn]:
                hop_counts[peer_asn][dst_asn] = hop_count
            if hop_count < hop_counts[peer_asn][dst_asn]:
                hop_counts[peer_asn][dst_asn] = hop_count


def extract_rtts(msg: dict, rtts: dict, mode: str) -> None:
    src_id = get_source_identifier(msg, mode)
    if src_id is None:
        return
    if check_key('hops', msg):
        logging.warning(f'Missing "hops" key in message: {msg}')
        return
    hops = msg['hops']
    for hop in hops:
        # Not all hops have RTT values and this is expected.
        if check_key('rtt', hop):
            continue
        if check_key('asn', hop):
            logging.warning(f'Missing "asn" key in hop: {hop}')
            continue
        rtt = hop['rtt']
        dst_asn = hop['asn']
        if dst_asn <= 0 or dst_asn == src_id:
            continue
        if dst_asn not in rtts[src_id]:
            rtts[src_id][dst_asn] = rtt
        if rtt < rtts[src_id][dst_asn]:
            rtts[src_id][dst_asn] = rtt


def filter_by_peers(day_values: dict, peer_ids: set) -> None:
    if not peer_ids:
        return

    for feature in day_values:
        day_values[feature] = \
            {peer: {dst: day_values[feature][peer][dst]
                    for dst in peer_ids
                    if dst in day_values[feature][peer]}
            for peer in peer_ids}


def process_window(daily_values: dict,
                   window_start: datetime,
                   window_end: datetime,
                   peer_ids: set) -> dict:
    curr_day = window_start
    curr_ts = int(curr_day.timestamp())
    window_data = daily_values[curr_ts]
    filter_by_peers(window_data, peer_ids)

    curr_day += timedelta(days=1)
    while curr_day < window_end:
        curr_ts = int(curr_day.timestamp())
        for feature in window_data:
            if peer_ids:
                for peer, dst in permutations(peer_ids, 2):
                    curr_val = None
                    if dst in window_data[feature][peer]:
                        curr_val = window_data[feature][peer][dst]
                    other_val = None
                    if dst in daily_values[curr_ts][feature][peer]:
                        other_val = daily_values[curr_ts][feature][peer][dst]
                    if curr_val is None and other_val is not None \
                      or curr_val is not None and other_val is not None \
                      and other_val < curr_val:
                        window_data[feature][peer][dst] = other_val
            else:
                for peer in window_data[feature]:
                    # Check for all destinations from peer that are
                    # in the window if they have a daily value and if
                    # that value is smaller.
                    for dst, curr_val in window_data[feature][peer].items():
                        if dst in daily_values[curr_ts][feature][peer] and \
                          daily_values[curr_ts][feature][peer][dst] < curr_val:
                            window_data[feature][peer][dst] = \
                              daily_values[curr_ts][feature][peer][dst]
                    # Possible destinations from peer that are not
                    # currently in the window_data.
                    for dst in daily_values[curr_ts][feature][peer].keys() \
                      - window_data[feature][peer].keys():
                        window_data[feature][peer][dst] = \
                          daily_values[curr_ts][feature][peer][dst]
                # Possible peers that are not currently in the
                # window_data
                for peer in daily_values[curr_ts][feature].keys() \
                  - window_data[feature].keys():
                    window_data[feature][peer] = \
                      daily_values[curr_ts][feature][peer]
        curr_day += timedelta(days=1)
    return window_data


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
        windows[None].append((window_start,
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
