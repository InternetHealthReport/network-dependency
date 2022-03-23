import logging
from copy import deepcopy
from datetime import datetime, timedelta
from itertools import permutations
from typing import Any

from network_dependency.utils.helper_functions import check_key, check_keys


AS_HOPS_FEATURE = 'as_hops'
IP_HOPS_FEATURE = 'ip_hops'
RTT_FEATURE = 'rtt'
VALID_FEATURES = {AS_HOPS_FEATURE, IP_HOPS_FEATURE, RTT_FEATURE}
AS_MODE = 'as'
PROBE_MODE = 'probe'
VALID_MODES = [AS_MODE, PROBE_MODE]


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


def build_as_path(hops: list, src_as: int) -> list:
    as_path = list()
    included_ases = {src_as}
    curr_hop = 0
    curr_as_set = {src_as}
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
    src_as = get_source_identifier(msg, AS_MODE)
    if src_as is None:
        return
    if check_key('hops', msg):
        logging.warning(f'Missing "hops" key in message: {msg}')
        return
    as_path = build_as_path(msg['hops'], src_as)
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
    window_data = deepcopy(daily_values[curr_ts])
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
