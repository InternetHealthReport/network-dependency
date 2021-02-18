import argparse
from collections import defaultdict
import configparser
from datetime import datetime
import logging
import sys
from network_dependency.utils.as_path import ASPath
from network_dependency.kafka.kafka_reader import KafkaReader
from network_dependency.kafka.kafka_writer import KafkaWriter
from network_dependency.utils.ip_lookup import IPLookup
from network_dependency.utils import atlas_api_helper
from network_dependency.utils.helper_functions import convert_date_to_epoch

stats = {'total': 0,
         'no_dst_addr': 0,
         'no_dst_asn': 0,
         'no_prefix': 0,
         'no_from': 0,
         'no_peer_asn': 0,
         'accepted': 0,
         'dnf': 0,
         'single_as': 0,
         'used': 0,
         'start_as_missing': 0,
         'end_as_missing': 0,
         'ixp_in_path': 0}


def process_hop(hop: dict, lookup: IPLookup) -> (int, str):
    global stats
    if 'error' in hop:
        logging.warning('Traceroute did not reach destination: {}'
                        .format(msg))
        stats['dnf'] += 1
        return -1, str()
    replies = hop['result']
    reply_addresses = set()
    for reply in replies:
        if 'error' in reply:
            logging.warning('Skipping erroneous reply in traceroute: {}'
                            .format(msg))
            continue
        if 'x' in reply:
            reply_addresses.add('*')
            continue
        if 'from' not in reply:
            logging.error('No "from" in hop {}'.format(msg))
            reply_addresses.add('*')
        else:
            reply_addresses.add(reply['from'])
    if len(reply_addresses) > 1:
        logging.debug('Responses from different sources: {}.'
                      .format(reply_addresses))
        # Remove timeout placeholder from set (if
        # applicable) so that a real IP is chosen.
        reply_addresses.discard('*')
    if len(reply_addresses) == 0:
        logging.warning('Traceroute did not reach destination: {}'
                        .format(msg))
        stats['dnf'] += 1
        return -1, str()
    address = reply_addresses.pop()
    if address == '*':
        return 0, str()
    else:
        return lookup.ip2asn(address), address


def process_message(msg: dict, lookup: IPLookup, msm_probe_map: dict,
                    msm_ids=None, target_asn=None) -> dict:
    global stats
    if msm_ids is not None and msg['msm_id'] not in msm_ids:
        return dict()
    if msg['prb_id'] in msm_probe_map[msg['msm_id']]:
        logging.debug('Skipping duplicate probe result for msm_id {} prb_id {}'
                      .format(msg['msm_id'], msg['prb_id']))
        return dict()
    stats['total'] += 1
    dst_addr = atlas_api_helper.get_dst_addr(msg)
    if not dst_addr:
        stats['no_dst_addr'] += 1
        return dict()
    dst_asn = lookup.ip2asn(dst_addr)
    if dst_asn == 0:
        logging.error('Failed to look up destination AS for destination'
                      ' address {}'.format(dst_addr))
        stats['no_dst_asn'] += 1
        return dict()
    if target_asn is not None and dst_asn != target_asn:
        return dict()
    prefix = lookup.ip2prefix(dst_addr)
    if not prefix:
        logging.error('Failed to look up prefix for destination address {}'
                      .format(dst_addr))
        stats['no_prefix'] += 1
        return dict()
    if 'from' not in msg or not msg['from']:
        logging.error('No "from" in result {}'.format(msg))
        stats['no_from'] += 1
        return dict()
    peer_asn = lookup.ip2asn(msg['from'])
    if peer_asn == 0:
        logging.error('Failed to look up peer_asn for peer_address {}'
                      .format(msg['from']))
        stats['no_peer_asn'] += 1
        return dict()
    stats['accepted'] += 1
    path = ASPath()
    path.set_start_end_asn(peer_asn, dst_asn)
    traceroute = msg['result']
    for hop in traceroute:
        asn, address = process_hop(hop, lookup)
        if asn == -1:
            return dict()
        is_ixp = False
        if lookup.ip2ixp(address):
            is_ixp = True
        path.append(asn, address, is_ixp)
    reduced_path, reduced_path_len = path.get_reduced_path(stats)
    if reduced_path_len <= 1:
        logging.warning('Reduced AS path is too short (<=1 AS): {}'
                        .format(reduced_path))
        stats['single_as'] += 1
        return dict()
    stats['used'] += 1
    ret = {'rec': {'status': 'valid',
                   'time': unified_timestamp},
           'elements': [{
               'type': 'R',
               'peer_address': msg['from'],
               'peer_asn': peer_asn,
               'fields': {
                   'as-path': reduced_path,
                   'ixp-path-indexes': path.get_reduced_ixp_indexes(),
                   'full-as-path': path.get_raw_path(),
                   'full-ixp-path-indexes': path.get_raw_ixp_indexes(),
                   'prefix': prefix
                   }
               }]
           }
    if path.get_reduced_ixp_indexes():
        stats['ixp_in_path'] += 1
    msm_probe_map[msg['msm_id']].add(msg['prb_id'])
    return ret


def print_stats() -> None:
    if stats['total'] == 0:
        print('No values.')
        return
    p_total = 100 / stats['total']
    p_accepted = 100 / stats['accepted']
    p_used = 100 / stats['used']
    print(f'           Total: {stats["total"]:6d} '
          f'{100:6.2f}%')
    print(f'     No dst_addr: {stats["no_dst_addr"]:6d} '
          f'{p_total * stats["no_dst_addr"]:6.2f}%')
    print(f'      No dst_asn: {stats["no_dst_asn"]:6d} '
          f'{p_total * stats["no_dst_asn"]:6.2f}%')
    print(f'       No prefix: {stats["no_prefix"]:6d} '
          f'{p_total * stats["no_prefix"]:6.2f}%')
    print(f'         No from: {stats["no_from"]:6d} '
          f'{p_total * stats["no_from"]:6.2f}%')
    print(f'     No peer_asn: {stats["no_peer_asn"]:6d} '
          f'{p_total * stats["no_peer_asn"]:6.2f}%')
    print(f'        Accepted: {stats["accepted"]:6d} '
          f'{p_total * stats["accepted"]:6.2f}% '
          f'{100:6.2f}%')
    print(f'             DNF: {stats["dnf"]:6d} '
          f'{p_total * stats["dnf"]:6.2f}% '
          f'{p_accepted * stats["dnf"]:6.2f}%')
    print(f'       Single AS: {stats["single_as"]:6d} '
          f'{p_total * stats["single_as"]:6.2f}% '
          f'{p_accepted * stats["single_as"]:6.2f}%')
    print(f'            Used: {stats["used"]:6d} '
          f'{p_total * stats["used"]:6.2f}% '
          f'{p_accepted * stats["used"]:6.2f}% '
          f'{100:6.2f}%')
    print(f'Start AS missing: {stats["start_as_missing"]:6d} '
          f'{p_total * stats["start_as_missing"]:6.2f}% '
          f'{p_accepted * stats["start_as_missing"]:6.2f}% '
          f'{p_used * stats["start_as_missing"]:6.2f}%')
    print(f'  End AS Missing: {stats["end_as_missing"]:6d} '
          f'{p_total * stats["end_as_missing"]:6.2f}% '
          f'{p_accepted * stats["end_as_missing"]:6.2f}% '
          f'{p_used * stats["end_as_missing"]:6.2f}%')
    print(f'     IXP in path: {stats["ixp_in_path"]:6d} '
          f'{p_total * stats["ixp_in_path"]:6.2f}% '
          f'{p_accepted * stats["ixp_in_path"]:6.2f}% '
          f'{p_used * stats["ixp_in_path"]:6.2f}%')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    # Logging
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT, filename='traceroute_to_bgp.log',
        level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S'
        )
    logging.info("Started: %s" % sys.argv)

    args = parser.parse_args()

    # Read config
    config = configparser.ConfigParser()
    config.read(args.config)
    start = convert_date_to_epoch(config.get('input', 'start'))
    stop = convert_date_to_epoch(config.get('input', 'stop'))
    if start == 0 or stop == 0:
        logging.error('Invalid start or end time specified: {} {}'
                      .format(config.get('input', 'start'),
                              config.get('input', 'stop')))
        exit(1)
    traceroute_kafka_topic = config.get('input', 'kafka_topic',
                                        fallback='ihr_atlas_traceroutev4')
    output_kafka_topic_prefix = config.get('output', 'kafka_topic_prefix',
                                           fallback='ihr_bgp_traceroutev4')
    output_kafka_topic = output_kafka_topic_prefix + '_ribs'
    msm_ids = config.get('input', 'msm_ids', fallback=None)
    if msm_ids is not None:
        msm_ids = set(map(int, msm_ids.split(',')))
        logging.info('Filtering for msm ids: {}'.format(msm_ids))
    target_asn = config.getint('input', 'target_asn', fallback=None)
    if not target_asn:
        target_asn = None
    if target_asn is not None:
        logging.info('Filtering for target ASN: {}'.format(target_asn))
    output_timestamp = config.get('output', 'time', fallback=None)
    if output_timestamp is None:
        unified_timestamp = int(datetime.utcnow().timestamp())
    else:
        unified_timestamp = convert_date_to_epoch(output_timestamp)
        if unified_timestamp == 0:
            logging.error('Invalid output time specified: {}'
                          .format(output_timestamp))
    logging.info('Output timestamp: {}'
                 .format(datetime.utcfromtimestamp(unified_timestamp)
                         .strftime('%Y-%m-%dT%H:%M')))
    bootstrap_servers = config.get('kafka', 'bootstrap_servers')

    lookup = IPLookup(config)
    msm_probe_map = defaultdict(set)
    reader = KafkaReader([traceroute_kafka_topic], bootstrap_servers, start * 1000, stop * 1000)
    writer = KafkaWriter(output_kafka_topic, bootstrap_servers)
    with reader, writer:
        for msg in reader.read():
            data = process_message(msg, lookup, msm_probe_map, msm_ids, target_asn)
            if not data:
                continue
            writer.write(None, data, unified_timestamp * 1000)
    # Fake entry to force dump
    update_writer = KafkaWriter(output_kafka_topic_prefix + '_updates', bootstrap_servers)
    with update_writer:
        fake = {'rec': {'time': unified_timestamp + 1},
                'elements': [{
                    'type': 'A',
                    'time': unified_timestamp + 1,
                    'peer_address': '0.0.0.0',
                    'peer_asn': 0,
                    'fields': {
                        'prefix': '0.0.0.0/0'
                        }
                    }]
                }
        update_writer.write(None, fake, (unified_timestamp + 1) * 1000)
    stats_writer = KafkaWriter(output_kafka_topic_prefix + '_stats', bootstrap_servers)
    with stats_writer:
        entry = {'start': start,
                 'stop': stop,
                 'msm_ids': list(msm_ids),
                 'target_asn': target_asn,
                 'stats': stats}
        stats_writer.write(None, entry, unified_timestamp * 1000)
    print_stats()
