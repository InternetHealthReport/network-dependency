import argparse
import bz2
import configparser
import logging
import pickle
import sys
from datetime import datetime

from kafka_wrapper.kafka_reader import KafkaReader
from kafka_wrapper.kafka_writer import KafkaWriter
from network_dependency.utils import atlas_api_helper
from network_dependency.utils.as_path import ASPath
from network_dependency.utils.helper_functions import convert_date_to_epoch, \
    parse_timestamp_argument
from iplookup.ip_lookup import IPLookup

stats = {'total': 0,
         'no_dst_addr': 0,
         'no_dst_asn': 0,
         'no_prefix': 0,
         'no_from': 0,
         'no_peer_asn': 0,
         'duplicate': 0,
         'changed_ip': 0,
         'accepted': 0,
         'dnf': 0,
         'empty_path': 0,
         'end_as_in_path': 0,
         'start_as_in_path': 0,
         'single_as': 0,
         'used': 0,
         'too_many_hops': 0,
         'errors': 0,
         'start_as_missing': 0,
         'end_as_missing': 0,
         'ixp_in_path': 0,
         'as_set': 0,
         'scopes': set()}

probe_ip_map = dict()


def process_hop(msg: dict, hop: dict, lookup: IPLookup, path: ASPath) -> bool:
    if 'error' in hop:
        # Packet send failed.
        logging.debug('Traceroute did not reach destination: {}'
                      .format(msg))
        stats['dnf'] += 1
        return True
    replies = hop['result']
    reply_addresses = set()
    errors_in_hop = set()
    for reply in replies:
        if 'error' in reply:
            # This seems to be a bug that happens if sending the packet
            # only fails for _some_ of the probes for a single hop.
            # Should be treated the same as 'error' in hop.
            stats['dnf'] += 1
            return True
        if 'err' in reply \
                and 'from' in reply \
                and path.contains_ip(reply['from']):
            # Reply received with ICMP error (e.g., network unreachable)
            # We allow this reply once, if the IP was not seen before.
            logging.debug(f'Skipping erroneous reply with existing IP: '
                          f'{msg}')
            continue
        if 'x' in reply:
            # Timeout
            reply_addresses.add('*')
            continue
        if 'from' not in reply:
            logging.debug('No "from" in hop {}'.format(msg))
            reply_addresses.add('*')
        else:
            if 'err' in reply:
                errors_in_hop.add(reply['err'])
            reply_addresses.add(reply['from'])
    if len(reply_addresses) == 0:
        logging.debug('Traceroute did not reach destination: {}'
                      .format(msg))
        stats['dnf'] += 1
        return True
    if hop['hop'] == 255:
        # Always interpret hop 255 as timeout. Some probes claim to
        # reach the target at hop 255 without any previous replies,
        # which is obviously not true.
        path.append('0', '*')
        path.flag_too_many_hops()
        return False
    if len(reply_addresses) > 1:
        logging.debug('Responses from different sources: {}.'
                      .format(reply_addresses))
        # Remove timeout placeholder from set (if
        # applicable) so that a real IP is chosen.
        reply_addresses.discard('*')
    if errors_in_hop:
        # However unlikely it is that we have two different errors
        # present in replies for the same hop, just in case, handle it.
        # For a single error, this results in a normal string.
        error_str = ' '.join(map(str, errors_in_hop))
    if len(reply_addresses) > 1:
        # Still a set after * removal. Add as set.
        as_set = list()
        ip_set = list()
        contains_ixp = False
        for address in reply_addresses:
            ixp = lookup.ip2ixpid(address)
            if ixp != 0:
                # We represent IXPs with negative "AS numbers".
                as_set.append(f'-{ixp}')
                as_set.append(f'-{ixp}_{lookup.ip2asn(address)}')
                as_set.append(address)
                # It is a bit stupid, but we need to keep the set
                # sizes equal.
                ip_set += [address] * 3
                contains_ixp = True
            as_set.append(lookup.ip2asn(address))
            ip_set.append(address)
        if errors_in_hop:
            path.mark_hop_error(error_str)
        path.append_set(tuple(as_set), tuple(ip_set), contains_ixp)
    else:
        address = reply_addresses.pop()
        if address == '*':
            path.append('0', '*')
        else:
            ixp = lookup.ip2ixpid(address)
            if ixp != 0:
                # We represent IXPs with negative "AS numbers".
                path.append(f'-{ixp}', address, ixp=True)
                path.append(f'-{ixp}_{lookup.ip2asn(address)}',
                            address, ixp=True)
                path.append(address, address, ixp=True)
            if errors_in_hop:
                path.mark_hop_error(error_str)
            path.append(lookup.ip2asn(address), address)
    return False


def process_message(msg: dict,
                    lookup: IPLookup,
                    seen_peer_prefixes: set,
                    unified_timestamp: int,
                    msm_ids=None,
                    probe_ids=None,
                    target_asn=None,
                    include_duplicates=False) -> dict:
    if msm_ids is not None and msg['msm_id'] not in msm_ids \
            or probe_ids is not None and msg['prb_id'] not in probe_ids:
        return dict()
    stats['total'] += 1
    dst_addr = atlas_api_helper.get_dst_addr(msg)
    if not dst_addr:
        stats['no_dst_addr'] += 1
        return dict()
    dst_asn = lookup.ip2asn(dst_addr)
    if dst_asn == '0':
        logging.debug('Failed to look up destination AS for destination'
                      ' address {}'.format(dst_addr))
        stats['no_dst_asn'] += 1
        return dict()
    if target_asn is not None and dst_asn != target_asn:
        return dict()
    prefix = lookup.ip2prefix(dst_addr)
    if not prefix:
        logging.debug('Failed to look up prefix for destination address {}'
                      .format(dst_addr))
        stats['no_prefix'] += 1
        return dict()
    if 'from' not in msg or not msg['from']:
        logging.debug('No "from" in result {}'.format(msg))
        stats['no_from'] += 1
        return dict()
    peer_asn = lookup.ip2asn(msg['from'])
    if peer_asn == '0':
        logging.debug('Failed to look up peer_asn for peer_address {}'
                      .format(msg['from']))
        stats['no_peer_asn'] += 1
        return dict()
    peer_prefix_tuple = (msg['from'], prefix)
    if peer_prefix_tuple in seen_peer_prefixes and not include_duplicates:
        logging.debug('Skipping duplicate result for peer {} prefix {}'
                      .format(*peer_prefix_tuple))
        stats['duplicate'] += 1
        return dict()
    if msg['prb_id'] in probe_ip_map \
            and msg['from'] != probe_ip_map[msg['prb_id']]:
        logging.debug('Probe {} changed IP during time window. {} -> {}'
                      .format(msg['prb_id'], probe_ip_map[msg['prb_id']],
                              msg['from']))
        stats['changed_ip'] += 1
    probe_ip_map[msg['prb_id']] = msg['from']
    stats['accepted'] += 1
    path = ASPath()
    path.set_start_end_asn(peer_asn, dst_asn)
    traceroute = msg['result']
    for hop in traceroute:
        if process_hop(msg, hop, lookup, path):
            return dict()
    reduced_path, reduced_ip_path, reduced_path_len = path.get_reduced_path(
        stats)
    if reduced_path_len == 0:
        return dict()
    elif reduced_path_len == 1:
        logging.debug('Reduced AS path is too short (=1 AS): {}'
                      .format(reduced_path))
        stats['single_as'] += 1
        return dict()
    raw_path, raw_ip_path = path.get_raw_path()
    stats['used'] += 1
    if path.has_too_many_hops():
        stats['too_many_hops'] += 1
    if path.has_errors():
        stats['errors'] += 1
    ret = {'rec': {'status': 'valid',
                   'time': unified_timestamp},
           'elements': [{
               'type': 'R',
               'peer_address': msg['from'],
               'peer_asn': peer_asn,
               'prb_id': msg['prb_id'],
               'fields': {
                   'as-path': reduced_path,
                   'ip-path': reduced_ip_path,
                   'ixp-path-indexes': path.get_reduced_ixp_indexes(),
                   'full-as-path': raw_path,
                   'full-ip-path': raw_ip_path,
                   'full-ixp-path-indexes': path.get_raw_ixp_indexes(),
                   'prefix': prefix,
                   'path-attributes': path.attributes
               }
           }]
           }
    if path.get_reduced_ixp_indexes():
        stats['ixp_in_path'] += 1
    if dst_asn not in stats['scopes']:
        stats['scopes'].add(dst_asn)
    seen_peer_prefixes.add(peer_prefix_tuple)
    return ret


def print_stats() -> None:
    if stats['total'] == 0:
        print('No values.')
        return
    p_total = 100 / stats['total']
    p_accepted = 100 / stats['accepted']
    p_used = 100 / stats['used']
    print(f'           Total: {stats["total"]:7d} '
          f'{100:6.2f}%')
    print(f'     No dst_addr: {stats["no_dst_addr"]:7d} '
          f'{p_total * stats["no_dst_addr"]:6.2f}%')
    print(f'      No dst_asn: {stats["no_dst_asn"]:7d} '
          f'{p_total * stats["no_dst_asn"]:6.2f}%')
    print(f'       No prefix: {stats["no_prefix"]:7d} '
          f'{p_total * stats["no_prefix"]:6.2f}%')
    print(f'         No from: {stats["no_from"]:7d} '
          f'{p_total * stats["no_from"]:6.2f}%')
    print(f'     No peer_asn: {stats["no_peer_asn"]:7d} '
          f'{p_total * stats["no_peer_asn"]:6.2f}%')
    print(f'      Duplicates: {stats["duplicate"]:7d} '
          f'{p_total * stats["duplicate"]:6.2f}%')
    print(f'      Changed IP: {stats["changed_ip"]:7d} '
          f'{p_total * stats["changed_ip"]:6.2f}%')
    print(f'        Accepted: {stats["accepted"]:7d} '
          f'{p_total * stats["accepted"]:6.2f}% '
          f'{100:6.2f}%')
    print(f'             DNF: {stats["dnf"]:7d} '
          f'{p_total * stats["dnf"]:6.2f}% '
          f'{p_accepted * stats["dnf"]:6.2f}%')
    print(f'      Empty path: {stats["empty_path"]:7d} '
          f'{p_total * stats["empty_path"]:6.2f}% '
          f'{p_accepted * stats["empty_path"]:6.2f}%')
    print(f'Start AS in path: {stats["start_as_in_path"]:7d} '
          f'{p_total * stats["start_as_in_path"]:6.2f}% '
          f'{p_accepted * stats["start_as_in_path"]:6.2f}%')
    print(f'  End AS in path: {stats["end_as_in_path"]:7d} '
          f'{p_total * stats["end_as_in_path"]:6.2f}% '
          f'{p_accepted * stats["end_as_in_path"]:6.2f}%')
    print(f'       Single AS: {stats["single_as"]:7d} '
          f'{p_total * stats["single_as"]:6.2f}% '
          f'{p_accepted * stats["single_as"]:6.2f}%')
    print(f'            Used: {stats["used"]:7d} '
          f'{p_total * stats["used"]:6.2f}% '
          f'{p_accepted * stats["used"]:6.2f}% '
          f'{100:6.2f}%')
    print(f'   Too many hops: {stats["too_many_hops"]:7d} '
          f'{p_total * stats["too_many_hops"]:6.2f}% '
          f'{p_accepted * stats["too_many_hops"]:6.2f}% '
          f'{p_used * stats["too_many_hops"]:6.2f}%')
    print(f'     With errors: {stats["errors"]:7d} '
          f'{p_total * stats["errors"]:6.2f}% '
          f'{p_accepted * stats["errors"]:6.2f}% '
          f'{p_used * stats["errors"]:6.2f}%')
    print(f'Start AS missing: {stats["start_as_missing"]:7d} '
          f'{p_total * stats["start_as_missing"]:6.2f}% '
          f'{p_accepted * stats["start_as_missing"]:6.2f}% '
          f'{p_used * stats["start_as_missing"]:6.2f}%')
    print(f'  End AS Missing: {stats["end_as_missing"]:7d} '
          f'{p_total * stats["end_as_missing"]:6.2f}% '
          f'{p_accepted * stats["end_as_missing"]:6.2f}% '
          f'{p_used * stats["end_as_missing"]:6.2f}%')
    print(f'     IXP in path: {stats["ixp_in_path"]:7d} '
          f'{p_total * stats["ixp_in_path"]:6.2f}% '
          f'{p_accepted * stats["ixp_in_path"]:6.2f}% '
          f'{p_used * stats["ixp_in_path"]:6.2f}%')
    print(f'  AS set in path: {stats["as_set"]:7d} '
          f'{p_total * stats["as_set"]:6.2f}% '
          f'{p_accepted * stats["as_set"]:6.2f}% '
          f'{p_used * stats["as_set"]:6.2f}%')
    print(f'          Scopes: {len(stats["scopes"]):7d}')


def read_probes(probes: str) -> set:
    if probes.endswith('.pickle.bz2'):
        logging.info(f'Reading prb_ids from object {probes}')
        with bz2.open(probes, 'rb') as f:
            ret = pickle.load(f)
        if not isinstance(ret, set):
            logging.error(f'Specified probes object is not a set: {ret}')
            return set()
    else:
        logging.info(f'Reading prb_ids from CSV string: {probes}')
        ret = set(probes.split(','))
    logging.info(f'Read {len(ret)} probes')
    return ret


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('-s', '--start', help='Start timestamp (as UNIX epoch '
                                              'in seconds or milliseconds, or '
                                              'in YYYY-MM-DDThh:mm format)')
    parser.add_argument('-e', '--stop', help='Stop timestamp (as UNIX epoch '
                                             'in seconds or milliseconds, or '
                                             'in YYYY-MM-DDThh:mm format)')
    parser.add_argument('--ixp2as-timestamp',
                        help='Start timestamp from which to reader ixp2as '
                             'Kafka topic')
    # Logging
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT, filename='traceroute_to_bgp.log',
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info("Started: %s" % sys.argv)

    args = parser.parse_args()

    # Read config
    config = configparser.ConfigParser()
    config.read(args.config)
    start_ts_argument = config.get('input', 'start', fallback=None)
    stop_ts_argument = config.get('input', 'stop', fallback=None)
    if args.start is not None:
        logging.info('Overriding config start timestamp.')
        start_ts_argument = args.start
    if args.stop is not None:
        logging.info('Overriding config stop timestamp.')
        stop_ts_argument = args.stop
    start = parse_timestamp_argument(start_ts_argument)
    stop = parse_timestamp_argument(stop_ts_argument)
    if start == 0 or stop == 0:
        logging.error('Invalid start or end time specified: {} {}'
                      .format(start_ts_argument, stop_ts_argument))
        exit(1)
    logging.info('Start timestamp: {} {}'
                 .format(datetime.utcfromtimestamp(start)
                         .strftime('%Y-%m-%dT%H:%M'), start))
    logging.info('Stop timestamp: {} {}'
                 .format(datetime.utcfromtimestamp(stop)
                         .strftime('%Y-%m-%dT%H:%M'), stop))
    ixp2as_timestamp_arg = args.ixp2as_timestamp
    ixp2as_timestamp = None
    if ixp2as_timestamp_arg is not None:
        ixp2as_timestamp = parse_timestamp_argument(ixp2as_timestamp_arg)
        if ixp2as_timestamp == 0:
            logging.error(f'Invalid ixp2as timestamp specified: '
                          f'{ixp2as_timestamp_arg}')
            sys.exit(1)
        ixp2as_timestamp *= 1000

    traceroute_kafka_topic = config.get('input', 'kafka_topic',
                                        fallback='ihr_atlas_traceroutev4')
    output_kafka_topic_prefix = config.get('output', 'kafka_topic_prefix',
                                           fallback='ihr_bgp_traceroutev4')
    output_kafka_topic = output_kafka_topic_prefix + '_ribs'
    msm_ids = config.get('input', 'msm_ids', fallback=None)
    if msm_ids is not None:
        msm_ids = set(map(int, msm_ids.split(',')))
        logging.info('Filtering for msm ids: {}'.format(msm_ids))
    target_asn = config.get('input', 'target_asn', fallback=None)
    prb_ids = config.get('input', 'prb_ids', fallback=None)
    if prb_ids is not None:
        prb_ids = read_probes(prb_ids)
        if len(prb_ids) == 0:
            logging.warning(f'Specified prb_ids parameter resulted in an empty '
                            f'set. Ignoring filter.')
            prb_ids = None
    if not target_asn:
        target_asn = None
    if target_asn is not None:
        logging.info('Filtering for target ASN: {}'.format(target_asn))
    output_timestamp = config.get('output', 'time', fallback=None)
    if output_timestamp is None:
        logging.warning('No output time specified. Using the stop timestamp.')
        unified_timestamp = stop
    else:
        unified_timestamp = convert_date_to_epoch(output_timestamp)
        if unified_timestamp == 0:
            logging.error('Invalid output time specified: {}'
                          .format(output_timestamp))
    logging.info('Output timestamp: {} {}'
                 .format(datetime.utcfromtimestamp(unified_timestamp)
                         .strftime('%Y-%m-%dT%H:%M'), unified_timestamp))
    bootstrap_servers = config.get('kafka', 'bootstrap_servers')
    include_duplicates = config.getboolean('input', 'include_duplicates',
                                           fallback=False)

    lookup = IPLookup(config, ixp2as_timestamp)
    seen_peer_prefixes = set()
    reader = KafkaReader([traceroute_kafka_topic], bootstrap_servers,
                         start * 1000, stop * 1000)
    writer = KafkaWriter(output_kafka_topic,
                         bootstrap_servers,
                         num_partitions=10,
                         # 2 months
                         config={'retention.ms': 5184000000})
    with reader, writer:
        for msg in reader.read():
            data = process_message(msg, lookup, seen_peer_prefixes,
                                   unified_timestamp, msm_ids, prb_ids,
                                   target_asn, include_duplicates)
            if not data:
                continue
            key = msg['prb_id']
            if type(key) == int:
                key = key.to_bytes(4, byteorder='big')
            writer.write(key,
                         data,
                         unified_timestamp * 1000)
    # Fake entry to force dump
    update_writer = KafkaWriter(output_kafka_topic_prefix + '_updates',
                                bootstrap_servers,
                                num_partitions=10,
                                # 2 months
                                config={'retention.ms': 5184000000})
    with update_writer:
        fake = {'rec': {'time': unified_timestamp + 1},
                'elements': [{
                    'type': 'A',
                    'time': unified_timestamp + 1,
                    'peer_address': '0.0.0.0',
                    'peer_asn': '0',
                    'fields': {
                        'prefix': '0.0.0.0/0'
                    }
                }]
                }
        update_writer.write(None, fake, (unified_timestamp + 1) * 1000)
    stats_writer = KafkaWriter(output_kafka_topic_prefix + '_stats',
                               bootstrap_servers,
                               num_partitions=10,
                               # 2 months
                               config={'retention.ms': 5184000000})
    with stats_writer:
        # Convert set to list so that msgpack does not explode.
        msm_id_list = list()
        if msm_ids:
            msm_id_list = list(msm_ids)
        stats['scopes'] = list(stats['scopes'])
        entry = {'start': start,
                 'stop': stop,
                 'msm_ids': msm_id_list,
                 'target_asn': target_asn,
                 'stats': stats}
        stats_writer.write(None, entry, unified_timestamp * 1000)
    print_stats()


if __name__ == '__main__':
    main()
