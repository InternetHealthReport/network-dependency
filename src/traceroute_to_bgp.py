import configparser
import logging
import sys
from datetime import datetime, timezone
from ASPath import ASPath
from KafkaReader import KafkaReader
from KafkaWriter import KafkaWriter
from IPLookup import IPLookup
import AtlasAPIHelper

stats = {'total': 0,
         'dnf': 0,
         'single_as': 0,
         'start_as_missing': 0,
         'end_as_missing': 0}


def convert_date_to_epoch(s) -> int:
    """Parse date from config file"""
    try:
        return int(datetime.strptime(s, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc).timestamp())
    except ValueError:
        # Not a valid date:
        return 0


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


def process_message(msg: dict, lookup: IPLookup, msm_ids=None,
                    target_asn=None) -> dict:
    global stats
    if msm_ids is not None and msg['msm_id'] not in msm_ids:
        return dict()
    dst_addr = AtlasAPIHelper.get_dst_addr(msg)
    if not dst_addr:
        return dict()
    dst_asn = lookup.ip2asn(dst_addr)
    if dst_asn == 0:
        logging.error('Failed to look up destination AS for destination address {}'
                      .format(dst_addr))
        return dict()
    if target_asn is not None and dst_asn != target_asn:
        return dict()
    prefix = lookup.ip2prefix(dst_addr)
    if not prefix:
        logging.error('Failed to look up prefix for destination address {}'
                      .format(dst_addr))
        return dict()
    if 'from' not in msg or not msg['from']:
        logging.error('No "from" in result {}'.format(msg))
        return dict()
    peer_asn = lookup.ip2asn(msg['from'])
    if peer_asn == 0:
        logging.error('Failed to look up peer_asn for peer_address {}'
                      .format(msg['from']))
        return dict()
    stats['total'] += 1
    path = ASPath()
    path.set_start_end_asn(peer_asn, dst_asn)
    traceroute = msg['result']
    for hop in traceroute:
        asn, address = process_hop(hop, lookup)
        if asn == -1:
            return dict()
        path.append(asn, address)
    reduced_path, reduced_path_len = path.get_reduced_path(stats)
    if reduced_path_len <= 1:
        logging.warning('Reduced AS path is too short (<=1 AS): {}'
                        .format(reduced_path))
        stats['single_as'] += 1
        return dict()
    if dst_asn != 7500:
        print(dst_asn)
    ret = {'rec': {'status': 'valid',
                   'time': unified_timestamp},
           'elements': [{
               'type': 'R',
               'peer_address': msg['from'],
               'peer_asn': peer_asn,
               'fields': {
                   'as-path': reduced_path,
                   'prefix': prefix
                   }
               }]
           }
    return ret


def print_stats() -> None:
    if stats['total'] == 0:
        print('No values.')
        return
    p = 100 / stats['total']
    print(f'           Total: {stats["total"]:6d} {100:6.2f}%')
    print(f'             DNF: {stats["dnf"]:6d} {p * stats["dnf"]:6.2f}%')
    print(f'       Single AS: {stats["single_as"]:6d} {p * stats["single_as"]:6.2f}%')
    print(f'Start AS missing: {stats["start_as_missing"]:6d} {p * stats["start_as_missing"]:6.2f}%')
    print(f'  End AS Missing: {stats["end_as_missing"]:6d} {p * stats["end_as_missing"]:6.2f}%')


if __name__ == '__main__':
    # Logging
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT, filename='traceroute_to_bgp.log',
        level=logging.WARNING, datefmt='%Y-%m-%d %H:%M:%S'
        )
    logging.info("Started: %s" % sys.argv)

    # Read config
    config = configparser.ConfigParser()
    config.read('../conf/default.ini')
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
    reader = KafkaReader([traceroute_kafka_topic], bootstrap_servers, start * 1000, stop * 1000)
    writer = KafkaWriter(output_kafka_topic, bootstrap_servers)
    with reader, writer:
        for msg in reader.read():
            data = process_message(msg, lookup, msm_ids, target_asn)
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
    print_stats()
