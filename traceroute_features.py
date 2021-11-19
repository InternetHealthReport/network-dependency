import argparse
import configparser
import logging
import sys
from datetime import datetime


from network_dependency.kafka.kafka_reader import KafkaReader
from network_dependency.kafka.kafka_writer import KafkaWriter
from network_dependency.utils import atlas_api_helper
from network_dependency.utils.helper_functions import parse_timestamp_argument
from network_dependency.utils.ip_lookup import IPLookup


DATE_FMT = '%Y-%m-%dT%H:%M'


def check_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(config_path)
    try:
        config.get('input', 'kafka_topic')
        config.get('output', 'kafka_topic')
        config.get('kafka', 'bootstrap_servers')
        config.get('ip2asn', 'path')
        config.get('ip2asn', 'db')
        config.get('ip2ixp', 'kafka_bootstrap_servers')
        config.get('ip2ixp', 'ix_kafka_topic')
        config.get('ip2ixp', 'netixlan_kafka_topic')
    except configparser.NoSectionError as e:
        logging.error(f'Missing section in config file: {e}')
        return configparser.ConfigParser()
    except configparser.NoOptionError as e:
        logging.error(f'Missing option in config file: {e}')
        return configparser.ConfigParser()
    return config


def process_hop(hop: dict, seen_ips: set, lookup: IPLookup) -> dict:
    if 'error' in hop or hop['hop'] == 255:
        # Packet send failed or end of traceroute reached.
        return
    replies = hop['result']
    reply_addresses = set()
    for reply in replies:
        if 'error' in reply:
            # This seems to be a bug that happens if sending the packet
            # only fails for _some_ of the probes for a single hop.
            # Should be treated the same as 'error' in hop.
            continue
        if 'err' in reply \
                and 'from' in reply \
                and reply['from'] in seen_ips:
            # Reply received with ICMP error (e.g., network unreachable)
            # We allow this reply once, if the IP was not seen before.
            continue
        # Timeout or no reply address
        if 'x' in reply or 'from' not in reply:
            continue
        address = reply['from']
        # Result already seen for this hop
        if address in reply_addresses:
            continue
        reply_addresses.add(address)
        if address not in seen_ips:
            seen_ips.add(address)
        asn = lookup.ip2asn(address)
        reply_hop = {'hop': hop['hop'],
                     'ip': address,
                     'asn': asn}
        if 'rtt' in reply:
            reply_hop['rtt'] = reply['rtt']
        if 'err' in reply:
            reply_hop['err'] = reply['err']
        yield reply_hop
        ixp = lookup.ip2ixpid(address)
        if ixp != 0:
            ixp_hop = reply_hop.copy()
            # We represent IXPs with negative "AS numbers".
            ixp_hop['asn'] = ixp * -1
            yield ixp_hop


def process_message(msg: dict, lookup: IPLookup) -> dict:
    dst_addr = atlas_api_helper.get_dst_addr(msg)
    if not dst_addr:
        return dict()
    dst_asn = lookup.ip2asn(dst_addr)
    if dst_asn == 0:
        return dict()
    if 'from' not in msg or not msg['from']:
        return dict()
    peer_asn = lookup.ip2asn(msg['from'])
    if peer_asn == 0:
        return dict()
    hops = msg['result']
    seen_ips = set()
    reply_hops = [reply_hop
                  for hop in hops
                  for reply_hop in process_hop(hop, seen_ips, lookup)]
    if not reply_hops:
        return dict()
    return {'prb_id': msg['prb_id'],
            'timestamp': msg['timestamp'],
            'src_ip': msg['from'],
            'src_asn': peer_asn,
            'dst_ip': dst_addr,
            'dst_asn': dst_asn,
            'hops': reply_hops}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('-s', '--start', help='Start timestamp (as UNIX epoch '
                                              'in seconds or milliseconds, or '
                                              'in YYYY-MM-DDThh:mm format)')
    parser.add_argument('-e', '--stop', help='Stop timestamp (as UNIX epoch '
                                             'in seconds or milliseconds, or '
                                             'in YYYY-MM-DDThh:mm format)')
    FORMAT = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=FORMAT, filename='traceroute_features.log',
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S'
    )
    logging.info(f'Started: {sys.argv}')

    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)
    start_ts_argument = args.start
    stop_ts_argument = args.stop
    start_ts = parse_timestamp_argument(start_ts_argument)
    if start_ts == 0:
        logging.error(f'Invalid start time specified: {start_ts_argument}')
        sys.exit(1)
    logging.info(f'Start timestamp: '
                 f'{datetime.utcfromtimestamp(start_ts).strftime(DATE_FMT)} '
                 f'{start_ts}')

    stop_ts = parse_timestamp_argument(stop_ts_argument)
    if stop_ts == 0:
        logging.error(f'Invalid stop time specified: {stop_ts_argument}')
        sys.exit(1)
    logging.info(f'Stop timestamp: '
                 f'{datetime.utcfromtimestamp(stop_ts).strftime(DATE_FMT)} '
                 f'{stop_ts}')

    input_topic = config.get('input', 'kafka_topic')
    output_topic = config.get('output', 'kafka_topic')

    bootstrap_servers = config.get('kafka', 'bootstrap_servers')

    lookup = IPLookup(config)
    reader = KafkaReader([input_topic],
                         bootstrap_servers,
                         start_ts * 1000,
                         stop_ts * 1000)
    writer = KafkaWriter(output_topic,
                         bootstrap_servers,
                         num_partitions=10,
                         # 2 months
                         config={'retention.ms': 5184000000})
    with reader, writer:
        for msg in reader.read():
            data = process_message(msg, lookup)
            if not data:
                continue
            writer.write(data['prb_id'].to_bytes(4, 'big'), data, data['timestamp'] * 1000)


if __name__ == '__main__':
    main()
