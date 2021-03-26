import argparse
from collections import defaultdict
import configparser
from datetime import datetime, timedelta
import logging
import sys
import time
from network_dependency.kafka.kafka_reader import KafkaReader
from network_dependency.utils import atlas_api_helper
from network_dependency.utils.ip_lookup import IPLookup


def process_interval(start: datetime,
                     stop: datetime,
                     prefix_set: set,
                     asn_set: set):
    logging.info('Processing interval {} - {}'
                 .format(start.strftime('%Y-%m-%dT%H:%M'),
                         stop.strftime('%Y-%m-%dT%H:%M')))
    s1 = time.time()
    reader = KafkaReader(['ihr_atlas_traceroutev4'],
                         'kafka1:9092,kafka2:9092,kafka3:9092',
                         int(start.timestamp() * 1000),
                         int(stop.timestamp() * 1000))
    interval_dict = defaultdict(int)
    msg_count = 0
    with reader:
        for msg in reader.read():
            msg_count += 1
            if msm_ids is not None and msg['msm_id'] not in msm_ids:
                continue
            dst_addr = atlas_api_helper.get_dst_addr(msg)
            if not dst_addr:
                continue
            pfx = lookup.ip2prefix(dst_addr)
            if not pfx:
                continue
            if pfx not in prefix_set:
                prefix_set.add(pfx)
            dst_asn = lookup.ip2asn(dst_addr)
            if dst_asn == 0:
                continue
            if dst_asn not in asn_set:
                asn_set.add(dst_asn)
            peer_asn = 0
            if 'from' in msg and msg['from']:
                peer_asn = lookup.ip2asn(msg['from'])
            interval_dict[(peer_asn, dst_asn)] += 1
    s2 = time.time()
    logging.info('Read {} messages in {} s -> {} msg/s'
                 .format(msg_count, s2 - s1, int(msg_count / (s2 - s1))))
    with open('scan-stats/'
              + start.strftime('%Y-%m-%dT%H:%M')
              + '--'
              + stop.strftime('%Y-%m-%dT%H:%M') + '.dat', 'w') as f:
        f.write('peer_asn dst_asn count\n')
        for (peer_asn, dst_asn) in interval_dict:
            f.write(' '.join(map(str,
                                 [peer_asn,
                                  dst_asn,
                                  interval_dict[(peer_asn, dst_asn)]])) + '\n')


def interval_scan():
    end_time = datetime.utcnow().replace(hour=0, minute=0, second=0,
                                         microsecond=0)
    start_time = end_time - timedelta(hours=1)
    prefix_set = set()
    asn_set = set()
    process_interval(start_time, end_time, prefix_set, asn_set)
    unique_prefixes = 0
    unique_asns = 0
    while len(prefix_set) > unique_prefixes:
        new_asns = len(asn_set) - unique_asns
        unique_asns = len(asn_set)
        new_prefixes = len(prefix_set) - unique_prefixes
        unique_prefixes = len(prefix_set)
        logging.info('total prefixes: {} new: {} total asn: {} new: {}'
                     .format(unique_prefixes, new_prefixes, unique_asns,
                             new_asns))
        end_time = start_time
        start_time = end_time - timedelta(hours=1)
        process_interval(start_time, end_time, prefix_set, asn_set)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    # Logging
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT,  filename='topology_scanner.log',
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S',
        filemode='w')
    logging.info("Started: %s" % sys.argv)

    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)
    msm_ids = config.get('input', 'msm_ids', fallback=None)
    if msm_ids is not None:
        msm_ids = set(map(int, msm_ids.split(',')))
        logging.info('Filtering for msm ids: {}'.format(msm_ids))

    lookup = IPLookup(config)
    interval_scan()

