import argparse
import logging
import sys
from datetime import datetime, timezone

import msgpack
from confluent_kafka import Consumer, TopicPartition, TIMESTAMP_CREATE_TIME, \
    OFFSET_INVALID

from network_dependency.utils.helper_functions import parse_timestamp_argument

BOOTSTRAP_SERVERS = 'localhost:9092'
DATE_FMT = '%Y-%m-%dT%H:%M'
OUT_FILE_SUFFIX = '.csv'
OUT_FILE_DELIMITER = ','
TIMEOUT_THRESHOLD = 30
TOPIC = 'ihr_hegemony_traceroutev4_topology'
ts = OFFSET_INVALID
partition_total = 0


def on_assign(consumer: Consumer, partitions: list) -> None:
    global partition_total
    partition_total = len(partitions)
    for p in partitions:
        p.offset = ts
    offsets = consumer.offsets_for_times(partitions)
    logging.info(f'Assigning partitions {offsets}')
    consumer.assign(offsets)


def main() -> None:
    global ts
    desc = f"""Retrieve (scope, number of peers) pairs for topology hegemony
               data for the specified timestamp. Timestamp can be specified as 
               UNIX epoch in (milli)seconds or in the format '%Y-%m-%dT%H:%M'.
               Output is written to file scopes.<timestamp>{OUT_FILE_SUFFIX}"""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('timestamp')
    args = parser.parse_args()

    log_fmt = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(f'Started: {sys.argv}')
    ts = parse_timestamp_argument(args.timestamp) * 1000
    if ts == 0:
        logging.error(f'Invalid timestamp specified: {args.timestamp}')
        sys.exit(1)
    ts_string = datetime.fromtimestamp(ts / 1000, tz=timezone.utc) \
        .strftime(DATE_FMT)
    logging.info(f'Start reading topic {TOPIC} at timestamp {ts_string}')

    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': TOPIC + '_scope_reader'
    })
    timeout_count = 0
    partitions_paused = 0
    scope_peer_map = dict()
    try:
        consumer.subscribe([TOPIC], on_assign=on_assign)
        while True:
            if timeout_count >= TIMEOUT_THRESHOLD:
                logging.info('Timeout threshold reached. Aborting.')
                break
            msg = consumer.poll(1)
            if not msg:
                timeout_count += 1
                continue
            if msg.error():
                logging.error(f'Consumer error: {msg.error()}')
                continue
            msg_ts = msg.timestamp()
            if msg_ts[0] != TIMESTAMP_CREATE_TIME:
                logging.error(f'Message timestamp is broken: {msg}')
                continue
            if msg_ts[1] != ts:
                logging.info(f'Partition {msg.partition()} finished')
                consumer.pause([TopicPartition(msg.topic(), msg.partition())])
                partitions_paused += 1
                if partitions_paused < partition_total:
                    continue
                else:
                    break
            val = msgpack.unpackb(msg.value(), raw=False)
            if 'scope' not in val or not val['scope'] \
                    or 'nb_peers' not in val or not val['nb_peers']:
                logging.warning(f'"scope" or "nb_peers" field missing in msg: '
                                f'{val}')
                continue
            if val['scope'] not in scope_peer_map:
                scope_peer_map[val['scope']] = val['nb_peers']
            elif scope_peer_map[val['scope']] != val['nb_peers']:
                logging.critical(f'CRITICAL: Number of peers does not match '
                                 f'for scope {val["scope"]}')
    finally:
        consumer.close()

    out_file = 'scopes.' + ts_string + OUT_FILE_SUFFIX
    logging.info(f'Writing {len(scope_peer_map)} scopes to file {out_file}')
    with open(out_file, 'w') as f:
        f.write(OUT_FILE_DELIMITER.join(['as', 'peers']) + '\n')
        f.write('\n'.join([OUT_FILE_DELIMITER.join(map(str, t))
                           for t in sorted(scope_peer_map.items(),
                                           key=lambda k: k[1],
                                           reverse=True)]) + '\n')


if __name__ == '__main__':
    main()
    sys.exit(0)
