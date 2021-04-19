import argparse
import bz2
import logging
import os
import pickle
from collections import namedtuple
from datetime import datetime, timedelta, timezone

import msgpack
from confluent_kafka import Consumer, KafkaException, TopicPartition, \
    OFFSET_BEGINNING, OFFSET_END, TIMESTAMP_CREATE_TIME

from network_dependency.utils.helper_functions import parse_timestamp_argument

BIN_SIZE = timedelta(hours=1)
IN_TOPIC = 'traceroutev4_as_pairs'
BOOTSTRAP_SERVERS = 'kafka1:9092,kafka2:9092,kafka3:9092'

TS_FMT = '%Y-%m-%dT%H:%M'
Record = namedtuple('Record', 'prb_id peer dst')
Result = namedtuple('Result', 'prb_id peer dst bin_times bin_values')


def process_msg(msg, mode: str, msm_ids: set) -> Record:
    value = msgpack.unpackb(msg.value(), raw=False)
    if value['msm_id'] not in msm_ids:
        return Record(None, None, None)
    prb_id = value['prb_id']
    peer = value['peer_asn']
    dst = None
    if mode == 'as':
        dst = value['dst_asn']
        if dst == 0:
            dst = None
    elif mode == 'pfx':
        dst = value['dst_pfx']
    return Record(prb_id, peer, dst)


def compute_bins(mode: str, msm_ids: set, start_ts: int, end_ts: int) -> Result:
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'compute_bins',
        'auto.offset.reset': 'earliest',
        'max.poll.interval.ms': 1800 * 1000
    })
    if start_ts != OFFSET_BEGINNING:
        partition = consumer.offsets_for_times(
            [TopicPartition(IN_TOPIC, partition=0, offset=start_ts)])[0]
    else:
        partition = TopicPartition(IN_TOPIC, partition=0,
                                   offset=OFFSET_BEGINNING)
    msg_count = 0
    bin_times = list()
    bin_values = list()
    bin_start = None
    bin_end = None
    curr_bin_values = dict()
    unique_prb_id = set()
    unique_peer_asn = set()
    unique_dst = set()  # Depends on the mode
    try:
        consumer.assign([partition])
        _, high_watermark = consumer.get_watermark_offsets(partition)
        logging.info('High watermark: {}'.format(high_watermark))
        while True:
            msg = consumer.poll(1)
            if msg is None:
                logging.warning('Timeout')
                continue
            if msg.error():
                raise KafkaException(msg.error())
            ts_type, ts_value = msg.timestamp()
            if ts_type != TIMESTAMP_CREATE_TIME:
                logging.error('Message timestamp is kaputt. Stoppping.')
                break
            if end_ts != OFFSET_END and ts_value >= end_ts:
                break
            ts = datetime.fromtimestamp(ts_value // 1000, tz=timezone.utc)
            if bin_start is None:
                bin_start = ts
                bin_end = bin_start + BIN_SIZE
            if ts >= bin_end:
                bin_times.append(bin_start)
                bin_values.append(curr_bin_values.copy())
                curr_bin_values.clear()
                bin_start = bin_end
                bin_end = bin_start + BIN_SIZE
            val = process_msg(msg, mode, msm_ids)
            if val.dst:
                if val.dst not in curr_bin_values:
                    curr_bin_values[val.dst] = dict()
                if val.peer not in curr_bin_values[val.dst]:
                    curr_bin_values[val.dst][val.peer] = 0
                curr_bin_values[val.dst][val.peer] += 1
                if val.prb_id not in unique_prb_id:
                    unique_prb_id.add(val.prb_id)
                if val.peer not in unique_peer_asn:
                    unique_peer_asn.add(val.peer)
                if val.dst not in unique_dst:
                    unique_dst.add(val.dst)
            msg_count += 1
            if msg.offset() + 1 >= high_watermark:
                logging.info('Reached high watermark.')
                break
            if msg_count % 1000000 == 0:
                logging.info('At offset {}, {} messages left'
                             .format(msg.offset(),
                                     high_watermark - msg.offset()))
    finally:
        consumer.close()
    return Result(unique_prb_id, unique_peer_asn, unique_dst, bin_times, bin_values)


def write_output(mode: str, res: Result, output_dir: str) -> None:
    range_start = res.bin_times[0].strftime(TS_FMT)
    range_end = (res.bin_times[-1] + BIN_SIZE).strftime(TS_FMT)
    out_file = output_dir + 'bins.' + range_start + '--' + range_end \
               + '.pickle.bz2'
    out_data = {'mode': mode, 'probes': len(res.prb_id),
                'peer_as': len(res.peer), 'unique_dst': len(res.dst),
                'bin_size': BIN_SIZE,
                'data': list(zip(res.bin_times, res.bin_values))}
    with bz2.open(out_file, 'wb') as f:
        pickle.dump(out_data, f, pickle.HIGHEST_PROTOCOL)


def main() -> None:
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['as', 'pfx'],
                        help='Scan mode. AS number or prefix.')
    parser.add_argument('msm_ids', help='Comma separated list of measurement '
                                        'ids that should be included')
    parser.add_argument('output_dir', help='Output directory in which '
                                           'compressed bins are stored')
    parser.add_argument('-st', '--start', help='Start timestamp')
    parser.add_argument('-e', '--end', help='End timestamp')
    args = parser.parse_args()

    msm_ids = set()
    for msm_id in args.msm_ids.split(','):
        if not msm_id.isdigit():
            logging.error('Invalid measurement id specified: {}'
                          .format(args.msm_ids))
            exit(1)
        msm_ids.add(int(msm_id))
    output_dir = args.output_dir
    if not output_dir.endswith('/'):
        output_dir += '/'
    start_ts = OFFSET_BEGINNING
    if args.start:
        start_ts = parse_timestamp_argument(args.start) * 1000
        if start_ts == 0:
            logging.error('Invalid start time specified.')
            exit(1)
        logging.info('Start reading topic at {}.'
                     .format(datetime
                             .fromtimestamp(start_ts / 1000, tz=timezone.utc)
                             .strftime(TS_FMT)))
    end_ts = OFFSET_END
    if args.end:
        end_ts = parse_timestamp_argument(args.end) * 1000
        if end_ts == 0:
            logging.error('Invalid end time specified.')
            exit(1)
        logging.info('Stop reading topic at {}.'
                     .format(datetime
                             .fromtimestamp(end_ts / 1000, tz=timezone.utc)
                             .strftime(TS_FMT)))
    os.makedirs(output_dir, exist_ok=True)

    res = compute_bins(args.mode, msm_ids, start_ts, end_ts)
    write_output(args.mode, res, output_dir)


if __name__ == '__main__':
    main()
