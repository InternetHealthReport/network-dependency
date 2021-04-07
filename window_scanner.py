import argparse
import logging
import os
from collections import namedtuple
from datetime import datetime, timedelta, timezone

import msgpack
from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING, \
    OFFSET_END, KafkaException, TIMESTAMP_CREATE_TIME

from network_dependency.utils.helper_functions import parse_timestamp_argument

MAX_ASN = 69551
MAX_PFX = 752954
BIN_SIZE = timedelta(hours=1)
IN_TOPIC = 'traceroutev4_as_pairs'
BOOTSTRAP_SERVERS = 'kafka1:9092,kafka2:9092,kafka3:9092'

TS_FMT = '%Y-%m-%dT%H:%M'
Window = namedtuple('Window', 'size length start end')


def process_msg(msg, mode: str, msm_ids: set):
    value = msgpack.unpackb(msg.value(), raw=False)
    if value['msm_id'] not in msm_ids:
        return None
    ret_val = None
    if mode == 'as':
        ret_val = value['dst_asn']
        if ret_val == 0:
            ret_val = None
    elif mode == 'pfx':
        ret_val = value['dst_pfx']
    return ret_val


def compute_bins(mode: str,
                 msm_ids: set,
                 start_ts: int,
                 end_ts: int) -> (list, list):
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'window_scanner',
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
    curr_bin_values = set()
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
                bin_values.append(set(curr_bin_values))
                curr_bin_values.clear()
                bin_start = bin_end
                bin_end = bin_start + BIN_SIZE
            val = process_msg(msg, mode, msm_ids)
            if val and val not in curr_bin_values:
                curr_bin_values.add(val)
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
    return bin_times, bin_values


def calculate_window_size(bin_times: list,
                          bin_values: list,
                          mode: str,
                          target_p: int) -> list:
    ret = list()
    logging.info('Computing window sizes on range {} - {}'
                 .format(bin_times[0].strftime(TS_FMT),
                         (bin_times[-1] + BIN_SIZE).strftime(TS_FMT)))
    target_count = 0
    if mode == 'as':
        target_count = (MAX_ASN / 100) * target_p
        logging.info('Targeting {}% of ASes: {}'.format(target_p, target_count))
    elif mode == 'pfx':
        target_count = (MAX_PFX / 100) * target_p
        logging.info('Targeting {}% of prefixes: {}'.format(target_p,
                                                            target_count))
    for start_idx in range(len(bin_values)):
        window_set = set()
        curr_idx = start_idx
        while curr_idx < len(bin_values) and len(window_set) < target_count:
            window_set.update(bin_values[curr_idx])
            curr_idx += 1
        if len(window_set) < target_count:
            logging.info('Target size unreachable for windows starting at {}'
                         .format(bin_times[start_idx].strftime(TS_FMT)))
            break
        curr_idx -= 1
        window_length = bin_times[curr_idx] + BIN_SIZE - bin_times[start_idx]
        curr_window = Window(len(window_set), window_length,
                             bin_times[start_idx],
                             bin_times[curr_idx] + BIN_SIZE)
        logging.debug('Window {} size: {} length: {} range: {} - {}'
                      .format(start_idx, curr_window.size,
                              str(curr_window.length),
                              curr_window.start.strftime(TS_FMT),
                              curr_window.end.strftime(TS_FMT)))
        ret.append(curr_window)
    return ret


def write_output(output_dir: str, target_p: int, data: list) -> None:
    with open(output_dir + str(target_p) + '.dat', 'w') as f:
        f.write('size length(s) start end\n')
        for window in data:
            out_line = [window.size, int(window.length.total_seconds()),
                        int(window.start.timestamp()),
                        int(window.end.timestamp())]
            f.write(' '.join(map(str, out_line)) + '\n')


def main() -> None:
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['as', 'pfx'],
                        help='Scan mode. AS number or prefix.')
    parser.add_argument('msm_ids', help='Comma separated list of measurement '
                                        'ids that should be included.')
    parser.add_argument('target_p',
                        help='Comma separated list of window sizes in terms of '
                             'target percentage [1-100] of the total number of '
                             'ASes/prefixes that should be covered.')
    parser.add_argument('output_dir', help='Output directory in which window '
                                           'sizes and lengths are stored')
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
    target_ps = set()
    for target_p in args.target_p.split(','):
        if not target_p.isdigit():
            logging.error('Invalid target percentage specified: {}'
                          .format(args.target_p))
            exit(1)
        target_p_int = int(target_p)
        if target_p_int <= 0 or target_p_int > 100:
            logging.error('Target percentage must be within range [1-100].')
            exit(1)
        target_ps.add(target_p_int)
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

    bin_times, bin_values = compute_bins(args.mode, msm_ids, start_ts, end_ts)
    for target_p in target_ps:
        windows = calculate_window_size(bin_times, bin_values, args.mode,
                                        target_p)
        write_output(output_dir, target_p, windows)


if __name__ == '__main__':
    main()
