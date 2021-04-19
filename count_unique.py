import argparse
import logging
import os
from collections import namedtuple
from datetime import datetime, timezone

import msgpack
from confluent_kafka import Consumer, KafkaException, TopicPartition, \
    OFFSET_BEGINNING, OFFSET_END, TIMESTAMP_CREATE_TIME

from network_dependency.utils.helper_functions import parse_timestamp_argument

IN_TOPIC = 'traceroutev4_as_pairs'
BOOTSTRAP_SERVERS = 'kafka1:9092,kafka2:9092,kafka3:9092'
OUTPUT_SUFFIX = '.csv'
OUTPUT_DELIMITER = ','

TS_FMT = '%Y-%m-%dT%H:%M'
Record = namedtuple('Info', 'prb_id dst_asn dst_pfx')
Result = namedtuple('Result', 'start_ts end_ts prb_id asn pfx')


def process_msg(msg, msm_ids: set) -> Record:
    value = msgpack.unpackb(msg.value(), raw=False)
    if value['msm_id'] not in msm_ids:
        return Record(None, None, None)
    prb_id = value['prb_id']
    dst_asn = value['dst_asn']
    if dst_asn == 0:
        dst_asn = None
    dst_pfx = value['dst_pfx']
    return Record(prb_id, dst_asn, dst_pfx)


def compute_uniques(msm_ids: set, start_ts: int, end_ts: int) -> Result:
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
    first_ts = None
    last_ts = None
    unique_prb_id = set()
    unique_asn = set()
    unique_pfx = set()
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
            if not first_ts:
                first_ts = ts_value / 1000
            last_ts = ts_value / 1000
            val = process_msg(msg, msm_ids)
            if val.dst_asn:
                if val.prb_id not in unique_prb_id:
                    unique_prb_id.add(val.prb_id)
                if val.dst_asn not in unique_asn:
                    unique_asn.add(val.dst_asn)
                if val.dst_pfx not in unique_pfx:
                    unique_pfx.add(val.dst_pfx)
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
    return Result(first_ts, last_ts, unique_prb_id, unique_asn, unique_pfx)


def write_output(res: Result, output_dir: str) -> None:
    range_start = datetime.fromtimestamp(res.start_ts, tz=timezone.utc).strftime(TS_FMT)
    range_end = datetime.fromtimestamp(res.end_ts, tz=timezone.utc).strftime(TS_FMT)
    out_file = output_dir + 'uniques.' + range_start + '--' + range_end \
               + OUTPUT_SUFFIX
    with open(out_file, 'w') as f:
        f.write(OUTPUT_DELIMITER
                .join(['unique_prb_id', 'unique_asn', 'unique_pfx']) + '\n')
        f.write(OUTPUT_DELIMITER
                .join(map(str, [len(res.prb_id), len(res.asn), len(res.pfx)]))
                + '\n')


def main() -> None:
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    desc = """Count unique ASes / prefixes as well as probes. The result
    can be used to specify bounds for window computation etc."""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('msm_ids', help='Comma separated list of measurement '
                                        'ids that should be included')
    parser.add_argument('output_dir', help='Output directory in which '
                                           'the result is stored')
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

    res = compute_uniques(msm_ids, start_ts, end_ts)
    write_output(res, output_dir)


if __name__ == '__main__':
    main()
