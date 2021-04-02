import logging
from collections import defaultdict
from datetime import datetime, timezone

import confluent_kafka
import matplotlib.pyplot as plt
import msgpack
import numpy as np
from confluent_kafka import Consumer, TopicPartition, KafkaException

BOOTSTRAP_SERVERS = 'kafka1:9092,kafka2:9092,kafka3:9092'
IN_TOPIC = 'traceroutev4_as_pairs'
MSM_IDS = {5051, 5151}  # 5051:UDP, 5151:ICMP


def process_msg(msg):
    value = msgpack.unpackb(msg.value(), raw=False)
    if value['msm_id'] not in MSM_IDS:
        return
    peer_asn = value['peer_asn']
    dst_asn = value['dst_asn']
    if dst_asn == 0:
        return
    if peer_asn not in as_map[dst_asn]:
        as_map[dst_asn].add(peer_asn)


def get_ratios():
    ret = list()
    for as_ in as_map:
        ret.append(len(as_map[as_]))
    return ret


def plot():
    x_tick_positions = np.linspace(bin_times[0], bin_times[-1], 10)
    x_tick_labels = [datetime.fromtimestamp(t, tz=timezone.utc)
                         .strftime('%Y-%m-%dT%H:%M') for t in x_tick_positions]
    fa = plt.subplots()
    ax: plt.Axes = fa[1]
    ax2: plt.Axes = ax.twinx()
    ax.set_ylabel('min / avg')
    ax2.set_ylabel('max')
    min_line, = ax.plot(bin_times, [np.min(v) for v in bin_values])
    avg_line, = ax.plot(bin_times, [np.average(v) for v in bin_values])
    q1_line, = ax.plot(bin_times, [np.quantile(v, 0.25) for v in bin_values],
                       c='blue', ls='--', alpha=0.8)
    q2_line, = ax.plot(bin_times, [np.quantile(v, 0.5) for v in bin_values],
                       c='blue', ls='--', alpha=0.8)
    q3_line, = ax.plot(bin_times, [np.quantile(v, 0.75) for v in bin_values],
                       c='blue', ls='--', alpha=0.8)
    max_line, = ax2.plot(bin_times, [np.max(v) for v in bin_values], c='red')
    ax.set_ylim(ymin=0)
    ax2.set_ylim(ymin=0)
    ax.set_xlim(xmin=bin_times[0])
    ax.set_xticks(x_tick_positions)
    ax.set_xticklabels(x_tick_labels, ha='right')
    ax.tick_params(axis='y', length=0)
    ax.tick_params(axis='x', labelrotation=45, direction='in')
    ax.grid()
    ax.legend((min_line, avg_line, max_line), ('min', 'avg', 'max'), ncol=3)
    plt.savefig('test.pdf', bbox_inches='tight')


if __name__ == '__main__':
    # Logging
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT,  # filename='topology_scanner.log',
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'fast_reader',
        'auto.offset.reset': 'earliest',
        'max.poll.interval.ms': 1800 * 1000
    })
    partition = TopicPartition(IN_TOPIC, partition=0,
                               offset=confluent_kafka.OFFSET_BEGINNING)
    msg_count = 0
    as_map = defaultdict(set)
    bin_times = list()
    bin_values = list()
    bin_start = None
    bin_end = None
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
            if ts_type != confluent_kafka.TIMESTAMP_CREATE_TIME:
                logging.error('Message timestamp is kaputt. Stoppping.')
                break
            ts = ts_value / 1000
            if bin_start is None:
                bin_start = ts
                bin_end = ts + (15 * 60)  # 15 minutes
            if ts >= bin_end:
                ratios = get_ratios()
                bin_times.append(bin_start)
                bin_values.append(ratios)
                bin_start = bin_end
                bin_end = bin_start + (15 * 60)
            process_msg(msg)
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
    plot()
