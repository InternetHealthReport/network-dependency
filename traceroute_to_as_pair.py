import argparse
import configparser
import logging
import sys

import confluent_kafka
import msgpack
from confluent_kafka import Consumer, Producer, TopicPartition, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from network_dependency.utils import atlas_api_helper
from network_dependency.utils.ip_lookup import IPLookup

BOOTSTRAP_SERVERS = 'kafka1:9092,kafka2:9092,kafka3:9092'
IN_TOPIC = 'ihr_atlas_traceroutev4'
OUT_TOPIC = 'traceroutev4_as_pairs'


def prepare_topic() -> None:
    admin_client = AdminClient({'bootstrap.servers':
                                    BOOTSTRAP_SERVERS})
    topic_list = [NewTopic(OUT_TOPIC, num_partitions=2,
                           replication_factor=2)]
    created_topic = admin_client.create_topics(topic_list)
    for topic, f in created_topic.items():
        try:
            f.result()  # The result itself is None
            logging.info("Topic {} created".format(topic))
        except Exception as e:
            logging.warning("Failed to create topic {}: {}"
                            .format(topic, e))


def delivery_report(err, msg):
    if err is not None:
        logging.error('message delivery failed: {}'.format(err))
    else:
        pass


def write(key, data, timestamp: int) -> None:
    try:
        producer.produce(
            OUT_TOPIC,
            msgpack.packb(data, use_bin_type=True),
            key,
            callback=delivery_report,
            timestamp=timestamp
            )
        producer.poll(0)

    except BufferError:
        logging.warning('buffer error, the queue must be full! Flushing...')
        producer.flush()

        logging.info('queue flushed, try re-write previous message')
        producer.produce(
            OUT_TOPIC,
            msgpack.packb(data, use_bin_type=True),
            key,
            callback=delivery_report,
            timestamp=timestamp
            )
        producer.poll(0)


def process_msg(msg):
    value = msgpack.unpackb(msg.value(), raw=False)
    if value['msm_id'] != 5051 and value['msm_id'] != 5151:
        return
    failed = False
    dst_addr = atlas_api_helper.get_dst_addr(value)
    if not dst_addr:
        failed = True
        dst_asn = 0
        prefix = None
    else:
        dst_asn = lookup.ip2asn(dst_addr)
        prefix = lookup.ip2prefix(dst_addr)
    if 'from' not in value or not value['from']:
        peer_asn = 0
    else:
        peer_asn = lookup.ip2asn(value['from'])
    for hop in value['result']:
        if 'error' in hop:
            failed = True
            break
    out = {'msm_id': value['msm_id'],
           'prb_id': value['prb_id'],
           'timestamp': value['timestamp'],
           'failed': failed,
           'peer_asn': peer_asn,
           'dst_asn': dst_asn,
           'dst_pfx': prefix}
    write(value['msm_id'].to_bytes(length=4, byteorder='big'), out, value['timestamp'] * 1000)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('-st', '--start', help='Start reading at this '
                                               'timestamp')

    # Logging
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    logging.info("Started: %s" % sys.argv)

    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)

    lookup = IPLookup(config)

    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'fast_reader',
        'auto.offset.reset': 'earliest',
        'max.poll.interval.ms': 1800 * 1000
    })
    producer = Producer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'default.topic.config': {
            'compression.codec': 'snappy'
        }
    })
    prepare_topic()
    if args.start:
        partition = TopicPartition(IN_TOPIC, partition=2, offset=int(args.start) * 1000)
        partition = consumer.offsets_for_times([partition])[0]
    else:
        partition = TopicPartition(IN_TOPIC, partition=2, offset=confluent_kafka.OFFSET_BEGINNING)
    msg_count = 0
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
        producer.flush()
        consumer.close()
