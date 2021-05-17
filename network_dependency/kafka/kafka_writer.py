import logging
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
import msgpack


class KafkaWriter:
    def __init__(self, topic: str, bootstrap_servers: str):
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.timeout_in_s = 60

    def __enter__(self):
        self.producer = Producer({
            'bootstrap.servers': self.bootstrap_servers,
            'default.topic.config': {
                'compression.codec': 'snappy'
                }
            })
        self.__prepare_topic()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.producer.flush(self.timeout_in_s)

    @staticmethod
    def __delivery_report(err, msg):
        if err is not None:
            logging.error('message delivery failed: {}'.format(err))
        else:
            pass

    def __prepare_topic(self) -> None:
        """Try to create the specified topic on the Kafka servers.
        Output a warning if the topic already exists.
        """
        admin_client = AdminClient({'bootstrap.servers':
                                        self.bootstrap_servers})
        topic_list = [NewTopic(self.topic, num_partitions=2,
                               replication_factor=2,
                               # 1 month
                               config={'retention.ms': '2592000000'})]
        created_topic = admin_client.create_topics(topic_list)
        for topic, f in created_topic.items():
            try:
                f.result()  # The result itself is None
                logging.warning("Topic {} created".format(topic))
            except Exception as e:
                logging.warning("Failed to create topic {}: {}"
                                .format(topic, e))

    def write(self, key, data, timestamp: int) -> None:
        try:
            self.producer.produce(
                self.topic,
                msgpack.packb(data, use_bin_type=True),
                key,
                callback=self.__delivery_report,
                timestamp=timestamp
                )
            self.producer.poll(0)

        except BufferError:
            logging.warning('buffer error, the queue must be full! Flushing...')
            self.producer.flush()

            logging.info('queue flushed, try re-write previous message')
            self.producer.produce(
                self.topic,
                msgpack.packb(data, use_bin_type=True),
                key,
                callback=self.__delivery_report,
                timestamp=timestamp
                )
            self.producer.poll(0)
