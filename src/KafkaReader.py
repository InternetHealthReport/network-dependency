import confluent_kafka
from confluent_kafka import Consumer, TopicPartition
import logging
import msgpack


class KafkaReader:
    """Generic reader to read from Kafka topics.

    Subscribe to a list of topics and read all messages within the
    specified timeframe. If no timeframe is specified, read the entire
    topic. In this case, the last read needs to go into a timeout in
    order to return.
    """

    def __init__(self,
                 topic: list,
                 bootstrap_servers: str,
                 start: int = confluent_kafka.OFFSET_BEGINNING,
                 end: int = confluent_kafka.OFFSET_END):
        self.topics = topic
        self.start = start
        self.end = end
        self.bootstrap_servers = bootstrap_servers
        self.partition_paused = 0
        self.partition_total = 0
        self.timeout_in_s = 10

    def __enter__(self):
        self.consumer = Consumer({
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.topics[0] + '_reader',
            'auto.offset.reset': 'earliest',
            'max.poll.interval.ms': 1800 * 1000,
            })
        self.consumer.subscribe(self.topics, on_assign=self.__on_assign)
        logging.debug('Created consumer and subscribed to topic(s) {}.'
                      .format(self.topics))

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.consumer.close()
        logging.debug('Closed consumer.')

    def __on_assign(self, consumer: Consumer, partitions: list):
        """Position the consumer to the offset corresponding to the
        given start timestamp.
        """
        # Initialize total number of assigned partitions
        self.partition_total = len(partitions)

        # Seek offset for given start timestamp
        for p in partitions:
            p.offset = self.start
        offsets = consumer.offsets_for_times(partitions)
        consumer.assign(offsets)

        logging.info("topic: {}, start: {}, end: {}, {} partitions"
                     .format(self.topics, self.start, self.end,
                             self.partition_total))

    def read(self):
        """Read a value from one of the topics and return the raw
        decoded value structure.
        """
        logging.debug('Start reading data')
        while True:
            msg = self.consumer.poll(self.timeout_in_s)
            if msg is None:
                logging.warning('Timeout! ({}s)'.format(self.timeout_in_s))
                break
            if msg.error():
                logging.error("Consumer error: {}".format(msg.error()))
                continue
            # Filter with start and end times
            # tuple of message timestamp type and timestamp
            ts = msg.timestamp()
            if ts[0] == confluent_kafka.TIMESTAMP_CREATE_TIME \
                    and ts[1] < self.start:
                continue

            if ts[0] == confluent_kafka.TIMESTAMP_CREATE_TIME \
                    and ts[1] >= self.end != confluent_kafka.OFFSET_END:
                # Stop reading from this partition since we have reached
                # the end of the record range in which we are
                # interested.
                self.consumer.pause([TopicPartition(msg.topic(),
                                                    msg.partition())])
                self.partition_paused += 1
                if self.partition_paused < self.partition_total:
                    continue
                else:
                    break

            value = msgpack.unpackb(msg.value(), raw=False)
            yield value
        logging.debug('Stopped reading.')
