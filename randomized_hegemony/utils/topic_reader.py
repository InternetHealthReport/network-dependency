import logging
from collections import defaultdict, namedtuple

import msgpack
from confluent_kafka import Consumer, Message, TopicPartition, \
    TIMESTAMP_CREATE_TIME

MessageStruct = namedtuple('MessageStruct', 'peer_address peer_asn msg')


class TopicReader:
    PARTITION_EOF = -191
    TIMEOUT_IN_S = 10

    def __init__(self,
                 topic: str,
                 timestamp: int,
                 scopes: set,
                 bootstrap_server: str):
        self.topic = topic
        self.timestamp = timestamp
        self.scopes = scopes
        self.bootstrap_server = bootstrap_server
        self.total_partitions = 0
        self.raw_messages = list()
        self.scope_peer_messages = defaultdict(lambda: defaultdict(list))
        self.scope_asn_messages = defaultdict(lambda: defaultdict(list))

    def __on_assign(self, consumer: Consumer, partitions: list) -> None:
        self.total_partitions = len(partitions)
        logging.info(f'Reading from {self.total_partitions} partitions '
                     f'{self.timestamp}')
        for p in partitions:
            p.offset = self.timestamp
        offsets = consumer.offsets_for_times(partitions)
        consumer.assign(offsets)

    @staticmethod
    def __check_key(key, dictionary: dict) -> bool:
        if key not in dictionary or not dictionary[key]:
            logging.error(f'"{key}" key missing in message')
            return True
        return False

    @staticmethod
    def __get_dst_asn(path: str) -> tuple:
        split_path = path.split(' ')
        if not split_path:
            return tuple()
        dst_asn = split_path[-1]
        if dst_asn.startswith('{'):
            return tuple(dst_asn.strip('{}').split(','))
        return (dst_asn,)

    def __process_msg(self, msg: Message) -> None:
        value = msgpack.unpackb(msg.value(), raw=False)

        if self.__check_key('elements', value):
            return
        if len(value['elements']) != 1:
            logging.error(f'Unexpected element count in message. Expected 1, '
                          f'got {len(value["elements"])}')
            return
        element = value['elements'][0]
        if self.__check_key('peer_address', element) or \
                self.__check_key('peer_asn', element) or \
                self.__check_key('fields', element) or \
                self.__check_key('as-path', element['fields']):
            return

        # Get destination ASN and also handle possible AS sets.
        dst_asn = self.__get_dst_asn(element['fields']['as-path'])
        found = False
        scope = -1
        for asn in dst_asn:
            if asn in self.scopes:
                found = True
                scope = asn
                break
        if not found:
            return

        msg_idx = len(self.raw_messages)
        self.scope_peer_messages[scope][element['peer_address']].append(msg_idx)
        self.scope_asn_messages[scope][element['peer_asn']].append(msg_idx)
        self.raw_messages.append(MessageStruct(element['peer_address'],
                                               element['peer_asn'], msg))

    def read(self) -> None:
        logging.info(f'Start reading topic {self.topic}')
        consumer = Consumer({'bootstrap.servers': self.bootstrap_server,
                             'group.id': self.topic + '-randomize-reader',
                             'enable.partition.eof': True
                             })
        paused_partitions = 0
        try:
            consumer.subscribe([self.topic], on_assign=self.__on_assign)

            while True:
                msg = consumer.poll(self.TIMEOUT_IN_S)
                if msg is None:
                    logging.warning('Timeout')
                    continue
                if msg.error():
                    if msg.error().code() == self.PARTITION_EOF:
                        logging.info(f'Partition {msg.partition()} reached EOF')
                        consumer.pause([TopicPartition(msg.topic(),
                                                       msg.partition())])
                        paused_partitions += 1
                        if paused_partitions >= self.total_partitions:
                            break
                        else:
                            continue
                    logging.error(f'Read error: {msg.error()}')
                    break
                ts = msg.timestamp()
                if ts[0] != TIMESTAMP_CREATE_TIME:
                    logging.warning(f'Message has unexpected timestamp type: '
                                    f'{ts[0]}')
                    continue
                if ts[1] != self.timestamp:
                    continue
                self.__process_msg(msg)
        finally:
            consumer.close()
