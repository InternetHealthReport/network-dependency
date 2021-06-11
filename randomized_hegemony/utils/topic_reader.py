import logging
from collections import defaultdict, namedtuple
from datetime import datetime, timezone
from enum import Enum

import msgpack
from confluent_kafka import Consumer, Message, TopicPartition, \
    TIMESTAMP_CREATE_TIME, OFFSET_END

MessageStruct = namedtuple('MessageStruct', 'peer_address peer_asn msg')


class ReadMode(Enum):
    EXACT_TS = 1
    EXACT_AS = 2
    EXACT_PEER = 3


class TopicReader:
    PARTITION_EOF = -191
    TIMEOUT_IN_S = 10

    def __init__(self,
                 topic: str,
                 start_ts: int,
                 scopes: set,
                 bootstrap_server: str,
                 read_mode: ReadMode,
                 peers: int = 0,
                 end_ts: int = OFFSET_END):
        self.topic = topic
        self.start_ts = start_ts
        self.end_ts = end_ts
        self.scopes = scopes
        self.bootstrap_server = bootstrap_server
        self.read_mode = read_mode
        self.min_peers = peers
        self.total_partitions = 0
        self.raw_messages = list()
        self.scope_peer_messages = defaultdict(lambda: defaultdict(list))
        self.scope_asn_messages = defaultdict(lambda: defaultdict(list))
        self.min_peers_reached = set()

    def __on_assign(self, consumer: Consumer, partitions: list) -> None:
        self.total_partitions = len(partitions)
        logging.info(f'Reading from {self.total_partitions} partitions '
                     f'{self.start_ts}')
        for p in partitions:
            p.offset = self.start_ts
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

        peer_address = element['peer_address']
        peer_asn = element['peer_asn']

        if self.read_mode == ReadMode.EXACT_AS \
                and len(self.scope_asn_messages[scope]) >= self.min_peers:
            return
        if self.read_mode == ReadMode.EXACT_PEER \
                and len(self.scope_peer_messages[scope]) >= self.min_peers:
            return

        self.scope_peer_messages[scope][peer_address].append(msg_idx)
        self.scope_asn_messages[scope][peer_asn].append(msg_idx)
        self.raw_messages.append(MessageStruct(peer_address,
                                               peer_asn, msg))
        if scope not in self.min_peers_reached \
                and len(self.scope_asn_messages[scope]) == self.min_peers:
            logging.info(f'Min peers for scope {scope} reached at '
                         f'{datetime.fromtimestamp(int(msg.timestamp()[1] / 1000), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")}')
            self.min_peers_reached.add(scope)

    def __check_min_peers(self) -> bool:
        if self.read_mode == ReadMode.EXACT_AS:
            for scope in self.scopes:
                if scope not in self.scope_asn_messages or \
                        len(self.scope_asn_messages[scope]) < self.min_peers:
                    return False
        elif self.read_mode == ReadMode.EXACT_PEER:
            for scope in self.scopes:
                if scope not in self.scope_peer_messages or \
                        len(self.scope_peer_messages[scope]) < self.min_peers:
                    return False
        else:
            logging.warning(f'check_min_peers called for invalid mode '
                            f'{self.read_mode}')
        return True

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
                if self.read_mode == ReadMode.EXACT_TS \
                        and ts[1] != self.start_ts:
                    continue
                if ts[1] < self.start_ts:
                    continue
                if self.end_ts != OFFSET_END and ts[1] >= self.end_ts:
                    continue
                self.__process_msg(msg)
                if (self.read_mode == ReadMode.EXACT_AS
                    or self.read_mode == ReadMode.EXACT_PEER) \
                        and self.__check_min_peers():
                    break
        finally:
            consumer.close()
