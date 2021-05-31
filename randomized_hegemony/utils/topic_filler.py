import logging
import random
from enum import Enum

import msgpack
from confluent_kafka import Message, Producer
from utils.topic_reader import TopicReader, MessageStruct


class SamplingMode(Enum):
    ABSOLUTE = 1
    RELATIVE = 2


class PopulationMode(Enum):
    ASN = 1
    PEER = 2


class TopicFiller:
    TIMEOUT_IN_S = 10

    def __init__(self,
                 collectors: list,
                 reader: TopicReader,
                 timestamp: int,
                 bootstrap_server: str):
        self.collectors = collectors
        self.reader = reader
        self.timestamp = timestamp
        self.bootstrap_server = bootstrap_server
        self.sampled_data = dict()

    @staticmethod
    def __sample(data: dict, sampling_value: int, mode: SamplingMode) -> dict:
        ret = dict()
        for scope in data:
            population = data[scope]
            population_size = len(population)
            if mode == SamplingMode.RELATIVE:
                sample_size = int(population_size / 100 * sampling_value)
            else:
                sample_size = sampling_value
            if sample_size > population_size:
                continue
            sampled_keys = random.sample(population.keys(), sample_size)
            ret[scope] = {k: v for k, v in population.items()
                          if k in sampled_keys}
        return ret

    @staticmethod
    def __delivery_report(err, msg) -> None:
        if err is not None:
            logging.error(f'Message delivery failed: {err}')

    def __fill_topic(self, collector: str, data: dict, producer: Producer):
        rib_topic = 'ihr_bgp_' + collector + '_ribs'
        logging.info(f'Filling topic {rib_topic}')
        scope_dict = dict()
        for scope in data:
            entity_dict = {key: {'peer_asn': 0, 'traceroute_count': 0,
                                 'peer_addresses': dict()}
                           for key in data[scope].keys()}
            for entity in data[scope]:
                for msg_idx in data[scope][entity]:
                    msg_struct: MessageStruct = \
                        self.reader.raw_messages[msg_idx]
                    entity_dict[entity]['peer_asn'] = msg_struct.peer_asn
                    entity_dict[entity]['traceroute_count'] += 1
                    peer_address = msg_struct.peer_address
                    if peer_address not in \
                            entity_dict[entity]['peer_addresses']:
                        entity_dict[entity]['peer_addresses'][peer_address] = 0
                    entity_dict[entity]['peer_addresses'][peer_address] += 1
                    msg: Message = msg_struct.msg
                    try:
                        producer.produce(rib_topic,
                                         key=msg.key(),
                                         value=msg.value(),
                                         timestamp=msg.timestamp()[1],
                                         on_delivery=self.__delivery_report)
                    except BufferError:
                        logging.warning('Buffer error. Flushing queue...')
                        producer.flush(self.TIMEOUT_IN_S)
                        logging.warning('Rewriting previous message')
                        producer.produce(rib_topic,
                                         key=msg.key(),
                                         value=msg.value(),
                                         timestamp=msg.timestamp()[1],
                                         on_delivery=self.__delivery_report)
                    producer.poll(0)
            scope_dict[scope] = entity_dict
        self.sampled_data[collector] = scope_dict
        update_topic = 'ihr_bgp_' + collector + '_updates'
        logging.info(f'Adding fake entry to {update_topic}')
        fake_ts_in_s = int(self.timestamp / 1000) + 1
        fake = {'rec': {'time': fake_ts_in_s},
                'elements': [{
                    'type': 'A',
                    'time': fake_ts_in_s,
                    'peer_address': '0.0.0.0',
                    'peer_asn': 0,
                    'fields': {
                        'prefix': '0.0.0.0/0'
                    }
                }]
                }
        try:
            producer.produce(update_topic,
                             key=None,
                             value=msgpack.packb(fake, use_bin_type=True),
                             timestamp=fake_ts_in_s * 1000,
                             on_delivery=self.__delivery_report)
        except BufferError:
            logging.warning('Buffer error. Flushing queue...')
            producer.flush(self.TIMEOUT_IN_S)
            logging.warning('Rewriting previous message')
            producer.produce(update_topic,
                             key=None,
                             value=msgpack.packb(fake, use_bin_type=True),
                             timestamp=fake_ts_in_s * 1000,
                             on_delivery=self.__delivery_report)
        producer.flush(self.TIMEOUT_IN_S)

    def fill_topics(self,
                    sampling_value: int,
                    sampling_mode: SamplingMode,
                    population_mode: PopulationMode):
        if sampling_mode == SamplingMode.RELATIVE:
            logging.info(f'Creating {len(self.collectors)} samples with size '
                         f'{sampling_value}%')
        else:
            logging.info(f'Creating {len(self.collectors)} samples with size '
                         f'{sampling_value}')
        if population_mode == PopulationMode.ASN:
            logging.info('Mode: ASN')
            population_data = self.reader.scope_asn_messages
        else:
            logging.info('Mode: Peers')
            population_data = self.reader.scope_peer_messages
        sampled_data = list()
        for _ in range(len(self.collectors)):
            sampled_data.append(self.__sample(population_data,
                                              sampling_value,
                                              sampling_mode))
        producer = Producer({'bootstrap.servers': self.bootstrap_server,
                             'compression.codec': 'lz4',
                             'delivery.report.only.error': True,
                             'queue.buffering.max.messages': 10000000,
                             'queue.buffering.max.kbytes': 4194304,  # 4 GiB
                             'queue.buffering.max.ms': 1000,
                             'batch.num.messages': 1000000
                             })
        try:
            for idx, collector in enumerate(self.collectors):
                self.__fill_topic(collector, sampled_data[idx], producer)
        finally:
            producer.flush(self.TIMEOUT_IN_S)
