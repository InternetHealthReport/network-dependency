import logging
from collections import namedtuple

from confluent_kafka.admin import AdminClient, KafkaException, NewTopic

TOPIC_CONFIG = {'retention.ms': 2592000000}
ListPair = namedtuple('ListPair', 'success fail')


def verify_topic_configs(topics: list, admin_client: AdminClient) -> ListPair:
    success = list()
    fail = list()
    result = admin_client.create_topics(topics, validate_only=True)
    for topic in result:
        try:
            result[topic].result()
        except KafkaException as e:
            logging.error(f'Topic validation failed for topic {topic}: {e}')
            fail.append(topic)
            continue
        success.append(topic)
    return ListPair(success, fail)


def create_topics(topics: list, admin_client: AdminClient) -> ListPair:
    success = list()
    fail = list()
    result = admin_client.create_topics(topics)
    for topic in result:
        try:
            result[topic].result()
        except KafkaException as e:
            logging.error(f'Topic creation failed for topic {topic}: {e}')
            fail.append(topic)
            continue
        success.append(topic)
    return ListPair(success, fail)


def delete_topics(topics: list, admin_client: AdminClient) -> ListPair:
    success = list()
    fail = list()
    result = admin_client.delete_topics(topics)
    for topic in result:
        try:
            result[topic].result()
        except KafkaException as e:
            logging.error(f'Topic deletion failed for topic {topic}: {e}')
            fail.append(topic)
            continue
        success.append(topic)
    return ListPair(success, fail)


def generate_topics(collector_prefix: str, count: int, bootstrap_server: str):
    admin_client = AdminClient({'bootstrap.servers': bootstrap_server})
    collectors = list()
    prepared_topics = list()
    for i in range(count):
        if count == 1:
            collector = collector_prefix
        else:
            collector = collector_prefix + '_' + str(i)
        collectors.append(collector)
        topic_prefix = 'ihr_bgp_' + collector
        rib_topic_name = topic_prefix + '_ribs'
        updates_topic_name = topic_prefix + '_updates'
        rib_topic = NewTopic(rib_topic_name, num_partitions=1,
                             replication_factor=1, config=TOPIC_CONFIG)
        updates_topic = NewTopic(updates_topic_name, num_partitions=1,
                                 replication_factor=1, config=TOPIC_CONFIG)
        prepared_topics.append(rib_topic)
        prepared_topics.append(updates_topic)
    verify_topic_result = verify_topic_configs(prepared_topics, admin_client)
    if verify_topic_result.fail:
        logging.error(f'Error during topic validation.')
        return list()
    create_topic_result = create_topics(prepared_topics, admin_client)
    if create_topic_result.fail:
        logging.error(f'Error during topic creation. Rolling back.')
        delete_topic_result = delete_topics(create_topic_result.success,
                                            admin_client)
        if delete_topic_result.fail:
            logging.critical(f'Rollback failed! Check for topic leftovers.')
            return list()
        logging.error(f'Rollback succeeded.')
        return list()
    return collectors
