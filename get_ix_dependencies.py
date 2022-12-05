import argparse
import configparser
import logging
import sys
from collections import defaultdict, namedtuple

from kafka_wrapper.kafka_reader import KafkaReader
from kafka_wrapper.kafka_writer import KafkaWriter
from network_dependency.utils.helper_functions import check_keys, \
    parse_timestamp_argument


IXData = namedtuple('IXData', 'name name_long country')


def check_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(config_path)
    try:
        config.get('input', 'traceroute_topic')
        config.get('input', 'peeringdb_ix_topic')
        config.get('output', 'kafka_topic')
        config.get('kafka', 'bootstrap_servers')
    except configparser.NoSectionError as e:
        logging.error(f'Missing section in config file: {e}')
        return configparser.ConfigParser()
    except configparser.NoOptionError as e:
        logging.error(f'Missing option in config file: {e}')
        return configparser.ConfigParser()
    return config


def read_ix_topic(reader: KafkaReader) -> dict:
    ret = dict()
    required_keys = ['ix_id', 'name', 'name_long', 'country']
    for msg in reader.read():
        if check_keys(required_keys, msg):
            logging.error(f'Missing one of {required_keys} in msg: {msg}')
            continue
        ix_id = msg['ix_id']
        ix_data = IXData(msg['name'], msg['name_long'], msg['country'])
        if ix_id in ret and ret[ix_id] != ix_data:
            logging.debug(f'Updating IX entry with different data.\n'
                          f'Old: {ret[ix_id]}\n'
                          f'New: {ix_data}')
        ret[ix_id] = ix_data
    return ret


def read_hegemony_topic(reader: KafkaReader) -> dict:
    ret = defaultdict(list)
    required_keys = ['hege', 'asn', 'scope', 'nb_peers']
    for msg in reader.read():
        if check_keys(required_keys, msg):
            logging.error(f'Missing one of {required_keys} in msg: {msg}')
            continue
        asn: str = msg['asn']
        scope: str = msg['scope']
        if not asn.startswith('ix|') or ';' in asn or scope == '-1':
            continue
        ix_id = int(asn.lstrip('ix|'))
        ret[ix_id].append((msg['hege'], msg['scope'], msg['nb_peers']))
    return ret


def main() -> None:
    log_fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        level=logging.INFO,
        filename='get_ix_dependencies.log',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('timestamp', help='Timestamp (as UNIX epoch in seconds '
                                          'or milliseconds, or in '
                                          'YYYY-MM-DDThh:mm format)')
    args = parser.parse_args()

    logging.info(f'Started {sys.argv}')

    config = check_config(args.config)
    if not config.sections():
        sys.exit(1)

    hegemony_topic = config.get('input', 'traceroute_topic')
    ix_topic = config.get('input', 'peeringdb_ix_topic')
    output_topic = config.get('output', 'kafka_topic')
    bootstrap_servers = config.get('kafka', 'bootstrap_servers')

    start_ts = parse_timestamp_argument(args.timestamp) * 1000
    if start_ts == 0:
        logging.error(f'Invalid timestamp specified: {args.timestamp}')
        sys.exit(1)
    end_ts = start_ts + 1

    ix_reader = KafkaReader([ix_topic], bootstrap_servers)
    with ix_reader:
        ix_data = read_ix_topic(ix_reader)

    hegemony_reader = KafkaReader([hegemony_topic],
                                  bootstrap_servers,
                                  start_ts,
                                  end_ts)
    with hegemony_reader:
        ix_dependencies = read_hegemony_topic(hegemony_reader)

    output_writer = KafkaWriter(output_topic,
                                bootstrap_servers,
                                # 2 months
                                config={'retention.ms': 5184000000})
    with output_writer:
        data = {'timestamp': start_ts // 1000}
        for ix_id in ix_dependencies:
            if ix_id not in ix_data:
                logging.error(f'No mapping for IX {ix_id}')
                continue
            data['ix_id'] = ix_id
            data.update(ix_data[ix_id]._asdict())
            data['hege'] = ix_dependencies[ix_id]
            output_writer.write(ix_id.to_bytes(4, 'big'), data, start_ts)


if __name__ == '__main__':
    main()
    sys.exit(0)
