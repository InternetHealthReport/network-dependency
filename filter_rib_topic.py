import argparse
import configparser
import logging
import sys
from datetime import datetime, timezone

from confluent_kafka import OFFSET_BEGINNING, OFFSET_END

from network_dependency.kafka.kafka_reader import KafkaReader
from network_dependency.kafka.kafka_writer import KafkaWriter
from network_dependency.utils.helper_functions import check_key, check_keys, \
    parse_timestamp_argument

DATE_FMT = '%Y-%m-%dT%H:%M'


def parse_kv_csv(option: str) -> dict:
    ret = dict()
    for pair in option.split(','):
        key, value = pair.split(':')
        ret[key.strip()] = value.strip()
    return ret


def parse_csv(option: str) -> list:
    return [entry.strip() for entry in option.split(',')]


def check_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser(converters={'kv_csv': parse_kv_csv,
                                                   'csv': parse_csv})
    config.read(config_path)
    try:
        config.get('input', 'collector')
        config.get('output', 'collector')
        config.get('kafka', 'bootstrap_servers')
        if config.get('filter', 'path_attributes', fallback=None):
            config.getkv_csv('filter', 'path_attributes')
        if config.get('filter', 'excluded_path_attributes', fallback=None):
            config.getcsv('filter', 'excluded_path_attributes')
    except configparser.NoSectionError as e:
        logging.error(f'Missing section in config file: {e}')
        return configparser.ConfigParser()
    except configparser.NoOptionError as e:
        logging.error(f'Missing option in config file: {e}')
        return configparser.ConfigParser()
    except ValueError as e:
        logging.error(f'Malformed option in config file: {e}')
        return configparser.ConfigParser()
    return config


def filter_msg(msg: dict,
               path_attributes: dict = None,
               excluded_path_attributes: set = None) -> (dict, int):
    if check_keys(['rec', 'elements'], msg) \
            or check_key('time', msg['rec']):
        logging.error(f'Missing "rec", "time", or "elements" field in message: '
                      f'{msg}')
        return dict(), -1
    ret = msg.copy()
    timestamp = msg['rec']['time']
    ret['elements'] = list()
    for element in msg['elements']:
        if check_key('fields', element) \
                or check_key('path-attributes', element['fields']):
            logging.error(f'Missing "fields" or "path-attributes" field in '
                          f'message: {msg}')
            continue
        msg_path_attributes = element['fields']['path-attributes']

        if excluded_path_attributes and \
                excluded_path_attributes.intersection(
                    msg_path_attributes.keys()):
            continue
        if path_attributes:
            filtered = False
            for attribute, value in path_attributes.items():
                if attribute not in msg_path_attributes:
                    filtered = True
                    break
                if str(msg_path_attributes[attribute]) != value:
                    filtered = True
                    break
            if filtered:
                continue
        ret['elements'].append(element)
    if not ret['elements']:
        return dict(), -1
    return ret, timestamp


def main() -> None:
    log_fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        level=logging.INFO,
        filename='filter_rib_topic.log',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    read_group_desc = """By using the --start, --end, and --timestamp options,
                      only a specific range or an exact timestamp of TOPIC can
                      be dumped. Either or both range options can be specified,
                      but are exclusive with --timestamp. Timestamps can be
                      specified as UNIX epoch in (milli)seconds or in the format
                      '%Y-%m-%dT%H:%M'."""
    read_group = parser.add_argument_group('Interval specification',
                                           description=read_group_desc)
    read_group.add_argument('-st', '--start',
                            help='start timestamp (default: read topic from '
                                 'beginning)')
    read_group.add_argument('-e', '--end',
                            help='end timestamp (default: read topic to the '
                                 'end)')
    read_group.add_argument('-ts', '--timestamp', help='exact timestamp')
    args = parser.parse_args()

    logging.info(f'Started {sys.argv}')

    config = check_config(args.config)
    if not config.sections():
        sys.exit(1)

    if (args.start or args.end) and args.timestamp:
        logging.error(f'Range and exact timestamp arguments are exclusive.')
        sys.exit(1)

    start_ts = OFFSET_BEGINNING
    end_ts = OFFSET_END
    if args.timestamp:
        start_ts = parse_timestamp_argument(args.timestamp) * 1000
        if start_ts == 0:
            logging.error(f'Invalid timestamp specified: {args.timestamp}')
            sys.exit(1)
        end_ts = start_ts + 1
    if args.start:
        start_ts = parse_timestamp_argument(args.start) * 1000
        if start_ts == 0:
            logging.error(f'Invalid start timestamp specified: {args.start}')
            sys.exit(1)
    if args.end:
        end_ts = parse_timestamp_argument(args.end) * 1000
        if end_ts == 0:
            logging.error(f'Invalid end timestamp specified: {args.end}')
            sys.exit(1)
    if start_ts != OFFSET_BEGINNING:
        start_ts_dt = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc)
        logging.info(f'Start reading at {start_ts_dt.strftime(DATE_FMT)}')
    else:
        logging.info(f'Start reading at beginning of topic')
    if end_ts != OFFSET_END:
        end_ts_dt = datetime.fromtimestamp(end_ts / 1000, tz=timezone.utc)
        logging.info(f'Stop reading at {end_ts_dt.strftime(DATE_FMT)}')
    else:
        logging.info(f'Stop reading at end of topic')

    path_attributes = config.getkv_csv('filter',
                                       'path_attributes',
                                       fallback=None)

    excluded_path_attributes = None
    if config.get('filter', 'excluded_path_attributes', fallback=None):
        excluded_path_attributes = set(config.getcsv('filter',
                                                     'excluded_path_attributes',
                                                     fallback=None))


    bootstrap_servers = config.get('kafka', 'bootstrap_servers')

    input_rib_topic = f'ihr_bgp_{config.get("input", "collector")}_ribs'
    output_rib_topic = f'ihr_bgp_{config.get("output", "collector")}_ribs'
    logging.info(f'Reading from RIB topic {input_rib_topic}')
    logging.info(f'Writing to RIB topic {output_rib_topic}')
    rib_reader = KafkaReader([input_rib_topic],
                             bootstrap_servers,
                             start_ts,
                             end_ts)
    rib_writer = KafkaWriter(output_rib_topic, bootstrap_servers,
                             num_partitions=10,
                             # 2 months
                             config={'retention.ms': 5184000000})
    with rib_reader, rib_writer:
        last_ts = -1
        for msg in rib_reader.read():
            filtered_msg, timestamp = filter_msg(msg,
                                                 path_attributes,
                                                 excluded_path_attributes)
            if filtered_msg:
                if last_ts > timestamp:
                    logging.warning(f'Writing out-of-order message: {last_ts} '
                                    f'> {timestamp}')
                rib_writer.write(None, filtered_msg, timestamp * 1000)
                last_ts = timestamp

    input_updates_topic = f'ihr_bgp_{config.get("input", "collector")}_updates'
    output_updates_topic = \
        f'ihr_bgp_{config.get("output", "collector")}_updates'
    logging.info(f'Copying messages from updates topic {input_updates_topic} '
                 f'to {output_updates_topic}')
    if end_ts != OFFSET_END:
        # Updates topic message have an incremented timestamp.
        end_ts += 1000
    updates_reader = KafkaReader([input_updates_topic],
                                 bootstrap_servers,
                                 start_ts,
                                 end_ts)
    updates_writer = KafkaWriter(output_updates_topic, bootstrap_servers,
                                 num_partitions=10,
                                 # 2 months
                                 config={'retention.ms': 5184000000})
    with updates_reader, updates_writer:
        last_ts = -1
        for msg in updates_reader.read():
            if check_key('rec', msg) or check_key('time', msg['rec']):
                logging.error(f'Missing "rec" or "time" field in msg {msg}')
                continue
            timestamp = msg['rec']['time']
            if last_ts > timestamp:
                logging.warning(f'Writing out-of-order message: {last_ts} '
                                f'> {timestamp}')
            updates_writer.write(None, msg, timestamp * 1000)
            last_ts = timestamp


if __name__ == '__main__':
    main()
