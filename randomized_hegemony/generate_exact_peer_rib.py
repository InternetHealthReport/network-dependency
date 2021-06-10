import argparse
import configparser
import logging
import sys

from utils.topic_creation import generate_topics
from utils.topic_filler import PopulationMode, TopicFiller
from utils.topic_reader import ReadMode, TopicReader

sys.path.insert(0, '../')
from network_dependency.utils.helper_functions import parse_timestamp_argument


def verify_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser(
        converters={'csv': lambda entry: entry.split(',')})
    if not config.read(config_path):
        logging.error('Failed to read configuration file.')
        return config
    try:
        config.get('input', 'collector')
        config.getcsv('input', 'scopes')
    except ValueError as e:
        logging.error(f'Invalid configuration value specified: {e}')
        return configparser.ConfigParser()
    except configparser.NoOptionError as e:
        logging.error(f'Missing configuration value: {e}')
        return configparser.ConfigParser()
    return config


def main() -> None:
    log_fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('timestamp')
    parser.add_argument('peers', type=int)
    parser.add_argument('-m', '--max')
    population_mode_group = parser.add_mutually_exclusive_group(required=True)
    population_mode_group.add_argument('-a', '--asn', action='store_true')
    population_mode_group.add_argument('-p', '--peer', action='store_true')
    parser.add_argument('-s', '--server', default='localhost:9092')
    parser.add_argument('-o', '--output', default='./')
    args = parser.parse_args()

    logging.info(f'Started: {sys.argv}')

    requested_peers = args.peers

    output_dir = args.output
    if not output_dir.endswith('/'):
        output_dir += '/'

    config = verify_config(args.config)
    if not config.sections():
        sys.exit(1)

    collector = config.get('input', 'collector')

    timestamp = parse_timestamp_argument(args.timestamp)
    if timestamp == 0:
        logging.error(f'Invalid timestamp specified: {args.timestamp}')
        sys.exit(1)
    max_timestamp = None
    if args.max:
        max_timestamp = parse_timestamp_argument(args.max)
        if max_timestamp == 0:
            logging.error(f'Invalid timestamp specified: {args.max}')
            sys.exit(1)

    if args.asn:
        logging.info('Mode ASN')
        population_mode = PopulationMode.ASN
        read_mode = ReadMode.EXACT_AS
    else:
        population_mode = PopulationMode.PEER
        read_mode = ReadMode.EXACT_PEER

    rib_topic = f'ihr_bgp_{collector}_ribs'
    if max_timestamp:
        reader = TopicReader(rib_topic,
                             timestamp * 1000,
                             set(config.getcsv('input', 'scopes')),
                             args.server,
                             read_mode,
                             peers=requested_peers,
                             end_ts=max_timestamp * 1000)
    else:
        reader = TopicReader(rib_topic,
                             timestamp * 1000,
                             set(config.getcsv('input', 'scopes')),
                             args.server,
                             read_mode,
                             peers=requested_peers)
    reader.read()

    unreached_topics = False
    for scope in reader.scopes:
        if read_mode == ReadMode.EXACT_AS:
            num_peers = len(reader.scope_asn_messages[scope])
        else:
            num_peers = len(reader.scope_peer_messages[scope])
        if num_peers < requested_peers:
            logging.warning(f'Requested peer number not reached for scope '
                            f'{scope}: {num_peers}')
            unreached_topics = True
    if unreached_topics:
        sys.exit(1)

    output_collector = f'{collector}_{requested_peers}'
    output_topics = generate_topics(output_collector, 1, args.server)
    if not output_topics:
        sys.exit(1)

    filler = TopicFiller(output_topics,
                         reader,
                         timestamp * 1000,
                         args.server,
                         force_timestamp=True)
    filler.fill_topics_direct(population_mode)

    output_config = configparser.ConfigParser()
    output_config.add_section('input')
    output_config.set('input', 'collector', output_collector)
    output_config.set('input', 'scopes', config.get('input', 'scopes'))
    output_config_path = f'{output_dir}{collector}_{requested_peers}.ini'
    logging.info(f'Writing output config to {output_config_path}')
    with open(output_config_path, 'w') as f:
        output_config.write(f)


if __name__ == '__main__':
    main()
    sys.exit(0)
