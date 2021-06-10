import argparse
import bz2
import configparser
import logging
import pickle
import sys
from datetime import datetime, timezone

from utils.topic_creation import generate_topics
from utils.topic_filler import PopulationMode, SamplingMode, TopicFiller
from utils.topic_reader import ReadMode, TopicReader

sys.path.insert(0, '../')
from network_dependency.utils.helper_functions import parse_timestamp_argument

DATE_FMT = '%Y-%m-%dT%H:%M'


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
    parser.add_argument('iterations', type=int)
    sampling_mode_group = parser.add_mutually_exclusive_group(required=True)
    sampling_mode_group.add_argument('-sr', '--relative', type=int,
                                     help='sampling value as a percentage')
    sampling_mode_group.add_argument('-sa', '--absolute', type=int,
                                     help='sampling value as an absolute '
                                          'number')
    population_mode_group = parser.add_mutually_exclusive_group(required=True)
    population_mode_group.add_argument('-a', '--asn', action='store_true')
    population_mode_group.add_argument('-p', '--peer', action='store_true')
    parser.add_argument('-s', '--server', default='localhost:9092')
    parser.add_argument('-o', '--output', default='./')

    args = parser.parse_args()

    logging.info(f'Started: {sys.argv}')

    output_dir = args.output
    if not output_dir.endswith('/'):
        output_dir += '/'

    config = verify_config(args.config)
    if not config.sections():
        sys.exit(1)

    timestamp = parse_timestamp_argument(args.timestamp)
    if timestamp == 0:
        logging.error(f'Invalid timestamp specified: {args.timestamp}')
        sys.exit(1)

    if args.relative:
        sampling_mode = SamplingMode.RELATIVE
        sampling_mode_str = 'relative'
        sampling_value = args.relative
        if 1 > sampling_value > 100:
            logging.error('Relative sampling value needs to be in range '
                          '[1-100]')
            sys.exit(1)
    else:
        sampling_mode = SamplingMode.ABSOLUTE
        sampling_mode_str = 'absolute'
        sampling_value = args.absolute
        if sampling_value < 1:
            logging.error('Absolute sampling value needs to be at least 1')
            sys.exit(1)

    if args.asn:
        population_mode = PopulationMode.ASN
        population_mode_str = 'asn'
    else:
        population_mode = PopulationMode.PEER
        population_mode_str = 'peer'

    collector = config.get('input', 'collector')

    input_topic = 'ihr_bgp_' + collector + '_ribs'
    reader = TopicReader(input_topic,
                         timestamp * 1000,
                         set(config.getcsv('input', 'scopes')),
                         args.server,
                         ReadMode.EXACT_TS)
    reader.read()
    logging.info('Scope stats:')
    for scope in reader.scope_asn_messages:
        logging.info(f'{scope}: AS: {len(reader.scope_asn_messages[scope])} '
                     f'peers: {len(reader.scope_peer_messages[scope])}')

    output_topics = generate_topics(collector + '_' +
                                    str(sampling_value),
                                    args.iterations, args.server)
    if not output_topics:
        sys.exit(1)

    filler = TopicFiller(output_topics, reader, timestamp * 1000, args.server)
    filler.fill_topics_sampling(sampling_value, sampling_mode, population_mode)

    timestamp_str = datetime.fromtimestamp(timestamp, tz=timezone.utc) \
        .strftime(DATE_FMT)
    sample_stat_file = output_dir + '.'.join(['samples', sampling_mode_str,
                                              population_mode_str, collector,
                                              timestamp_str, 'pickle.bz2'])
    logging.info(f'Writing sample stats to {sample_stat_file}')
    with bz2.open(sample_stat_file, 'wb') as f:
        pickle.dump(filler.sampled_data, f, protocol=pickle.HIGHEST_PROTOCOL)

    collector_stat_file = output_dir + '.'.join([collector, 'collectors',
                                                 'csv'])
    logging.info(f'Writing intermediate collectors to {collector_stat_file}')
    with open(collector_stat_file, 'w') as f:
        f.write('\n'.join(output_topics) + '\n')


if __name__ == '__main__':
    main()
    sys.exit(0)
