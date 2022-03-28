import argparse
import configparser
import logging
import sys
from datetime import datetime

import numpy as np
import numpy.ma as ma

from network_dependency.kafka.kafka_reader import KafkaReader
from network_dependency.kafka.kafka_writer import KafkaWriter
from network_dependency.utils.helper_functions import parse_timestamp_argument
from shared_extract_functions import  VALID_FEATURES, AS_HOPS_FEATURE
from optimum_selector.selector import Selector
from optimum_selector.score_functions import get_score_function, reset_state


DATA_DELIMITER = ','


def parse_csv(value: str) -> list:
    return [entry.strip() for entry in value.split(',')]


def check_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser(converters={'csv': parse_csv})
    config.read(config_path)
    try:
        config.get('input', 'kafka_topic')
        config.get('output', 'kafka_topic_prefix')
        enabled_features = config.getcsv('options', 'enabled_features')
        config.get('kafka', 'bootstrap_servers')
    except configparser.NoSectionError as e:
        logging.error(f'Missing section in config file: {e}')
        return configparser.ConfigParser()
    except configparser.NoOptionError as e:
        logging.error(f'Missing option in config file: {e}')
        return configparser.ConfigParser()
    for feature in enabled_features:
        if feature not in VALID_FEATURES:
            logging.error(f'Invalid feature specified: {feature}')
            return configparser.ConfigParser()
    return config


def read_probe_as(probe_as_file: str) -> list:
    ret = list()
    with open(probe_as_file, 'r') as f:
        f.readline()
        for line in f:
            asn = int(line.split(DATA_DELIMITER, maxsplit=1)[0])
            ret.append(asn)
    ret.sort()
    return ret


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('start',
                        help='Start timestamp (as UNIX epoch in seconds or '
                             'milliseconds, or in YYYY-MM-DDThh:mm format)')
    parser.add_argument('stop',
                        help='Stop timestamp (as UNIX epoch in seconds or '
                             'milliseconds, or in YYYY-MM-DDThh:mm format)')
    parser.add_argument('probe_as_file')
    args = parser.parse_args()

    log_fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        level=logging.INFO,
        filename='compute_as_rank.log',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logging.info(f'Started: {sys.argv}')

    config = check_config(args.config)
    if not config.sections():
        sys.exit(1)


    start_ts_arg = args.start
    start_ts = parse_timestamp_argument(start_ts_arg)
    if start_ts == 0:
        logging.error(f'Invalid start timestamp specified: {start_ts_arg}')
        sys.exit(1)
    logging.info(f'Starting read at timestamp: {start_ts} '
        f'{datetime.utcfromtimestamp(start_ts).strftime("%Y-%m-%dT%H:%M")}')

    end_ts_arg = args.stop
    end_ts = parse_timestamp_argument(end_ts_arg)
    if end_ts == 0:
        logging.error(f'Invalid end timestamp specified: {end_ts_arg}')
        sys.exit(1)
    logging.info(f'Ending read at timestamp: {end_ts} '
        f'{datetime.utcfromtimestamp(end_ts).strftime("%Y-%m-%dT%H:%M")}')

    probe_as_list = read_probe_as(args.probe_as_file)
    asn_idx = {asn: idx for idx, asn in enumerate(probe_as_list)}

    enabled_features = config.getcsv('options', 'enabled_features')
    num_as = len(probe_as_list)
    feature_values = {feature: np.zeros((num_as, num_as))
                      for feature in enabled_features}
    input_topic = config.get('input', 'kafka_topic')
    bootstrap_servers = config.get('kafka', 'bootstrap_servers')

    reader = KafkaReader([input_topic],
                         bootstrap_servers,
                         start_ts * 1000,
                         end_ts * 1000)
    # Read feature values for the specified time interval from Kafka.
    with reader:
        for msg in reader.read():
            # Only include values for probe-AS pairs.
            if msg['peer'] not in asn_idx or msg['dst'] not in asn_idx:
                continue
            p_idx = asn_idx[msg['peer']]
            d_idx = asn_idx[msg['dst']]
            for feature, matrix in feature_values.items():
                if feature not in msg['features']:
                    continue
                msg_feature_value = msg['features'][feature]
                if feature == AS_HOPS_FEATURE:
                    # Convert to AS-path length.
                    msg_feature_value += 1
                # If the value for this AS pair does not yet exist,
                # or the message value is smaller, use the message
                # value.
                # We create a symmetric matrix, so it suffices to
                # only look at one side of the diagonal.
                if matrix[p_idx, d_idx] == 0 \
                  or matrix[p_idx, d_idx] > msg_feature_value:
                    matrix[p_idx, d_idx] = msg_feature_value
                    matrix[d_idx, p_idx] = msg_feature_value

    output_topic_prefix = config.get('output', 'kafka_topic_prefix')
    score_function = get_score_function('max_weighted_dist')
    # Compute the ranking for each feature and write the result
    # to Kafka.
    for feature, matrix in feature_values.items():
        reset_state()
        logging.info(f'Processing feature {feature}')
        selector = Selector(probe_as_list,
                            matrix,
                            score=score_function,
                            summary=np.nanmean,
                            mask_value=0)
        selector.process()
        output_topic = output_topic_prefix + feature
        writer = KafkaWriter(output_topic,
                            bootstrap_servers,
                            num_partitions=10,
                            # 6 months
                            config={'retention.ms': 15552000000})
        logging.info(f'Writing {len(selector.steps)} ranks to topic '
                     f'{output_topic}')
        rank = 1
        with writer:
            for line in reversed(selector.steps):
                # Do not include the tail of the ranking, which only
                # contains ASes with no connection to each other (as
                # indicated by a masked summary value).
                if line[0] is ma.masked or line[1] == str():
                    continue
                logging.info(f'{rank} {line}')
                msg = {'timestamp': end_ts,
                       'rank': rank,
                       'asn': line[1],
                       'mean': line[0]}
                writer.write(None,
                            msg,
                            end_ts * 1000)
                rank += 1


if __name__ == '__main__':
    main()
    sys.exit(0)
