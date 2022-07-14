import argparse
import configparser
import logging
import sys
from collections import defaultdict
from datetime import datetime

from network_dependency.kafka.kafka_reader import KafkaReader
from network_dependency.kafka.kafka_writer import KafkaWriter
from network_dependency.utils.helper_functions import parse_timestamp_argument
from optimum_selector.candidate_selector import calculate_scores
from shared_extract_functions import AS_HOPS_FEATURE, VALID_FEATURES


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


def read_probe_as(probe_as_file: str) -> set:
    ret = set()
    with open(probe_as_file, 'r') as f:
        f.readline()
        for line in f:
            asn = line.split(DATA_DELIMITER, maxsplit=1)[0]
            ret.add(asn)
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
    parser.add_argument('--output-timestamp',
                        help='use this timestamp instead of stop timestamp '
                             'for output messages')
    args = parser.parse_args()

    log_fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        level=logging.INFO,
        filename='compute_candidates.log',
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

    output_ts_arg = args.output_timestamp
    output_ts = end_ts
    if output_ts_arg:
        output_ts = parse_timestamp_argument(output_ts_arg)
        if output_ts == 0:
            logging.error(f'Invalid output timestamp specified: {output_ts_arg}')
            sys.exit(1)
    logging.info(f'Output timestamp: {output_ts} '
        f'{datetime.utcfromtimestamp(output_ts).strftime("%Y-%m-%dT:%H:%M")}')

    probe_ases = read_probe_as(args.probe_as_file)

    enabled_features = config.getcsv('options', 'enabled_features')
    feature_values = {feature: defaultdict(lambda: defaultdict(int))
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
            # Only include values coming from probe ASes and going to
            # non-probe ASes.
            if msg['peer'] not in probe_ases or msg['dst'] in probe_ases:
                continue
            peer = msg['peer']
            dst = msg['dst']
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
                if matrix[dst][peer] == 0 \
                  or matrix[dst][peer] > msg_feature_value:
                    matrix[dst][peer] = msg_feature_value

    # We require that a destination was reached from at least half of
    # the probe ASes.
    min_samples = len(probe_ases) // 2

    output_topic_prefix = config.get('output', 'kafka_topic_prefix')
    # Compute the ranking for each feature and write the result
    # to Kafka.
    for feature, matrix in feature_values.items():
        logging.info(f'Processing feature {feature}')

        scores = calculate_scores(matrix, min_samples)

        output_topic = output_topic_prefix + feature
        if feature == AS_HOPS_FEATURE:
            output_topic = output_topic_prefix + 'as_path_length'
        writer = KafkaWriter(output_topic,
                            bootstrap_servers,
                            num_partitions=10,
                            # 6 months
                            config={'retention.ms': 15552000000})
        logging.info(f'Writing {len(scores)} ranks to topic '
                     f'{output_topic}')
        rank = 1
        with writer:
            for asn, score in scores:
                samples = [matrix[asn][peer] for peer in matrix[asn]]
                num_samples = len(samples)
                mean = sum(samples) / num_samples
                logging.info(f'{rank} {asn} {mean} {num_samples}')
                try:
                    asn = int(asn)
                except ValueError as e:
                    logging.error(f'Skipping AS {asn}: {e}')
                    continue
                msg = {'timestamp': output_ts,
                       'rank': rank,
                       'asn': asn,
                       'mean': mean,
                       'num_samples': num_samples}
                writer.write(None,
                            msg,
                            output_ts * 1000)
                rank += 1



if __name__ == '__main__':
    main()
    sys.exit(0)
