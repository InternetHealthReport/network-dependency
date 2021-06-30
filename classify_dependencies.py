import argparse
import configparser
import logging
import sys
from dataclasses import dataclass, field

from network_dependency.kafka.kafka_reader import KafkaReader
from network_dependency.kafka.kafka_writer import KafkaWriter
from network_dependency.utils.helper_functions import parse_timestamp_argument


@dataclass
class Scope:
    nb_peers: int
    dependencies: dict = field(default_factory=dict)
    dependency_ranks: dict = field(default_factory=dict)


def check_config(config_path: str) -> configparser.ConfigParser:
    config = configparser.ConfigParser()
    config.read(config_path)
    try:
        config.get('input', 'bgp_topic')
        config.get('input', 'traceroute_topic')
        config.get('output', 'kafka_topic')
        config.getint('options', 'min_peers')
        config.getfloat('options', 'min_hege')
        config.get('kafka', 'bootstrap_servers')
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


def check_key(key, dictionary: dict) -> bool:
    if key not in dictionary or not dictionary[key]:
        logging.error(f'"{key}" key missing in message')
        return True
    return False


def compute_ranks(scope: Scope) -> None:
    dependencies_by_rank = list(scope.dependencies.items())
    # Sort by descending score
    dependencies_by_rank.sort(key=lambda t: t[1], reverse=True)
    for rank, (asn, score) in enumerate(dependencies_by_rank):
        scope.dependency_ranks[asn] = rank


def read_scopes(reader: KafkaReader, min_peers: int, min_hege: float) -> dict:
    ret = dict()
    for msg in reader.read():
        if check_key('hege', msg) \
                or check_key('asn', msg) \
                or check_key('scope', msg) \
                or check_key('nb_peers', msg):
            continue
        if msg['nb_peers'] < min_peers or msg['hege'] < min_hege:
            continue
        scope = msg['scope']
        asn = msg['asn']
        hege = msg['hege']
        nb_peers = msg['nb_peers']
        if scope in ret:
            if ret[scope].nb_peers != nb_peers:
                logging.warning(f'Inconsistent number of peers for scope '
                                f'{scope}. Previous: {ret[scope].nb_peers} '
                                f'Now: {nb_peers}')
            if asn in ret[scope].dependencies:
                logging.error(f'Dependency {asn} already present for scope '
                              f'{scope}. Present: '
                              f'{ret[scope].dependencies[asn]} New: {hege}')
                continue
            ret[scope].dependencies[asn] = hege
        else:
            ret[scope] = Scope(nb_peers)
    for scope in ret:
        compute_ranks(ret[scope])
    return ret


def classify(bgp_scopes: dict,
             tr_scopes: dict,
             timestamp: int,
             writer: KafkaWriter) -> None:
    for scope in tr_scopes:
        if scope not in bgp_scopes:
            logging.warning(f'Scope {scope} not present in BGP data')
            continue
        tr_scope = tr_scopes[scope]
        bgp_scope = bgp_scopes[scope]
        tr_dependencies = set(tr_scope.dependencies.keys())
        bgp_dependencies = set(bgp_scope.dependencies.keys())
        bgp_only_dependencies = bgp_dependencies - tr_dependencies
        tr_only_dependencies = tr_dependencies - bgp_dependencies
        overlap_dependencies = tr_dependencies.intersection(bgp_dependencies)
        equal_dependencies = set()
        mismatched_dependencies = set()
        for asn in overlap_dependencies:
            if tr_scope.dependency_ranks[asn] \
                    == bgp_scope.dependency_ranks[asn]:
                equal_dependencies.add(asn)
            else:
                mismatched_dependencies.add(asn)
        bgp_only_scores = [(asn,
                            bgp_scope.dependencies[asn],
                            bgp_scope.dependency_ranks[asn])
                           for asn in bgp_only_dependencies]
        bgp_only_scores.sort(key=lambda t: t[1], reverse=True)
        tr_only_scores = [(asn,
                           tr_scope.dependencies[asn],
                           tr_scope.dependency_ranks[asn])
                          for asn in tr_only_dependencies]
        tr_only_scores.sort(key=lambda t: t[1], reverse=True)
        mismatched_scores = [(asn,
                              bgp_scope.dependencies[asn],
                              bgp_scope.dependency_ranks[asn],
                              tr_scope.dependencies[asn],
                              tr_scope.dependency_ranks[asn])
                             for asn in mismatched_dependencies]
        mismatched_scores.sort(key=lambda t: t[1], reverse=True)
        equal_scores = [(asn,
                         bgp_scope.dependencies[asn],
                         tr_scope.dependencies[asn],
                         tr_scope.dependency_ranks[asn])
                        for asn in equal_dependencies]
        equal_scores.sort(key=lambda t: t[1], reverse=True)

        msg = {'timestamp': timestamp // 1000,
               'scope': scope,
               'equal': equal_scores,
               'mismatched': mismatched_scores,
               'bgp_only': bgp_only_scores,
               'tr_only': tr_only_scores
               }
        writer.write(scope, msg, timestamp)

        logging.debug(f'scope: {scope}')
        logging.debug(f'  bgp: {len(bgp_only_dependencies)}')
        logging.debug(f'   tr: {len(tr_only_dependencies)}')
        logging.debug(f'   mm: {len(mismatched_dependencies)}')
        logging.debug(f'   eq: {len(equal_dependencies)}')
        logging.debug('')


def main() -> None:
    log_fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        level=logging.INFO,
        filename='classify_dependencies.log',
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

    bgp_topic = config.get('input', 'bgp_topic')
    traceroute_topic = config.get('input', 'traceroute_topic')
    min_peers = config.getint('options', 'min_peers')
    min_hege = config.getfloat('options', 'min_hege')
    logging.info(f'min_peers: {min_peers} min_hege: {min_hege}')

    start_ts = parse_timestamp_argument(args.timestamp) * 1000
    if start_ts == 0:
        logging.error(f'Invalid timestamp specified: {args.timestamp}')
        sys.exit(1)
    end_ts = start_ts + 1

    bgp_reader = KafkaReader([bgp_topic],
                             config.get('kafka', 'bootstrap_servers'),
                             start_ts,
                             end_ts)
    logging.info(f'Reading BGP scopes from {bgp_topic}')
    with bgp_reader:
        bgp_scopes = read_scopes(bgp_reader, min_peers, min_hege)
    logging.info(f'Read {len(bgp_scopes)} scopes')

    traceroute_reader = KafkaReader([traceroute_topic],
                                    config.get('kafka', 'bootstrap_servers'),
                                    start_ts,
                                    end_ts)
    logging.info(f'Reading traceroute scopes from {traceroute_topic}')
    with traceroute_reader:
        traceroute_scopes = read_scopes(traceroute_reader, min_peers, min_hege)
    logging.info(f'Read {len(traceroute_scopes)} scopes')

    writer = KafkaWriter(config.get('output', 'kafka_topic'),
                         config.get('kafka', 'bootstrap_servers'),
                         num_partitions=10,
                         config={'retention.ms': 5184000000})
    with writer:
        classify(bgp_scopes, traceroute_scopes, start_ts, writer)


if __name__ == '__main__':
    main()
