import argparse
import configparser
import logging
import sys
from collections import defaultdict
from dataclasses import dataclass, field

from kafka_wrapper.kafka_reader import KafkaReader
from kafka_wrapper.kafka_writer import KafkaWriter
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
        # Ignore "self" dependency with score 1.0.
        if asn == scope:
            continue
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
        else:
            ret[scope] = Scope(nb_peers)
        ret[scope].dependencies[asn] = hege
    for scope in ret:
        compute_ranks(ret[scope])
    return ret


def compute_standard_competition_ranking(data: list) -> (int, dict, dict):
    """Compute the standard competition ranking for the given data and
    return the maximum rank and two dictionaries mapping the rank to a
    set of AS numbers and the reverse, mapping the AS number to a rank.

    Parameters
    ----------
    data : list
        Sorted list of (asn, score) pairs.

    Returns
    -------
    max_rank : int
        Rank of last entry.
    rank_asn_map : dict
        Dictionary mapping each rank to a set of AS numbers.
    asn_rank_map : dict
        Dictionary mapping each AS number to a rank.

    Notes
    -----
    In competition ranking, items that compare equal receive the same
    ranking number, and then a gap is left in the ranking numbers.

    Example:
        Input: [(1, 1.0), (2, 0.5), (3, 0.5), (4, 0.1)]

        Output ranking:
            1 -> 0
            2 -> 1
            3 -> 1
            4 -> 3
    """
    rank_asn_map = defaultdict(set)
    asn_rank_map = dict()
    prev_score = -1
    curr_rank = -1
    for asn, score in data:
        if score != prev_score:
            if curr_rank == -1:
                curr_rank = 0
            else:
                curr_rank += len(rank_asn_map[curr_rank])
        rank_asn_map[curr_rank].add(asn)
        asn_rank_map[asn] = curr_rank
        prev_score = score
    return curr_rank, rank_asn_map, asn_rank_map


def classify_overlapping_dependencies(bgp: list, tr: list) -> (set, set, dict):
    """Classify overlapping dependencies based on their standard
    competition rank.

    Parameters
    ----------
    bgp : list
        List of sorted BGP scores.
    tr : list
        List of sorted traceroute scores.

    Returns
    -------
    equal : set
        Set of equal dependency AS numbers.
    mismatched : set
        Set of mismatched dependency AS numbers.
    asn_rank_map: dict
        Dictionary mapping each AS number to a (bgp_rank, tr_rank) pair.

    Notes
    -----
    Both input lists should be sorted by descending hegemony score.
    The lists should contain tuples of the form (asn, score).
    """
    highest_bgp_rank, bgp_rank_asn_map, bgp_asn_rank_map = \
        compute_standard_competition_ranking(bgp)
    highest_tr_rank, tr_rank_asn_map, tr_asn_rank_map = \
        compute_standard_competition_ranking(tr)
    highest_rank = max(highest_bgp_rank, highest_tr_rank)

    equal = set()
    mismatched = set()
    asn_competition_rank_map = dict()

    for rank in range(highest_rank + 1):
        if rank not in bgp_rank_asn_map and rank not in tr_rank_asn_map:
            continue
        elif rank not in bgp_rank_asn_map:
            tr_asns = tr_rank_asn_map[rank]
            mismatched.update(tr_asns)
        elif rank not in tr_rank_asn_map:
            bgp_asns = bgp_rank_asn_map[rank]
            mismatched.update(bgp_asns)
        else:
            bgp_asns = bgp_rank_asn_map[rank]
            tr_asns = tr_rank_asn_map[rank]
            intersection = bgp_asns.intersection(tr_asns)
            sym_difference = bgp_asns.symmetric_difference(tr_asns)
            if not mismatched.isdisjoint(intersection) \
                    or not equal.isdisjoint(sym_difference):
                logging.warning('Overlap between previously mismatched '
                                'dependencies and new equal dependencies or '
                                'previously equal dependencies and new '
                                'mismatched dependencies.')
                logging.warning(f'eq: {equal}')
                logging.warning(f'mm: {mismatched}')
                logging.warning(f'new eq: {intersection}')
                logging.warning(f'new mm: {sym_difference}')
            equal.update(intersection)
            mismatched.update(sym_difference)
    if not equal.isdisjoint(mismatched):
        logging.error(f'eq and mm are not disjoint.')
        logging.error(f'eq: {equal}')
        logging.error(f'mm: {mismatched}')
    for asn in equal:
        asn_competition_rank_map[asn] = (bgp_asn_rank_map[asn],
                                         tr_asn_rank_map[asn])
        if asn_competition_rank_map[asn][0] != asn_competition_rank_map[asn][1]:
            logging.error(f'Equal dependency has unequal rank.')
            logging.error(f'bgp: {asn_competition_rank_map[asn][0]}')
            logging.error(f' tr: {asn_competition_rank_map[asn][1]}')
    for asn in mismatched:
        asn_competition_rank_map[asn] = (bgp_asn_rank_map[asn],
                                         tr_asn_rank_map[asn])
    return equal, mismatched, asn_competition_rank_map


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
        overlap_bgp_scores = [(asn, bgp_scope.dependencies[asn])
                              for asn in overlap_dependencies]
        overlap_tr_scores = [(asn, tr_scope.dependencies[asn])
                             for asn in overlap_dependencies]
        overlap_bgp_scores.sort(key=lambda t: t[1], reverse=True)
        overlap_tr_scores.sort(key=lambda t: t[1], reverse=True)
        equal_dependencies, mismatched_dependencies, asn_competition_rank = \
            classify_overlapping_dependencies(overlap_bgp_scores,
                                              overlap_tr_scores)
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
                              asn_competition_rank[asn][0],
                              tr_scope.dependencies[asn],
                              tr_scope.dependency_ranks[asn],
                              asn_competition_rank[asn][1])
                             for asn in mismatched_dependencies]
        mismatched_scores.sort(key=lambda t: t[1], reverse=True)
        equal_scores = [(asn,
                         bgp_scope.dependencies[asn],
                         bgp_scope.dependency_ranks[asn],
                         tr_scope.dependencies[asn],
                         tr_scope.dependency_ranks[asn],
                         asn_competition_rank[asn][0])
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
