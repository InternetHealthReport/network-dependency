import argparse
import configparser
import logging
import sys
from datetime import datetime

from network_dependency.utils.helper_functions import parse_timestamp_argument
from network_dependency.utils.scope import read_legacy_scopes

stats = {'overlapping': {'set': set(),
                         'num': 0}}

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('-tt', '--traceroute_timestamp',
                        help='Timestamp (as UNIX epoch in seconds or '
                             'milliseconds, or in YYYY-MM-DDThh:mm format)')
    parser.add_argument('-bt', '--bgp_timestamp',
                        help='Timestamp (as UNIX epoch in seconds or '
                             'milliseconds, or in YYYY-MM-DDThh:mm format)')
    # Logging
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT,  # filename='../compare_results.log',
        level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S',
    )
    logging.info("Started: %s" % sys.argv)

    args = parser.parse_args()

    # Read config
    config = configparser.ConfigParser()
    config.read(args.config)
    traceroute_timestamp_argument = config.get('input', 'traceroute_timestamp', fallback=None)
    if args.traceroute_timestamp is not None:
        logging.info('Overriding config traceroute timestamp.')
        traceroute_timestamp_argument = args.traceroute_timestamp
    traceroute_timestamp = parse_timestamp_argument(traceroute_timestamp_argument)
    if traceroute_timestamp == 0:
        logging.error('Invalid traceroute timestamp specified: {}'
                      .format(traceroute_timestamp_argument))
        exit(1)
    bgp_timestamp_argument = config.get('input', 'bgp_timestamp', fallback=None)
    if args.bgp_timestamp is not None:
        logging.info('Overriding config BGP timestamp.')
        traceroute_timestamp_argument = args.bgp_timestamp
    bgp_timestamp = parse_timestamp_argument(bgp_timestamp_argument)
    if bgp_timestamp == 0:
        logging.error('Invalid BGP timestamp specified: {}'
                      .format(bgp_timestamp_argument))
        exit(1)
    logging.info('Timestamp: {} {}'
                 .format(datetime.utcfromtimestamp(traceroute_timestamp)
                         .strftime('%Y-%m-%dT%H:%M'), traceroute_timestamp))
    bgp_kafka_topic = config.get('input', 'bgp_kafka_topic',
                                 fallback='ihr_hegemony')
    traceroute_kafka_topic = config.get('input', 'traceroute_kafka_topic',
                                        fallback='ihr_hegemony_traceroutev4')
    bootstrap_servers = config.get('kafka', 'bootstrap_servers',
                                   fallback='kafka2:9092')
    scope_as_filter = config.get('input', 'asns', fallback=None)
    if not scope_as_filter:
        scope_as_filter = None
    if scope_as_filter is not None:
        scope_as_filter = set(scope_as_filter.split(','))
    bgp_scopes = read_legacy_scopes(bgp_kafka_topic,
                                    bgp_timestamp * 1000,
                                    bootstrap_servers,
                                    scope_as_filter)
    traceroute_scopes = read_legacy_scopes(traceroute_kafka_topic,
                                           traceroute_timestamp * 1000,
                                           bootstrap_servers,
                                           scope_as_filter)
    out_lines = [config.get('input', 'traceroute_timestamp') + ',' + str(traceroute_timestamp) + '\n']
    for tr_scope_as in traceroute_scopes:
        if tr_scope_as == '-1':
            continue
        tr_scope = traceroute_scopes[tr_scope_as]
        if tr_scope_as not in bgp_scopes:
            continue
        bgp_scope = bgp_scopes[tr_scope_as]
        print(f'AS {tr_scope_as}')
        print(f'Overlap: {tr_scope.overlap_with(bgp_scope)}')
        print(f'   Size: {len(tr_scope.overlap_with(bgp_scope)):3d} '
              f'{bgp_scope.get_overlap_percentage_with(tr_scope):6.2f}%')
        print('TR - BGP')
        print(f' Set: {tr_scope.not_in(bgp_scope)}')
        print(f'Size: {len(tr_scope.not_in(bgp_scope))}')
        print('BGP - TR')
        print(f' Set: {bgp_scope.not_in(tr_scope)}')
        print(f'Size: {len(bgp_scope.not_in(tr_scope))}')
        print(f'Score differences: {tr_scope.get_score_deltas_for_overlap(bgp_scope)}')
        print(f'Missing score sum: {bgp_scope.get_missing_score_sum(tr_scope)}')
        print(f'  Rank difference: {bgp_scope.get_rank_difference_number(tr_scope)}')
        print(f'        Magnitude: {bgp_scope.get_rank_difference_magnitudes(tr_scope)}')
        print('')
        print(tr_scope_as, tr_scope.not_in(bgp_scope), bgp_scope.not_in(tr_scope),
              bgp_scope.get_overlap_percentage_with(tr_scope))
        line = [tr_scope_as, ' '.join(map(str, tr_scope.not_in(bgp_scope))),
                ' '.join(map(str, bgp_scope.not_in(tr_scope))), bgp_scope.get_overlap_percentage_with(tr_scope)]
        out_lines.append(','.join(map(str, line)) + '\n')
        for i in tr_scope.overlap_with(bgp_scope):
            tr_score = tr_scope.get_score(i)
            bgp_score = bgp_scope.get_score(i)
            print(f'  {int(i):6d} {tr_score * 100:6.2f} {bgp_score * 100:6.2f} {(tr_score - bgp_score) * 100:=+7.2f}')
            line = ['', i, tr_score * 100, bgp_score * 100, (tr_score - bgp_score) * 100]
            out_lines.append(','.join(map(str, line)) + '\n')
    with open(config.get('output', 'file_name'), 'w') as f:
        f.writelines(out_lines)
