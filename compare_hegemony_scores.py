import configparser
import logging
import sys
from network_dependency.kafka.kafka_reader import KafkaReader
from network_dependency.utils.scope import Scope
from network_dependency.utils.helper_functions import convert_date_to_epoch


def read_bgp_scopes(topic: str,
                    timestamp: int,
                    bootstrap_servers: str,
                    scope_as_filter=None) -> dict:
    ret = dict()
    reader = KafkaReader([topic], bootstrap_servers, timestamp, timestamp + 1)
    logging.debug('Reading BGP topic {} at timestamp {}'
                  .format(topic, timestamp))
    with reader:
        for msg in reader.read():
            scope = msg['scope']
            if scope == 0 \
                    or (scope_as_filter is not None
                        and scope not in scope_as_filter):
                continue
            if scope not in ret:
                ret[scope] = Scope(scope)
            ret[scope].add_as(msg['asn'], msg['hege'])
    return ret


def read_traceroute_scopes(topic: str,
                           timestamp: int,
                           bootstrap_servers: str,
                           scope_as_filter=None) -> dict:
    ret = dict()
    reader = KafkaReader([topic], bootstrap_servers, timestamp, timestamp + 1)
    logging.debug('Reading traceroute topic {} at timestamp {}'
                  .format(topic, timestamp))
    with reader:
        for msg in reader.read():
            scope = msg['scope']
            if scope == 0 \
                    or (scope_as_filter is not None
                        and scope not in scope_as_filter):
                continue
            if scope in ret:
                logging.error('Duplicate scope {} for timestamp {}'
                              .format(scope, timestamp))
                continue
            ret[scope] = Scope(scope)
            for as_ in msg['scope_hegemony']:
                ret[scope].add_as(as_, msg['scope_hegemony'][as_])
    return ret


if __name__ == '__main__':
    # Logging
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT, filename='../compare_results.log',
        level=logging.DEBUG, datefmt='%Y-%m-%d %H:%M:%S',
        filemode='w'
        )
    logging.info("Started: %s" % sys.argv)

    # Read config
    config = configparser.ConfigParser()
    config.read('../conf/comparison.ini')
    timestamp = convert_date_to_epoch(config.get('input', 'timestamp'))
    if timestamp == 0:
        logging.error('Invalid timestamp specified: {}'
                      .format(config.get('input', 'timestamp')))
        exit(1)
    bgp_kafka_topic = config.get('input', 'bgp_kafka_topic',
                                 fallback='ihr_hegemony_values_ipv4')
    traceroute_kafka_topic = config.get('input', 'traceroute_kafka_topic',
                                        fallback='ihr_hegemony_traceroutev4')
    bootstrap_servers = config.get('kafka', 'bootstrap_servers',
                                   fallback='kafka2:9092')
    scope_as_filter = config.get('input', 'asns', fallback=None)
    if not scope_as_filter:
        scope_as_filter = None
    if scope_as_filter is not None:
        scope_as_filter = set(scope_as_filter.split(','))
    bgp_scopes = read_bgp_scopes(bgp_kafka_topic,
                                 timestamp * 1000,
                                 bootstrap_servers,
                                 scope_as_filter)
    traceroute_scopes = read_traceroute_scopes(traceroute_kafka_topic,
                                               timestamp * 1000,
                                               bootstrap_servers,
                                               scope_as_filter)
    out_lines = [config.get('input', 'timestamp') + ',' + str(timestamp) + '\n']
    for tr_scope_as in traceroute_scopes:
        tr_scope = traceroute_scopes[tr_scope_as]
        if tr_scope_as not in bgp_scopes:
            continue
        bgp_scope = bgp_scopes[tr_scope_as]
        print(tr_scope_as, tr_scope.not_in(bgp_scope), bgp_scope.not_in(tr_scope), bgp_scope.get_overlap_percentage_with(tr_scope))
        line = [tr_scope_as, ' '.join(map(str, tr_scope.not_in(bgp_scope))), ' '.join(map(str, bgp_scope.not_in(tr_scope))), bgp_scope.get_overlap_percentage_with(tr_scope)]
        out_lines.append(','.join(map(str, line)) + '\n')
        for i in tr_scope.overlap_with(bgp_scope):
            tr_score = tr_scope.get_score(i)
            bgp_score = bgp_scope.get_score(i)
            print(f'  {int(i):6d} {tr_score*100:6.2f} {bgp_score*100:6.2f} {(tr_score - bgp_score) * 100:=+7.2f}')
            line = ['', i, tr_score * 100, bgp_score * 100, (tr_score - bgp_score) * 100]
            out_lines.append(','.join(map(str, line)) + '\n')
    with open(config.get('output', 'file_name'), 'w') as f:
        f.writelines(out_lines)
