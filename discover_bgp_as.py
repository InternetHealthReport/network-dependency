import argparse
import logging
from datetime import datetime, timezone

from confluent_kafka import OFFSET_BEGINNING, OFFSET_END

from kafka_wrapper.kafka_reader import KafkaReader
from network_dependency.utils.helper_functions import parse_timestamp_argument

BOOTSTRAP_SERVERS = 'localhost:9092'
OUTPUT_DATE_FMT = '%Y-%m-%dT%H:%M:%S'

start_ts = OFFSET_BEGINNING
end_ts = OFFSET_END


def check_key(source: dict, key: str) -> bool:
    if key not in source or not source[key]:
        logging.debug(f'Missing or none "{key}" field in entry: {source}')
        return True
    return False


def flatten_as_path(path: str) -> list:
    ret = list()
    try:
        for hop in path.split(' '):
            if hop.startswith('{'):
                ret += map(int, hop.strip('{}').split(','))
            else:
                ret.append(int(hop))
    except ValueError as e:
        logging.error(f'Error while parsing AS path: {path}. {e}')
        return list()
    return ret


def read_topic(topic: str) -> set:
    reader = KafkaReader([topic], BOOTSTRAP_SERVERS, start_ts, end_ts)
    as_set = set()
    with reader:
        for msg in reader.read():
            if check_key(msg, 'elements'):
                continue
            for element in msg['elements']:
                if check_key(element, 'type') or check_key(element, 'fields'):
                    continue
                if element['type'] != 'R' and element['type'] != 'A':
                    # Process only RIBs and announcements.
                    continue
                if check_key(element['fields'], 'as-path'):
                    continue
                flattened_path = flatten_as_path(element['fields']['as-path'])
                for as_ in flattened_path:
                    if as_ not in as_set:
                        as_set.add(as_)
    return as_set


def write_output(data: set, collector: str) -> None:
    output = 'as.' + collector + '.'
    if start_ts != OFFSET_BEGINNING:
        output += datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc) \
            .strftime(OUTPUT_DATE_FMT)
    else:
        output += str(OFFSET_BEGINNING)
    output += '--'
    if end_ts != OFFSET_END:
        output += datetime.fromtimestamp(end_ts / 1000, tz=timezone.utc) \
            .strftime(OUTPUT_DATE_FMT)
    else:
        output += str(OFFSET_END)
    output += '.csv'
    logging.info(f'Writing {len(data)} ASes to {output}')
    with open(output, 'w') as f:
        f.write('\n'.join(map(str, sorted(data))) + '\n')


def main() -> None:
    global start_ts, end_ts
    desc = """Produce a list of unique ASes occurring in the AS paths visible at
              the specified collector. A start and/or end time can be specified
              either as UNIX epoch in (milli)secconds or in the format
              '%Y-%m-%dT%H:%M'. Output is written to
              as.<collector>.<start-ts>--<end-ts>.csv"""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('collector', help='Name of BGP collector. Will be '
                                          'inserted into template '
                                          'ihr_bgp_<name>_ribs')
    parser.add_argument('-s', '--start', help='Start timestamp')
    parser.add_argument('-e', '--end', help='End timestamp')
    log_fmt = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S'
    )

    args = parser.parse_args()
    if args.start:
        start_ts = parse_timestamp_argument(args.start) * 1000
        if start_ts == 0:
            logging.error(f'Invalid start timestamp specified: {args.start}')
            exit(1)
    if args.end:
        end_ts = parse_timestamp_argument(args.end) * 1000
        if end_ts == 0:
            logging.error(f'Invalid end timestamp specified: {args.end}')
            exit(1)

    rib_set = read_topic('ihr_bgp_' + args.collector + '_ribs')
    update_set = read_topic('ihr_bgp_' + args.collector + '_updates')

    write_output(rib_set.union(update_set), args.collector)


if __name__ == '__main__':
    main()
    exit(0)
