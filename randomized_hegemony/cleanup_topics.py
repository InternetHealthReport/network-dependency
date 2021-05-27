import argparse
import logging
import subprocess
import sys
from multiprocessing import Pool

from confluent_kafka.admin import AdminClient, KafkaException


def parse_collector_list(file: str) -> list:
    with open(file, 'r') as f:
        return [line.strip() for line in f.readlines()]


def cleanup_collector(collector: str, args: argparse.Namespace):
    rib_topic = f'ihr_bgp_{collector}_ribs'
    updates_topic = f'ihr_bgp_{collector}_updates'
    bgpatom_topic = f'ihr_bgp_atom_{collector}'
    bgpatom_meta_topic = f'ihr_bgp_atom_meta_{collector}'
    bcscore_topic = f'ihr_bcscore_{collector}'
    bcscore_meta_topic = f'ihr_bcscore_meta_{collector}'
    hegemony_topic = f'ihr_hegemony_{collector}'
    hegemony_meta_topic = f'ihr_hegemony_meta_{collector}'

    archive_topics = [rib_topic,
                      bgpatom_topic,
                      bcscore_topic,
                      hegemony_topic]
    delete_topics = [updates_topic,
                     bgpatom_meta_topic,
                     bcscore_meta_topic,
                     hegemony_meta_topic]

    dump_command = ['python3', args.dump_script, '-s', args.server]
    if args.output:
        dump_command += ['-o', args.output]
    logging.info(f'Dump command: {" ".join(dump_command)}')

    for topic in archive_topics:
        logging.info(f'Archiving topic: {topic}')
        try:
            subprocess.run(dump_command + [topic], check=True,
                           capture_output=True)
        except subprocess.CalledProcessError as e:
            logging.error(f'Failed to dump topic {topic}: {e}')
            continue
        delete_topics.append(topic)

    logging.info(f'Deleting topics: {delete_topics}')
    admin_client = AdminClient({'bootstrap.servers': args.server})
    res = admin_client.delete_topics(delete_topics, operation_timeout=10)
    for topic in res:
        try:
            res[topic].result()
        except KafkaException as e:
            logging.error(f'Failed to delete topic {topic}: {e}')


def main() -> None:
    log_fmt = '%(asctime)s %(levelname)s %(message)s'
    logging.basicConfig(
        format=log_fmt,
        level=logging.INFO,
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    parser = argparse.ArgumentParser()
    parser.add_argument('collector_list')
    parser.add_argument('dump_script')
    parser.add_argument('-s', '--server', default='localhost:9092')
    parser.add_argument('-o', '--output')

    args = parser.parse_args()

    logging.info(f'Started: {sys.argv}')

    collector_list = parse_collector_list(args.collector_list)

    with Pool() as p:
        p.starmap(cleanup_collector, [(c, args) for c in collector_list])


if __name__ == '__main__':
    main()
    sys.exit(0)
