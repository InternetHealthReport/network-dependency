import argparse
import bz2
import logging
import os
import pickle
from collections import namedtuple
from datetime import timedelta
from multiprocessing import Pool
from sys import exit

import msgpack

from network_dependency.utils.helper_functions import parse_range_argument

MAX_ASN = 69551
MAX_PFX = 752954
COMPRESSED_OUTPUT_SUFFIX = '.msgpack.bz2'
OUTPUT_SUFFIX = '.dat'
OUTPUT_SEPARATOR = ' '

WindowResult = namedtuple('WindowResult', 'start end size length')


def load_bins(bin_file: str) -> (str, timedelta, list):
    data = pickle.load(bz2.open(bin_file, 'rb'))
    logging.info('Loaded bin {} mode: {} bin_size: {} bins: {}'
                 .format(bin_file, data['mode'], str(data['bin_size']),
                         len(data['data'])))
    return data['mode'], data['bin_size'], data['data']


def assign_computations(mode: str,
                        bin_size: timedelta,
                        bins: list,
                        target_p: list,
                        target_c: list,
                        output_dir: str,
                        threads: int) -> list:
    arguments = list()
    for c in target_c:
        for p in target_p:
            for idx in range(len(bins)):
                arguments.append((mode, bin_size, idx, bins, p, c, output_dir))

    logging.info('Assigning {} window calculations to {} workers.'
                 .format(len(arguments), threads))
    with Pool(threads) as pl:
        windows = pl.starmap(compute_window, arguments)
    logging.info('Finished calculating {} windows.'.format(len(windows)))
    return windows


def compute_window(mode: str,
                   bin_size: timedelta,
                   start_idx: int,
                   bins: list,
                   target_p: int,
                   target_c: int,
                   output_dir: str) -> (int, int, WindowResult):
    if mode == 'as':
        coverage_target = (MAX_ASN / 100) * target_p
    else:
        coverage_target = (MAX_PFX / 100) * target_p
    covered_destinations = dict()
    uncovered_destinations = dict()
    bin_times, bin_values = zip(*bins)
    curr_idx = start_idx
    while curr_idx < len(bin_values) \
            and len(covered_destinations) < coverage_target:
        curr_bin = bin_values[curr_idx]
        for dst in curr_bin:
            if dst in covered_destinations:
                # Destination is already covered. Only increase counters.
                for peer in curr_bin[dst]:
                    if peer not in covered_destinations[dst]:
                        covered_destinations[dst][peer] = 0
                    covered_destinations[dst][peer] += 1
                continue
            # Destination not yet covered.
            if dst not in uncovered_destinations:
                uncovered_destinations[dst] = dict()
            for peer in curr_bin[dst]:
                if peer not in uncovered_destinations[dst]:
                    uncovered_destinations[dst][peer] = 0
                uncovered_destinations[dst][peer] += 1
            if len(uncovered_destinations[dst]) >= target_c:
                # Covered by enough peers.
                covered_destinations[dst] = uncovered_destinations.pop(dst)
        curr_idx += 1
    curr_idx -= 1
    start_ts = int(bin_times[start_idx].timestamp())
    end_ts = int((bin_times[curr_idx] + bin_size).timestamp())
    window_size = len(covered_destinations)
    window_length = bin_times[curr_idx] + bin_size - bin_times[start_idx]
    if len(covered_destinations) < coverage_target:
        # Coverage target unreachable for this start_idx.
        logging.debug('c: {} p: {} Coverage target unreachable: {}'
                      .format(target_c, target_p, start_ts))
        return target_c, target_p, WindowResult(start_ts, end_ts, -1, -1)
    output_path = output_dir + str(target_c) + '/' + str(target_p) + '/'
    output_file = output_path + str(start_ts) + COMPRESSED_OUTPUT_SUFFIX
    # Merge dicts for output
    covered_destinations.update(uncovered_destinations)
    os.makedirs(output_path, exist_ok=True)
    with bz2.open(output_file, 'wb') as f:
        msgpack.dump(covered_destinations, f)
    logging.debug('c: {} p: {} Finished: {} {} {} {}'
                  .format(target_c, target_p, start_ts, end_ts, window_size,
                          str(window_length)))
    return target_c, target_p, WindowResult(start_ts, end_ts, window_size,
                                            int(window_length.total_seconds()))


def write_summaries(windows: list, output_dir: str) -> None:
    out_lines = list()
    last_c = None
    last_p = None
    # windows has the structure
    #   (target_c, target_p, (start, end, size, length))
    # so this sort function orders by target_c, then target_p and the
    # windows by start time.
    for c, p, window in sorted(windows):
        if last_c is None:
            last_c = c
            last_p = p
        if c != last_c or p != last_p:
            out_file = output_dir + str(last_c) + '/' + str(last_p) \
                       + OUTPUT_SUFFIX
            with open(out_file, 'w') as f:
                f.write('start end size length(s)\n')
                f.writelines(out_lines)
            out_lines.clear()
            last_c = c
            last_p = p
        if window.size >= 0:
            out_lines.append(OUTPUT_SEPARATOR.join(map(str, window)) + '\n')
    # Flush last entry.
    out_file = output_dir + str(last_c) + '/' + str(last_p) \
               + OUTPUT_SUFFIX
    with open(out_file, 'w') as f:
        f.write('start end size length(s)\n')
        f.writelines(out_lines)


def main() -> None:
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    desc = """Compute windows based on the compressed bin file generated by
           compute_bins.py. Window size is limited by a combination of
           percentage of ASes / prefixes covered and peer ASes required for a
           destination to be counted as covered. target_p and target_c can
           either be specified by a single number, a comma-separated list, or in
           pythonic range notation start:end:step."""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('bins', help='Compressed bin file')
    parser.add_argument('target_p', help='Percentage of ASes / prefixes which '
                                         'should be covered by the windows')
    parser.add_argument('target_c', help='Number of peers required for a '
                                         'destination to be counted as covered')
    parser.add_argument('output_dir', help='Output directory in which data '
                                           'files are stored')
    parser.add_argument('-th', '--threads', type=int, help='Number of worker '
                                                           'threads',
                        default=os.cpu_count())
    args = parser.parse_args()

    target_p = parse_range_argument(args.target_p)
    if not target_p:
        logging.error('Invalid target_p specified.')
        exit(1)
    target_c = parse_range_argument(args.target_c)
    if not target_c:
        logging.error('Invalid target_c specified.')
        exit(1)
    output_dir = args.output_dir
    if not output_dir.endswith('/'):
        output_dir += '/'

    mode, bin_size, bins = load_bins(args.bins)
    windows = assign_computations(mode, bin_size, bins, target_p, target_c,
                                  output_dir, args.threads)
    write_summaries(windows, output_dir)


if __name__ == '__main__':
    main()
