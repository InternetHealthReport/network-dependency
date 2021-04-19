import argparse
import logging
import os

from time_bin.utils.helper_functions import load_bins

OUTPUT_DELIMITER = ','


def compute_windows(bins: dict) -> dict:
    full_feed_threshold = bins['peer_as']
    bin_times, bin_values = zip(*bins['data'])
    non_full_as = dict()
    full_as = set()  # We don't care about the details here.
    as_start_times = dict()
    windows = dict()
    logging.info(f'Starting full feed window calculation for a threshold of '
                 f'{full_feed_threshold} ASes over {len(bin_times)} bins')
    for idx in range(len(bin_times)):
        for target_as in bin_values[idx]:
            if target_as in full_as:
                continue
            if target_as not in non_full_as:
                non_full_as[target_as] = set()
                as_start_times[target_as] = bin_times[idx]
            for peer_as in bin_values[idx][target_as]:
                if peer_as not in non_full_as[target_as]:
                    non_full_as[target_as].add(peer_as)
            if len(non_full_as[target_as]) >= full_feed_threshold:
                window_length = bin_times[idx] - as_start_times[target_as]
                windows[target_as] = (len(non_full_as[target_as]),
                                      as_start_times[target_as],
                                      bin_times[idx],
                                      window_length.total_seconds())
                full_as.add(target_as)
                non_full_as.pop(target_as)
    total_as_count = len(windows) + len(non_full_as)
    logging.info(f'Got full feeds for {len(windows)} out of {total_as_count} '
                 f'ASes ({(len(windows) / total_as_count) * 100:5.2f}%)')
    # Add ASes which were seen, but got no full feed.
    for target_as in non_full_as:
        windows[target_as] = (len(non_full_as[target_as]),
                              as_start_times[target_as], None, -1)
    return windows


def write_output(windows: dict, output: str) -> None:
    os.makedirs(os.path.dirname(output), exist_ok=True)
    logging.info(f'Writing {len(windows)} windows to {output}')
    with open(output, 'w') as f:
        f.write(OUTPUT_DELIMITER
                .join(['as', 'peers', 'start', 'end', 'length(s)']) + '\n')
        for as_, (peers, start, end, length) in sorted(windows.items(),
                                                       key=lambda t: t[1][0],
                                                       reverse=True):
            start_ts = int(start.timestamp())
            if not end:
                end_ts = 0
            else:
                end_ts = int(end.timestamp())
            f.write(OUTPUT_DELIMITER
                    .join(map(str, [as_, peers, start_ts, end_ts, length]))
                    + '\n')


def main() -> None:
    FORMAT = '%(asctime)s %(processName)s %(message)s'
    logging.basicConfig(
        format=FORMAT,
        level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')

    desc = """Compute the window size required for each AS to get a full feed 
    (i.e., be targeted by all peer ASes available in the data set)."""
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('bins', help='Compressed bin file')
    parser.add_argument('output', help='Output file')
    args = parser.parse_args()

    bins = load_bins(args.bins)
    windows = compute_windows(bins)
    if windows:
        write_output(windows, args.output)


if __name__ == '__main__':
    main()
