import argparse
import configparser
from datetime import datetime
import logging
import os
import sys
from network_dependency.utils.helper_functions import parse_timestamp_argument
from network_dependency.utils.scope import read_scopes
import numpy as np
import matplotlib.pyplot as plt


def plot(data: dict, output: str) -> None:
    x = list()
    y = list()
    for as_ in data:
        mx, avg = data[as_]
        x.append(mx)
        y.append(avg)
    # definitions for the axes
    left, width = 0.1, 0.65
    bottom, height = 0.1, 0.65
    spacing = 0.035

    rect_scatter = [left, bottom, width, height]
    rect_histx = [left, bottom + height + spacing, width, 0.2]
    rect_histy = [left + width + spacing, bottom, 0.2, height]

    # start with a square Figure
    fig = plt.figure(figsize=(8, 8))

    ax = fig.add_axes(rect_scatter)
    ax_histx = fig.add_axes(rect_histx, sharex=ax)
    ax_histy = fig.add_axes(rect_histy, sharey=ax)
    # no labels
    ax_histx.tick_params(axis="x", labelbottom=False)
    ax_histy.tick_params(axis="y", labelleft=False)

    # the scatter plot:
    ax.set_xlim(0, 1)
    ax.set_xticks(np.arange(0, 1.1, 0.1))
    ax.set_xlabel('max')
    ax.set_ylim(0, 1)
    ax.set_yticks(np.arange(0, 1.1, 0.1))
    ax.set_ylabel('avg')
    ax.grid(which='both')
    ax.plot([0, 1], [0, 1], c='gray', ls='--')
    ax.scatter(x, y)

    # now determine nice limits by hand:
    binwidth = 0.1
    xymax = max(np.max(np.abs(x)), np.max(np.abs(y)))
    lim = (int(xymax / binwidth) + 1) * binwidth

    bins = np.arange(-lim, lim + binwidth, binwidth)
    ax_histx.hist(x, bins=bins)
    ax_histy.hist(y, bins=bins, orientation='horizontal')

    plt.savefig(output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config')
    parser.add_argument('-t', '--timestamp', help='Timestamp (as UNIX epoch'
                                                  'in seconds or '
                                                  'milliseconds, or in '
                                                  'YYYY-MM-DDThh:mm format)')
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
    timestamp_argument = config.get('input', 'timestamp', fallback=None)
    if args.timestamp is not None:
        logging.info('Overriding config timestamp.')
        timestamp_argument = args.timestamp
    timestamp = parse_timestamp_argument(timestamp_argument)
    if timestamp == 0:
        logging.error('Invalid timestamp specified: {}'
                      .format(timestamp_argument))
        exit(1)
    logging.info('Timestamp: {} {}'
                 .format(datetime.utcfromtimestamp(timestamp)
                         .strftime('%Y-%m-%dT%H:%M'), timestamp))
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
    bgp_scopes = read_scopes(bgp_kafka_topic,
                             timestamp * 1000,
                             bootstrap_servers,
                             scope_as_filter)
    traceroute_scopes = read_scopes(traceroute_kafka_topic,
                                    timestamp * 1000,
                                    bootstrap_servers,
                                    scope_as_filter)
    if not bgp_scopes or not traceroute_scopes:
        logging.error('No results for BGP ({} entries) or traceroute ({} '
                      'entries).'.format(len(bgp_scopes),
                                         len(traceroute_scopes)))
        exit(1)
    if traceroute_scopes.keys() - bgp_scopes.keys():
        logging.info('Traceroute scopes without matching BGP scope: {}'
                     .format(traceroute_scopes.keys() - bgp_scopes.keys()))
    # if bgp_scopes.keys() - traceroute_scopes.keys():
    #     logging.info('BGP scopes without matching traceroute scope: {}'
    #                  .format(bgp_scopes.keys() - traceroute_scopes.keys()))
    plot_data_overlap = dict()
    plot_data_union = dict()
    for as_ in traceroute_scopes.keys() & bgp_scopes.keys():
        if as_ == '-1':
            continue
        traceroute_scope = traceroute_scopes[as_]
        bgp_scope = bgp_scopes[as_]
        scores_overlap = [abs(s) for _, s in
                          traceroute_scope
                              .get_score_deltas_for_overlap(bgp_scope)]
        scores_union = [abs(s) for _, s in
                        traceroute_scope.get_score_deltas_for_union(bgp_scope)]
        # Maximum and average.
        if scores_overlap:
            plot_data_overlap[as_] = (max(scores_overlap),
                                      sum(scores_overlap) / len(scores_overlap))
        plot_data_union[as_] = (max(scores_union),
                                sum(scores_union) / len(scores_union))
    output_dir = config.get('output', 'directory', fallback='./')
    if not output_dir.endswith('/'):
        output_dir += '/'
    output_dir += datetime.utcfromtimestamp(timestamp) \
                      .strftime('%Y-%m-%dT%H:%M') + '/'
    os.makedirs(output_dir, exist_ok=True)
    out_lines_overlap = ['as max avg\n']
    out_lines_union = ['as max avg\n']
    for as_ in plot_data_overlap:
        mx, avg = plot_data_overlap[as_]
        out_lines_overlap.append(' '.join(map(str, [as_, mx, avg])) + '\n')
        mx, avg = plot_data_union[as_]
        out_lines_union.append(' '.join(map(str, [as_, mx, avg])) + '\n')
    with open(output_dir + 'overlap.dat', 'w') as fo, \
            open(output_dir + 'union.dat', 'w') as fu:
        fo.writelines(out_lines_overlap)
        fu.writelines(out_lines_union)
    plot(plot_data_overlap, output_dir + 'overlap.pdf')
    plot(plot_data_union, output_dir + 'union.pdf')
