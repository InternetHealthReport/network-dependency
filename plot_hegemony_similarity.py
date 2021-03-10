import argparse
from collections import namedtuple
import configparser
from datetime import datetime
import logging
import os
import sys
from network_dependency.utils.helper_functions import parse_timestamp_argument
from network_dependency.utils.scope import read_scopes
import numpy as np
import matplotlib.pyplot as plt

Stats = namedtuple('Stats', 'min max avg median ixp')


def plot_scopes(data: dict, x_metric: str, y_metric: str, output: str, title: str) -> None:
    x = list()
    y = list()
    x_ixp = list()
    y_ixp = list()
    for as_ in data:
        if data[as_].ixp:
            x_ixp.append(getattr(data[as_], x_metric))
            y_ixp.append(getattr(data[as_], y_metric))
        else:
            x.append(getattr(data[as_], x_metric))
            y.append(getattr(data[as_], y_metric))
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
    ax_histx.set_title(title)
    # no labels
    ax_histx.tick_params(axis="x", labelbottom=False)
    ax_histy.tick_params(axis="y", labelleft=False)

    # the scatter plot:
    ax.set_xlim(0, 1)
    ax.set_xticks(np.arange(0, 1.1, 0.1))
    ax.set_xlabel(x_metric)
    ax.set_ylim(0, 1)
    ax.set_yticks(np.arange(0, 1.1, 0.1))
    ax.set_ylabel(y_metric)
    ax.grid(which='both')
    ax.plot([0, 1], [0, 1], c='gray', ls='--')
    if x:
        ax.scatter(x, y)
    if x_ixp:
        ax.scatter(x_ixp, y_ixp, c='red')

    # now determine nice limits by hand:
    binwidth = 0.05
    xymax = max(np.max(np.abs(x + x_ixp)), np.max(np.abs(y + y_ixp)))
    lim = (int(xymax / binwidth) + 1) * binwidth

    bins = np.arange(-lim, lim + binwidth, binwidth)
    ax_histx.hist(x + x_ixp, bins=bins)
    ax_histy.hist(y + y_ixp, bins=bins, orientation='horizontal')

    plt.savefig(output, bbox_inches='tight')


def plot_raw_scores(data: dict, output: str, title: str) -> None:
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
    ax_histx.set_title(title)
    # no labels
    ax_histx.tick_params(axis="x", labelbottom=False)
    ax_histy.tick_params(axis="y", labelleft=False)

    # the scatter plot:
    ax.set_xlim(0, 1)
    ax.set_xticks(np.arange(0, 1.1, 0.1))
    ax.set_xlabel('traceroute')
    ax.set_ylim(0, 1)
    ax.set_yticks(np.arange(0, 1.1, 0.1))
    ax.set_ylabel('BGP')
    ax.grid(which='both')
    ax.plot([0, 1], [0, 1], c='gray', ls='--')

    agg_x = list()
    agg_y = list()
    for as_ in data:
        x, y = zip(*data[as_])
        agg_x += x
        agg_y += y
        ax.scatter(x, y)

    # now determine nice limits by hand:
    binwidth = 0.05
    xymax = max(np.max(np.abs(agg_x)), np.max(np.abs(agg_y)))
    lim = (int(xymax / binwidth) + 1) * binwidth

    bins = np.arange(0, lim + binwidth, binwidth)
    ax_histx.hist(agg_x, bins=bins)
    ax_histy.hist(agg_y, bins=bins, orientation='horizontal')

    plt.savefig(output, bbox_inches='tight')


def get_stats(data: list, ixp_dependent: bool):
    min_ = np.min(data)
    max_ = np.max(data)
    avg = np.mean(data)
    median = np.median(data)
    return Stats(min_, max_, avg, median, ixp_dependent)


def write_data(data: dict, output_file: str):
    out_lines = ['as min max avg median\n']
    for as_ in data:
        stats = data[as_]
        out_lines.append(' '.join(map(str,
                                      [as_, stats.min, stats.max,
                                       stats.avg, stats.median]))
                         + '\n')
    with open(output_file, 'w') as f:
        f.writelines(out_lines)


def write_raw_data(data: dict, output_file: str):
    out_lines = list()
    for as_ in data:
        out_lines.append(' '.join(map(str, [as_] + data[as_]))
                         + '\n')
    with open(output_file, 'w') as f:
        f.writelines(out_lines)


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
    raw_data_overlap = dict()
    plot_data_union = dict()
    raw_data_union = dict()
    raw_scores = dict()
    for as_ in traceroute_scopes.keys() & bgp_scopes.keys():
        if as_ == '-1':
            continue
        traceroute_scope = traceroute_scopes[as_]
        bgp_scope = bgp_scopes[as_]
        raw_scores[as_] = list()
        for dep_as in traceroute_scope.union(bgp_scope):
            raw_scores[as_].append((traceroute_scope.get_score(dep_as, True),
                                    bgp_scope.get_score(dep_as, True)))
        ixp_dependent = traceroute_scope.is_ixp_dependent
        deltas_overlap = traceroute_scope \
            .get_score_deltas_for_overlap(bgp_scope)
        deltas_union = traceroute_scope.get_score_deltas_for_union(bgp_scope)
        scores_overlap = [abs(s) for _, s in deltas_overlap]
        scores_union = [abs(s) for _, s in deltas_union]
        if scores_overlap:
            plot_data_overlap[as_] = get_stats(scores_overlap, ixp_dependent)
            raw_data_overlap[as_] = deltas_overlap
        plot_data_union[as_] = get_stats(scores_union, ixp_dependent)
        raw_data_union[as_] = deltas_union

    output_dir = config.get('output', 'directory', fallback='./')
    if not output_dir.endswith('/'):
        output_dir += '/'
    out_date = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M')
    output_dir += out_date + '/'
    os.makedirs(output_dir, exist_ok=True)

    write_data(plot_data_overlap, output_dir + 'overlap.dat')
    write_raw_data(raw_data_overlap, output_dir + 'overlap-raw.dat')
    write_data(plot_data_union, output_dir + 'union.dat')
    write_raw_data(raw_data_union, output_dir + 'union-raw.dat')

    plot_scopes(plot_data_overlap, 'max', 'avg', output_dir + 'overlap_max_avg.pdf', out_date)
    plot_scopes(plot_data_overlap, 'max', 'median', output_dir + 'overlap_max_median.pdf', out_date)
    plot_scopes(plot_data_union, 'max', 'avg', output_dir + 'union_max_avg.pdf', out_date)
    plot_scopes(plot_data_union, 'max', 'median', output_dir + 'union_max_median.pdf', out_date)
    plot_raw_scores(raw_scores, output_dir + 'raw.pdf', out_date)
