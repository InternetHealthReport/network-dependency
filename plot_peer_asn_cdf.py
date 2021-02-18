import argparse
import os

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np

STAT_SUFFIX = '-peers.dat'
PLOT_SUFFIX = '-peer_cdf.pdf'

mpl.rcParams['font.size'] = 12
mpl.rcParams['grid.linestyle'] = 'dashed'
mpl.rcParams['figure.autolayout'] = True


def read_stat_files(stats_dir: str) -> (str, np.ndarray):
    it = os.scandir(stats_dir)
    for entry in it:
        if not entry.is_file() or not entry.name.endswith(STAT_SUFFIX):
            continue
        asn = entry.name[:-len(STAT_SUFFIX)]
        data = np.loadtxt(output_dir + entry.name, skiprows=1)
        yield asn, data


def plot(asn: str, output_file_name: str, data: np.ndarray) -> None:
    print(f'Plotting {output_file_name}')
    p_values = np.arange(len(data)) / (len(data) - 1)
    occurrences = data[:, 1]
    occurrences.sort()
    fig, ax = plt.subplots()
    ax.set_title('Target AS ' + asn)
    ax.set_ylabel('p')
    ax.set_xlabel('Peers per AS')
    ax.plot(occurrences, p_values)
    ax.set_ylim(0.9, 1.01)
    ax.set_xlim(0, occurrences.max())
    ax.grid(which='both')
    plt.savefig(output_file_name)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('stats_dir')
    parser.add_argument('output_dir')
    args = parser.parse_args()

    stats_dir = args.stats_dir
    if not stats_dir.endswith('/'):
        stats_dir += '/'
    output_dir = args.output_dir
    if not output_dir.endswith('/'):
        output_dir += '/'
    for asn, data in read_stat_files(stats_dir):
        plot(asn, output_dir + asn + PLOT_SUFFIX, data)
