import bz2
import logging
import pickle


def load_bins(bin_file: str) -> dict:
    data = pickle.load(bz2.open(bin_file, 'rb'))
    logging.info(f'Loaded bin {bin_file} mode: {data["mode"]} probes: '
                 f'{data["probes"]} peer_as: {data["peer_as"]} unique_dst: '
                 f'{data["unique_dst"]} bin_size: {str(data["bin_size"])} bins: '
                 f'{len(data["data"])}')
    return data
