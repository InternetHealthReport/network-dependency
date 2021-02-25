import logging


class ASPath:
    def __init__(self):
        self.nodes = list()
        self.ixp_nodes = list()
        self.reduced_ixp_nodes = list()
        self.reduced_ixp_nodes_updated = False
        self.start_as = 0
        self.end_as = 0

    def __str__(self) -> str:
        return ' '.join(map(str, self.nodes))

    def append(self, as_: int, ip: str = str(), ixp: bool = False) -> None:
        if ixp:
            self.ixp_nodes.append(len(self.nodes))
        self.nodes.append((as_, ip))
        self.reduced_ixp_nodes_updated = False

    def set_start_end_asn(self, start: int = 0, end: int = 0):
        if start != 0:
            self.start_as = start
        if end != 0:
            self.end_as = end

    def get_raw_path(self) -> str:
        """Return the raw AS path as a space-separated list."""
        if len(self.nodes) == 0:
            return str()
        return ' '.join(map(str, list(zip(*self.nodes))[0]))

    def get_raw_ixp_indexes(self) -> list:
        return self.ixp_nodes

    def get_reduced_path(self, stats=None) -> (str, int):
        """Return the AS path as a space-separated list and with
        duplicate and zero AS values removed."""
        self.reduced_ixp_nodes_updated = True
        self.reduced_ixp_nodes = list()
        path = list()
        if len(self.nodes) == 0:
            stats['empty_path'] += 1
            return str(), 0
        unique_as = set()
        for idx, node in enumerate(self.nodes):
            as_, ip = node
            # Filter duplicate and zero AS values.
            if as_ in unique_as or as_ == 0:
                continue
            if idx in self.ixp_nodes:
                self.reduced_ixp_nodes.append(len(path))
            unique_as.add(as_)
            path.append(as_)
        if len(path) == 0:
            logging.debug('Empty path after filtering: {}'
                          .format(self.nodes))
            stats['empty_path'] += 1
            return str(), 0
        if self.start_as != 0 and path[0] != self.start_as:
            if stats is not None:
                stats['start_as_missing'] += 1
            logging.debug('Path does not begin with specified start AS: {}. '
                          'Prepending it now.'
                          .format(self.start_as))
            path.insert(0, self.start_as)
            # Shift IXP indexes, since we added a node at the beginning
            # of the path after the index calculation.
            self.reduced_ixp_nodes = [e + 1 for e in self.reduced_ixp_nodes]
        if self.end_as != 0 and path[-1] != self.end_as:
            if stats is not None:
                stats['end_as_missing'] += 1
            logging.debug('Path does not end with specified end AS: {}. '
                          'Appending it now.'
                          .format(self.end_as))
            path.append(self.end_as)
        return ' '.join(map(str, path)), len(path)

    def get_reduced_ixp_indexes(self) -> list:
        # Ensure that reduced path was at least calculated once.
        if not self.reduced_ixp_nodes_updated:
            self.get_reduced_path()
        return self.reduced_ixp_nodes
