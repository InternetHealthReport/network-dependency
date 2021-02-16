import logging


class ASPath:
    def __init__(self):
        self.nodes = list()
        self.start_as = 0
        self.end_as = 0

    def __str__(self) -> str:
        return ' '.join(map(str, self.nodes))

    def append(self, as_: int, ip: str = str()) -> None:
        self.nodes.append((as_, ip))

    def set_start_end_asn(self, start: int = 0, end: int = 0):
        if start != 0:
            self.start_as = start
        if end != 0:
            self.end_as = end

    def get_reduced_path(self, stats=None) -> (str, int):
        """Return the AS path as a space-separated list and with
        duplicate and zero AS values removed."""
        path = list()
        if len(self.nodes) == 0:
            return str(), 0
        unique_as = set()
        for as_, ip in self.nodes:
            # Filter duplicate and zero AS values.
            if as_ in unique_as or as_ == 0:
                continue
            unique_as.add(as_)
            path.append(as_)
        if len(path) == 0:
            logging.warning('Empty path after filtering: {}'
                            .format(self.nodes))
            return str(), 0
        if self.start_as != 0 and path[0] != self.start_as:
            if stats is not None:
                stats['start_as_missing'] += 1
            logging.debug('Path does not begin with specified start AS: {}. Prepending it now.'
                         .format(self.start_as))
            path.insert(0, self.start_as)
        if self.end_as != 0 and path[-1] != self.end_as:
            if stats is not None:
                stats['end_as_missing'] += 1
            logging.debug('Path does not end with specified end AS: {}. Appending it now.'
                         .format(self.end_as))
            path.append(self.end_as)
        return ' '.join(map(str, path)), len(path)
