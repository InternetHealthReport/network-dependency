import logging


class ASPath:
    def __init__(self):
        self.nodes = list()
        self.ixp_nodes = list()
        self.reduced_ixp_nodes = list()
        self.reduced_ixp_nodes_updated = False
        self.start_as = 0
        self.end_as = 0
        self.attributes = dict()
        self.trailing_timeouts_pruned = False

    def __str__(self) -> str:
        return ' '.join(map(str, self.nodes))

    def append(self, as_: int, ip: str = str(), ixp: bool = False) -> None:
        if ixp:
            self.ixp_nodes.append(len(self.nodes))
        self.nodes.append((as_, ip))
        self.reduced_ixp_nodes_updated = False
        self.trailing_timeouts_pruned = False

    def append_set(self,
                   as_set: tuple,
                   ip_set: tuple,
                   ixp: bool = False) -> None:
        if ixp:
            self.ixp_nodes.append(len(self.nodes))
        self.nodes.append((as_set, ip_set))
        self.reduced_ixp_nodes_updated = False
        self.trailing_timeouts_pruned = False

    def set_start_end_asn(self, start: int = 0, end: int = 0):
        if start != 0:
            self.start_as = start
        if end != 0:
            self.end_as = end

    def flag_too_many_hops(self) -> None:
        self.attributes['too_many_hops'] = True

    def has_too_many_hops(self) -> bool:
        return 'too_many_hops' in self.attributes

    def contains_ip(self, ip: str) -> bool:
        for as_, ip_ in self.nodes:
            if isinstance(ip_, tuple):
                if ip in ip_:
                    return True
            elif ip == ip_:
                return True
        return False

    def __prune_trailing_timeouts(self) -> None:
        self.trailing_timeouts_pruned = True
        cutoff = 0
        for idx, (as_, ip) in enumerate(self.nodes[::-1]):
            if isinstance(ip, tuple) and any(x != '*' for x in ip) \
                    or ip != '*':
                cutoff = idx
                break
        if cutoff > 0:
            # Remove all trailing * nodes and replace them with a single
            # * node.
            self.nodes = self.nodes[:-cutoff] + [(0, '*')]

    def get_raw_path(self) -> (str, str):
        """Return the raw AS path as a space-separated list."""
        if len(self.nodes) == 0:
            return str()
        if not self.trailing_timeouts_pruned:
            self.__prune_trailing_timeouts()
        as_path = list()
        ip_path = list()
        for as_, ip in self.nodes:
            if isinstance(as_, tuple):
                # AS Set
                as_path.append('{' + ','.join(map(str, as_)) + '}')
                ip_path.append('{' + ','.join(ip) + '}')
            else:
                as_path.append(str(as_))
                ip_path.append(ip)
        return ' '.join(as_path), ' '.join(ip_path)

    def get_raw_ixp_indexes(self) -> list:
        return self.ixp_nodes

    def get_reduced_path(self, stats=None) -> (str, str, int):
        """Return the AS path as a space-separated list and with
        duplicate and zero AS values removed."""
        self.reduced_ixp_nodes_updated = True
        self.reduced_ixp_nodes = list()
        as_path = list()
        ip_path = list()
        if len(self.nodes) == 0:
            if stats:
                stats['empty_path'] += 1
            return str(), str(), 0
        if not self.trailing_timeouts_pruned:
            self.__prune_trailing_timeouts()
        unique_as = set()
        # Used to keep track of how many hops we discarded because we
        # failed to map them. We can use this as an additional filter
        # later, since we might want to ignore paths where we dropped
        # the majority of hops.
        unknown_hops = 0
        as_set_counted = False
        for idx, (as_, ip) in enumerate(self.nodes):
            # TODO Check this
            if isinstance(as_, tuple):
                # Handle set.
                filtered_as_set = list()
                filtered_ip_set = list()
                unknown_hops_in_set = 0
                for sub_idx, entry in enumerate(as_):
                    # Filter duplicate and zero AS values.
                    if entry == 0:
                        unknown_hops_in_set += 1
                        continue
                    if entry in unique_as:
                        continue
                    filtered_as_set.append(entry)
                    filtered_ip_set.append(ip[sub_idx])
                    unique_as.add(entry)
                if len(filtered_as_set) >= 1 and idx in self.ixp_nodes:
                    self.reduced_ixp_nodes.append(len(as_path))
                if len(filtered_as_set) > 1:
                    if stats and not as_set_counted:
                        stats['as_set'] += 1
                        as_set_counted = True
                    # Still a set, add as such.
                    as_path.append(tuple(filtered_as_set))
                    ip_path.append(tuple(filtered_ip_set))
                elif len(filtered_as_set) == 1:
                    # Reduced to single AS.
                    as_path.append(filtered_as_set[0])
                    ip_path.append(filtered_ip_set[0])
                # Implicit else: No valid AS left.
                if len(as_) == unknown_hops_in_set:
                    # Set consisted entirely of unknown hops.
                    unknown_hops += 1
            else:
                # Handle normal hop.
                if as_ == 0:
                    unknown_hops += 1
                    continue
                if as_ in unique_as:
                    continue
                if idx in self.ixp_nodes:
                    self.reduced_ixp_nodes.append(len(as_path))
                unique_as.add(as_)
                as_path.append(as_)
                ip_path.append(ip)
        # How many percent of the original hops were removed due to
        # failed mapping.
        self.attributes['unknown_reduction'] = (unknown_hops / len(self.nodes)) * 100
        if len(as_path) == 0:
            logging.debug('Empty path after filtering: {}'
                          .format(self.nodes))
            if stats:
                stats['empty_path'] += 1
            return str(), str(), 0
        if self.start_as != 0 and as_path[0] != self.start_as:
            if stats:
                stats['start_as_missing'] += 1
            self.attributes['start_as_missing'] = True
            logging.debug('Path does not begin with specified start AS: {}. '
                          'Prepending it now.'
                          .format(self.start_as))
            as_path.insert(0, self.start_as)
            ip_path.insert(0, '*')
            # Shift IXP indexes, since we added a node at the beginning
            # of the path after the index calculation.
            self.reduced_ixp_nodes = [e + 1 for e in self.reduced_ixp_nodes]
        if self.end_as != 0 and as_path[-1] != self.end_as:
            if stats:
                stats['end_as_missing'] += 1
            self.attributes['end_as_missing'] = True
            logging.debug('Path does not end with specified end AS: {}. '
                          'Appending it now.'
                          .format(self.end_as))
            as_path.append(self.end_as)
            ip_path.append('*')
        # Assemble string
        as_path_str = list()
        ip_path_str = list()
        for idx, as_ in enumerate(as_path):
            if isinstance(as_, tuple):
                as_path_str.append('{' + ','.join(map(str, as_)) + '}')
                ip_path_str.append('{' + ','.join(ip_path[idx]) + '}')
            else:
                as_path_str.append(str(as_))
                ip_path_str.append(ip_path[idx])
        return ' '.join(as_path_str), ' '.join(ip_path_str), len(as_path)

    def get_reduced_ixp_indexes(self) -> list:
        # Ensure that reduced path was at least calculated once.
        if not self.reduced_ixp_nodes_updated:
            self.get_reduced_path()
        return self.reduced_ixp_nodes
