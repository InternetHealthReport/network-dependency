import logging
from typing import Tuple


class ASPath:
    def __init__(self):
        self.nodes = list()
        self.ixp_nodes = list()
        self.reduced_ixp_nodes = list()
        self.reduced_ixp_nodes_updated = False
        self.start_as = 'as|0'
        self.end_as = 'as|0'
        self.attributes = dict()
        self.trailing_timeouts_pruned = False

    def __str__(self) -> str:
        return ' '.join(map(str, self.nodes))

    def append(self, as_: str, ip: str = str(), ixp: bool = False) -> None:
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

    def set_start_end_asn(self, start: str = '0', end: str = '0'):
        if start != '0':
            self.start_as = f'as|{start}'
        if end != '0':
            self.end_as = f'as|{end}'

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

    def mark_hop_error(self, error: str) -> None:
        if 'errors' not in self.attributes:
            self.attributes['errors'] = list()
        self.attributes['errors'].append((len(self.nodes), error))

    def has_errors(self) -> bool:
        return 'errors' in self.attributes

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
            self.nodes = self.nodes[:-cutoff] + [('as|0', '*')]

    @staticmethod
    def __check_as_in_node(asn: str, node) -> Tuple[bool, bool]:
        """Check if the specified asn is in the specified node.

        Helper function that handles singleton/set nodes.
        Returns a tuple of booleans that indicate if there was an
        AS in the node and if this AS was the specified one."""
        as_node = False
        as_in_node = False
        if isinstance(node, tuple):
            # Set
            for subnode in node:
                if not subnode.startswith('as|'):
                    continue
                # There is at least one AS in the set.
                as_node = True
                if subnode == asn:
                    # Abort early since node was found.
                    as_in_node = True
                    break
        else:
            # Singleton
            if not node.startswith('as|'):
                return as_node, as_in_node
            as_node = True
            if node == asn:
                as_in_node = True
        return as_node, as_in_node

    def __check_start_as(self, as_path: list) -> bool:
        """Return True if self.start_as is set and the first AS node
        in the given AS path is different."""
        if self.start_as == 'as|0':
            return False
        for node in as_path:
            as_node, as_in_node = self.__check_as_in_node(self.start_as, node)
            if not as_node:
                # Skip leading non-AS nodes.
                continue
            # First AS node.
            if not as_in_node:
                return True
            # No need to continue after first AS node.
            break
        return False

    def __check_end_as(self, as_path: list) -> bool:
        """Return True if self.start_as is set and the first AS node
        in the given AS path is different."""
        if self.end_as == 'as|0':
            return False
        for node in as_path[::-1]:
            as_node, as_in_node = self.__check_as_in_node(self.end_as, node)
            if not as_node:
                # Skip trailing non-AS nodes.
                continue
            # Last AS node.
            if not as_in_node:
                return True
            # No need to continue after first AS node.
            break
        return False

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
                as_path.append('{' + ','.join(as_) + '}')
                ip_path.append('{' + ','.join(ip) + '}')
            else:
                as_path.append(as_)
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
            if isinstance(as_, tuple):
                # Handle set.
                filtered_as_set = list()
                filtered_ip_set = list()
                unknown_hops_in_set = 0
                for sub_idx, entry in enumerate(as_):
                    # Filter duplicate and zero AS values.
                    if entry == 'as|0':
                        unknown_hops_in_set += 1
                        continue
                    # Only remove duplicate AS nodes, i.e., keep IXP
                    # nodes, etc.
                    if entry.startswith('as|') and entry in unique_as:
                        continue
                    filtered_as_set.append(entry)
                    filtered_ip_set.append(ip[sub_idx])
                    unique_as.add(entry)
                if len(filtered_as_set) >= 1 and idx in self.ixp_nodes:
                    # IXP in possible set, mark as such.
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
                if as_ == 'as|0':
                    unknown_hops += 1
                    continue
                if as_.startswith('as|') and as_ in unique_as:
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
            logging.debug(f'Empty path after filtering: {self.nodes}')
            if stats:
                stats['empty_path'] += 1
            return str(), str(), 0
        if self.__check_start_as(as_path):
            # The path does not begin with the specified start AS.
            if self.start_as in unique_as:
                # The specified start AS is somewhere in the path,
                # but not the beginning. Best to ignore.
                if stats:
                    stats['start_as_in_path'] += 1
                return str(), str(), 0
            if stats:
                stats['start_as_missing'] += 1
            self.attributes['start_as_missing'] = True
            logging.debug(f'Path does not begin with specified start AS: {self.start_as}. '
                          'Prepending it now.')
            as_path.insert(0, self.start_as)
            ip_path.insert(0, '*')
            # Shift IXP indexes, since we added a node at the beginning
            # of the path after the index calculation.
            self.reduced_ixp_nodes = [e + 1 for e in self.reduced_ixp_nodes]
        if self.__check_end_as(as_path):
            # The path does not end with the specified end AS.
            if self.end_as in unique_as:
                # The specified end AS is somewhere in the path,
                # but not the end. Best to ignore.
                if stats:
                    stats['end_as_in_path'] += 1
                return str(), str(), 0
            if stats:
                stats['end_as_missing'] += 1
            self.attributes['end_as_missing'] = True
            logging.debug(f'Path does not end with specified end AS: {self.end_as}. Appending it '
                          f'now.')
            as_path.append(self.end_as)
            ip_path.append('*')
        # Assemble string
        as_path_str = list()
        ip_path_str = list()
        for idx, as_ in enumerate(as_path):
            if isinstance(as_, tuple):
                as_path_str.append('{' + ','.join(as_) + '}')
                ip_path_str.append('{' + ','.join(ip_path[idx]) + '}')
            else:
                as_path_str.append(as_)
                ip_path_str.append(ip_path[idx])
        return ' '.join(as_path_str), ' '.join(ip_path_str), len(as_path)

    def get_reduced_ixp_indexes(self) -> list:
        # Ensure that reduced path was at least calculated once.
        if not self.reduced_ixp_nodes_updated:
            self.get_reduced_path()
        return self.reduced_ixp_nodes
