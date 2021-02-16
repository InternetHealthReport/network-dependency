from collections import defaultdict
import logging


class ScopeStatistic:
    def __init__(self, as_: int):
        self.as_ = as_
        self.total_peer_count = 0
        self.peer_count = defaultdict(int)
        self.path_as_count = defaultdict(int)

    def add_path(self, path: str):
        hops = list(map(int, path.split()))
        if hops[-1] != self.as_:
            logging.warning('Path {} does not end in expected AS {}'
                            .format(path, self.as_))
            hops.append(self.as_)
        self.peer_count[hops[0]] += 1
        self.total_peer_count += 1
        for i in range(len(hops) - 1):
            self.path_as_count[hops[i]] += 1

    def peer_distribution(self) -> str:
        p = 100 / self.total_peer_count
        for peer in sorted(self.peer_count.keys()):
            yield f'{peer} {self.peer_count[peer]} {p * self.peer_count[peer]}\n'

    def path_distribution(self) -> str:
        p = 100 / self.total_peer_count
        for as_, count in sorted(self.path_as_count.items(), key=lambda k: k[1], reverse=True):
            yield f'{as_} {count} {p * count}\n'

    def __str__(self):
        ret = [str(self.as_),
               f'      Traceroutes: {self.total_peer_count}',
               f'  Unique peer ASs: {len(self.peer_count)}',
               '  Peer distribution']
        p = 100 / self.total_peer_count
        for peer in sorted(self.peer_count.keys()):
            ret.append(f'    {peer:6d} {self.peer_count[peer]:6d} ' +
                       f'{p * self.peer_count[peer]:9.4f}%')
        ret.append('  Path AS distribution')
        for as_, count in sorted(self.path_as_count.items(), key=lambda k: k[1], reverse=True):
            ret.append(f'    {as_:6d} {count:6d} {p * count: 9.4f}%')
        return '\n'.join(ret)

