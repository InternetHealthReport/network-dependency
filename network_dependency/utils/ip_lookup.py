import bz2
import configparser
import logging
import pickle
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from socket import AF_INET, AF_INET6

import radix

from network_dependency.kafka.kafka_reader import KafkaReader


@dataclass
class Prefixes:
    prefix_count: int = 0
    prefix_ip_sum: int = 0


@dataclass
class Visibility:
    ip2asn_visible: bool = False
    ip2ixp_visible: bool = False
    ip2asn_prefixes: Prefixes = field(default_factory=Prefixes)
    ip2ixp_prefixes: Prefixes = field(default_factory=Prefixes)


class IPLookup:
    """Lookup tool that combines the knowledge of ip2asn and ip2ixp.

    Provide the same interface to look up ASN info etc. from IPs, but
    try both data sources for retrieval. Use a provided offline version
    of ip2asn and construct the data for ip2ixp from the corresponding
    Kafka topic.
    """

    def __init__(self, config: configparser.ConfigParser):
        # ip2asn initialization.
        ip2asn_dir = config.get('ip2asn', 'path')
        ip2asn_db = config.get('ip2asn', 'db',
                               fallback=ip2asn_dir + '/db/latest.pickle')
        sys.path.append(ip2asn_dir)
        from ip2asn import ip2asn
        self.i2asn = ip2asn(ip2asn_db)
        self.i2asn_ipv4_asns = defaultdict(Prefixes)
        self.i2asn_ipv6_asns = defaultdict(Prefixes)
        self.__build_asn_sets_from_radix(ip2asn_db)

        # ip2ixp initialization.
        ip2ixp_ix_kafka_topic = config.get('ip2ixp', 'ix_kafka_topic',
                                           fallback='ihr_peeringdb_ix')
        ip2ixp_netixlan_kafka_topic = config.get('ip2ixp',
                                                 'netixlan_kafka_topic',
                                                 fallback=
                                                 'ihr_peeringdb_netixlan')
        ip2ixp_bootstrap_servers = config.get('ip2ixp',
                                              'kafka_bootstrap_servers',
                                              fallback=
                                              'localhost:9092')
        self.ixp_rtree = radix.Radix()
        self.__build_ixp_rtree_from_kafka(ip2ixp_ix_kafka_topic,
                                          ip2ixp_bootstrap_servers)
        self.ixp_asn_dict = dict()
        self.ixp_ipv4_asns = defaultdict(Prefixes)
        self.ixp_ipv6_asns = defaultdict(Prefixes)
        self.__fill_ixp_asn_dict_from_kafka(ip2ixp_netixlan_kafka_topic,
                                            ip2ixp_bootstrap_servers)

    def __build_asn_sets_from_radix(self, db: str):
        with bz2.open(db, 'rb') as f:
            rtree: radix.Radix = pickle.load(f)
        for node in rtree:
            if 'as' not in node.data:
                logging.warning(f'Missing "as" attribute for radix node: '
                                f'{node}')
                continue
            if node.family == AF_INET:
                address_count = 2 ** (32 - node.prefixlen)
                self.i2asn_ipv4_asns[node.data['as']].prefix_count += 1
                self.i2asn_ipv4_asns[node.data['as']].prefix_ip_sum += \
                    address_count
            elif node.family == AF_INET6:
                address_count = 2 ** (64 - node.prefixlen)
                self.i2asn_ipv6_asns[node.data['as']].prefix_count += 1
                self.i2asn_ipv6_asns[node.data['as']].prefix_ip_sum += \
                    address_count
            else:
                logging.warning(f'Unknown protocol family {node.family} for '
                                f'node {node}')

    def __build_ixp_rtree_from_kafka(self, topic: str, bootstrap_servers: str):
        reader = KafkaReader([topic], bootstrap_servers)
        with reader:
            for val in reader.read():
                if 'prefix' not in val \
                        or 'name' not in val \
                        or 'ix_id' not in val:
                    logging.warning('Received invalid entry from Kafka: {}'
                                    .format(val))
                    continue
                node = self.ixp_rtree.add(val['prefix'])
                node.data['name'] = val['name']
                node.data['id'] = val['ix_id']

    def __fill_ixp_asn_dict_from_kafka(self, topic: str,
                                       bootstrap_servers: str):
        reader = KafkaReader([topic], bootstrap_servers)
        with reader:
            for val in reader.read():
                if 'ipaddr4' not in val or 'ipaddr6' not in val \
                        or 'asn' not in val:
                    logging.warning('Received invalid entry from Kafka: {}'
                                    .format(val))
                    continue
                if val['asn'] is None:
                    continue
                asn = val['asn']
                if val['ipaddr4'] is not None:
                    if val['ipaddr4'] not in self.ixp_asn_dict:
                        self.ixp_ipv4_asns[asn].prefix_count += 1
                        self.ixp_ipv4_asns[asn].prefix_ip_sum += 1
                    elif self.ixp_asn_dict[val['ipaddr4']] != asn:
                        curr_as = self.ixp_asn_dict[val['ipaddr4']]
                        logging.debug(f'Updating AS entry for IP '
                                      f'{val["ipaddr4"]}: '
                                      f'{curr_as} -> {asn}')
                        self.ixp_ipv4_asns[curr_as].prefix_count -= 1
                        self.ixp_ipv4_asns[curr_as].prefix_ip_sum -= 1
                    self.ixp_asn_dict[val['ipaddr4']] = asn
                if val['ipaddr6'] is not None:
                    if val['ipaddr6'] not in self.ixp_asn_dict:
                        self.ixp_ipv6_asns[asn].prefix_count += 1
                        self.ixp_ipv6_asns[asn].prefix_ip_sum += 1
                    elif self.ixp_asn_dict[val['ipaddr6']] != asn:
                        curr_as = self.ixp_asn_dict[val['ipaddr6']]
                        logging.debug(f'Updating AS entry for IP '
                                      f'{val["ipaddr6"]}: '
                                      f'{curr_as} -> {val["asn"]}')
                        self.ixp_ipv6_asns[curr_as].prefix_count -= 1
                        self.ixp_ipv6_asns[curr_as].prefix_ip_sum -= 1
                    self.ixp_asn_dict[val['ipaddr6']] = asn

    def ip2asn(self, ip: str) -> int:
        """Find the ASN corresponding to the given IP address."""
        asn = self.i2asn.ip2asn(ip)
        if asn != 0:
            return asn
        if ip in self.ixp_asn_dict:
            return self.ixp_asn_dict[ip]
        return 0

    def ip2ixpname(self, ip: str) -> str:
        """Find the IXP name corresponding to the given IP address."""
        try:
            node = self.ixp_rtree.search_best(ip)
        except ValueError as e:
            logging.debug('Wrong IP address format: {} {}'.format(ip, e))
            return str()
        if node is None:
            return str()
        return node.data['name']

    def ip2ixpid(self, ip: str) -> int:
        """Find the IXP id corresponding to the given IP address.

        The IXP id corresponds to PeeringDB's ix_id."""
        try:
            node = self.ixp_rtree.search_best(ip)
        except ValueError as e:
            logging.debug('Wrong IP address format: {} {}'.format(ip, e))
            return 0
        if node is None:
            return 0
        return node.data['id']

    def ip2prefix(self, ip: str) -> str:
        """Find the IP prefix containing the given IP address."""
        prefix = self.i2asn.ip2prefix(ip)
        if prefix:
            return prefix
        try:
            node = self.ixp_rtree.search_best(ip)
        except ValueError as e:
            logging.debug('Wrong IP address format: {} {}'.format(ip, e))
            return str()
        if node is None:
            return str()
        return node.prefix

    def asn2source(self, asn: str, ip_version: int = AF_INET) -> Visibility:
        """Find the source (RIB or IXP DB) as well as number of prefixes
        and IPs for the given ASN.
        """
        ret = Visibility()
        if ip_version == AF_INET:
            if asn in self.i2asn_ipv4_asns:
                ret.ip2asn_visible = True
                ret.ip2asn_prefixes = self.i2asn_ipv4_asns[asn]
            if asn in self.ixp_ipv4_asns:
                ret.ip2ixp_visible = True
                ret.ip2ixp_prefixes = self.ixp_ipv4_asns[asn]
        elif ip_version == AF_INET6:
            if asn in self.i2asn_ipv6_asns:
                ret.ip2asn_visible = True
                ret.ip2asn_prefixes = self.i2asn_ipv6_asns[asn]
            if asn in self.ixp_ipv6_asns:
                ret.ip2ixp_visible = True
                ret.ip2ixp_prefixes = self.ixp_ipv6_asns[asn]
        else:
            logging.error(f'Invalid ip_version specified: {ip_version}')
            return Visibility()
        return ret
