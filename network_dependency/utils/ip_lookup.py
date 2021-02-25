import configparser
import logging
import radix
import sys
from network_dependency.kafka.kafka_reader import KafkaReader


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

        # ip2ipx initialization.
        ip2ixp_ix_kafka_topic = config.get('ip2ixp', 'ix_kafka_topic',
                                           fallback='ihr_peeringdb_ix')
        ip2ixp_netixlan_kafka_topic = config.get('ip2ixp',
                                                 'netixlan_kafka_topic',
                                                 fallback=
                                                 'ihr_peeringdb_netixlan')
        ip2ixp_bootstrap_servers = config.get('ip2ixp',
                                              'kafka_bootstrap_servers',
                                              fallback=
                                              'kafka1:9092,kafka2:9092,kafka3:9092')
        self.ixp_rtree = radix.Radix()
        self.__build_ixp_rtree_from_kafka(ip2ixp_ix_kafka_topic,
                                          ip2ixp_bootstrap_servers)
        self.ixp_asn_dict = dict()
        self.__fill_ixp_asn_dict_from_kafka(ip2ixp_netixlan_kafka_topic,
                                            ip2ixp_bootstrap_servers)

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
                if val['ipaddr4'] is not None:
                    self.ixp_asn_dict[val['ipaddr4']] = val['asn']
                if val['ipaddr6'] is not None:
                    self.ixp_asn_dict[val['ipaddr6']] = val['asn']

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
