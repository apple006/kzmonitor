# -*-coding:utf-8-*-

import json, re

from kafka import KafkaClient, KafkaConsumer
from kafka.errors import UnknownTopicOrPartitionError
from kafka.common import TopicPartition, OffsetRequestPayload
from kazoo.client import KazooClient
from kazoo.exceptions import NoNodeError
from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor

class Kafka(object):
    executor = ThreadPoolExecutor(20)

    def __init__(self, brokers, version):
        self.brokers = brokers
        self.version = version
        self.client = KafkaClient(brokers, timeout=5)

    def payload(self, topic, partitions):
        """ Return kafka topic payloads """

        return [ OffsetRequestPayload(topic, p, -1, 1) for p in partitions ]

    @run_on_executor
    def get_topic_partitions(self, topic):
        """ Return all partitions from the specified kafka topic """

        return self.client.get_partition_ids_for_topic(topic)

    @run_on_executor
    def get_logsize(self, topic, partitions):
        """ Fetch all partitions logsize from the specified kafka topic """
        
        tp = self.client.send_offset_request(self.payload(topic, partitions))
        logsize = {}

        for p in tp:
            logsize[p.partition] = p.offsets[0]

        return logsize

    @run_on_executor
    def get_offsets(self, topic, partitions, group):
        """ Fetch all partitions offsets from the specified kafka topic """

        offsets = {}
        
        try:
            # Call uses the v0 zookeeper-storage apis
            tp = self.client.send_offset_fetch_request(group, self.payload(topic, partitions))

            for p in tp:
                offsets[p.partition] = p.offset

        except UnknownTopicOrPartitionError as err:
            
            # Call uses the v1 kafka-storage apis
            consumer = KafkaConsumer(
                group_id = group,
                bootstrap_servers = self.brokers,
                enable_auto_commit = False,
                api_version = self.version
            )
            tp = [ TopicPartition(topic, p) for p in partitions ]
            consumer.assign(tp)

            for p in tp:
                offsets[p.partition] = consumer.position(p)

        return offsets

class Zookeeper(object):
    executor = ThreadPoolExecutor(20)

    def __init__(self, server):
        """ Initiate connection to ZK. """

        self.client = KazooClient(hosts=server, read_only=True)
        self.client.start()

    def __exit__(self, type, value, traceback):
        """
            Gracefully stop this Zookeeper session.
            Free any resources held by the client
        """

        self.client.stop()
        self.client.close()

    def __enter__(self):
        return self

    @run_on_executor
    def get_cons(self):
        """ 
            List full connection/session details for all clients connected to this server.
            Includes session id, information on numbers of packets received/sent, operation latencies, last operation performed
            ...
        """

        name = ('ip', 'port', 'recved', 'sent', 'sid', 'est', 'lresp', 'avglat', 'maxlat')
        data = []

        for x in self.client.command(cmd=b'cons').split('\n'):
            matched = re.findall(r'/(.+):(\d+).+recved=(\d+).+sent=(\d+).+sid=(\w+).+est=(\d+).+lresp=(\d+).+avglat=(\d+).+maxlat=(\d+)', x)
            
            if matched:
                data.append(dict(zip(name, matched[0])))

        return data

    @run_on_executor
    def get_mntr(self):
        """ Outputs a list of variables that could be used for monitoring the health of the brokers. """

        data = dict(
            item.split('\t', 1)
            for item in self.client.command(b'mntr').split('\n')[:-1]
        )
        data.update({'alive': (self.client.command(cmd=b'ruok')=='imok' and [True] or [False])[0]})
        return data

    @run_on_executor
    def get(self, path):
        """ Get the value of a node. """
        
        data, stat = self.client.get(path)
        return data, json.loads(json.dumps(stat._asdict()))

    @run_on_executor
    def get_children(self, path):
        """ Get a list of child nodes of a path. """

        try:
            children = map(str, self.client.get_children(path))
        except NoNodeError as e:
            return []
        return children
