# -*-coding:utf-8-*-

import os
import json
import time
import yaml

from tornado.web import RequestHandler
from tornado.gen import coroutine, Return
from collections import Counter
from ConfigParser import ConfigParser
from module.reader import Kafka, Zookeeper
from redis import ConnectionPool, StrictRedis

class BaseHandler(object):
    basedir = os.path.dirname(os.path.abspath(__file__))

    with open(os.path.join(basedir, '../etc/kafka.yaml')) as kf, \
         open(os.path.join(basedir, '../etc/zookeeper.yaml')) as zf:
        ktrees = yaml.load(kf)
        ztrees = yaml.load(zf)

    cf = ConfigParser()
    cf.read(os.path.join(basedir, '../etc/trees.conf'))

    rpool = ConnectionPool(
        host = cf.get('redis', 'host'), 
        port = cf.getint('redis', 'port'), 
        db = cf.getint('redis', 'db'),
        password = cf.get('redis', 'pass')
    )
    redis = StrictRedis(connection_pool=rpool)

    def date_to_timestamp(self, datetime, formatter='%Y-%m-%d %H:%M:%S'):
        fmtTime = time.strptime(datetime, formatter)
        timeTs = time.mktime(fmtTime)
        return int(timeTs)

    @coroutine
    def kafka_read_all_cluster(self):
        """ Fetch all the cluster Logsize Offsets and Lag """

        data = self.redis.get('trees_kafka_cache')

        if not data:
            clusters = self.ktrees.keys()
            data = {}

            for cluster in clusters:
                data[cluster] = {}
                topics = self.ktrees[cluster]['topic'].keys()
                reader = Kafka(self.ktrees[cluster]['server'])

                for topic in topics:
                    data[cluster][topic] = {}
                    groups = self.ktrees[cluster]['topic'][topic].strip().split(',')
                    partitions = yield reader.get_topic_partitions(topic)
                    logsize = yield reader.get_logsize(topic, partitions)
                    data[cluster][topic]['logsize'] = logsize
                    data[cluster][topic]['group'] = {}

                    for group in groups:
                        group = group.strip()
                        offsets = yield reader.get_offsets(topic, partitions, group)
                        lag = {}
                        
                        for partition in partitions:
                            value = logsize[partition] - offsets[partition]
                            lag[partition] = (value >= 0 and [value] or [0])[0]

                        data[cluster][topic]['group'][group] = {'offsets': offsets, 'lag': lag}

            self.redis.setex('trees_kafka_cache', self.cf.getint('kafka', 'cache_seconds'), json.dumps(data))

        else:
            data = json.loads(data)

        raise Return(data)

    def kafka_flow_counter(self, data, topicName=None, groupName=None):
        """ Return in/out messages flow count """

        inflow, outflow = 0, 0

        for topic, topicMetrics in data.iteritems():

            # Cluster level count
            if not topicName and not groupName:
                inflow += sum(data[topic]['logsize'].values())

                for group, groupMetrics in topicMetrics['group'].iteritems():
                    outflow += sum(groupMetrics['offsets'].values())

            # Topic and Group level count
            if topicName:

                if topicName == topic:
                    inflow += sum(data[topic]['logsize'].values())

                    for group, groupMetrics in topicMetrics['group'].iteritems():

                        if not groupName:
                            # topic out flow count
                            outflow += sum(groupMetrics['offsets'].values())

                        elif groupName == group:
                            # group out flow count
                            outflow = sum(data[topic]['group'][group]['offsets'].values())
                    break

        return(inflow, outflow)

    def kafka_highcharts_format(self, data, chartList):
        """ Format the given data for highcharts  """

        for _, metric in data.iteritems():
            chartList.append(metric)

    def kafka_highcharts_maker(self, rangedata, cluster, topicName=None, groupName=None):
        """ Return the highcharts data """

        logsizeList, offsetsList, lagList = [], [], [] 
        logsizeData, offsetsData, lagData = {}, {}, {}

        for data in rangedata:
            try:
                metrics = json.loads(data[0])[cluster]
            except KeyError as err:
                continue
                
            timestamp = data[1] * 1000
    
            for topic, _ in metrics.iteritems():
    
                # Make cluster level data
                if not topicName and not groupName:
                    if topic not in logsizeData.keys():
                        logsizeData[topic] = {'name': topic, 'data': []}
    
                    logsizeData[topic]['data'].append(
                        [ timestamp, sum(metrics[topic]['logsize'].values()) ]
                    )
    
                # Make topic level data
                if topicName and not groupName:
                    logsize = metrics[topicName]['logsize']
    
                    # Get logsize for each partition
                    for partition, value in logsize.iteritems():
                        uniqKey = "%s_%s" % (topicName, partition)
    
                        if uniqKey not in logsizeData.keys():
                            logsizeData[uniqKey] = {'name': '%02s' % partition, 'data': []}
    
                        logsizeData[uniqKey]['data'].append([timestamp, value])
    
                # Make group level data
                if topicName and groupName:
                    try:
                        groupData = metrics[topicName]['group'][groupName]
                    except KeyError as err:
                        continue
                    offsets = groupData['offsets']
                    lag = groupData['lag']
    
                    partitions = offsets.keys()
                    
                    for partition in partitions:
                        uniqKey = "%s_%s" % (topicName, partition)
    
                        # Get offsets for each partition
                        if uniqKey not in offsetsData.keys():
                            offsetsData[uniqKey] = {'name': '%02s' % partition, 'data': []}
    
                        offsetsData[uniqKey]['data'].append(
                            [ timestamp, offsets[partition] ]
                        )
    
                        # Get lag for each partition
                        if uniqKey not in lagData.keys():
                            lagData[uniqKey] = {'name': '%02s' % partition, 'data': []}

                        try:
                            lagData[uniqKey]['data'].append(
                                [ timestamp, lag[partition] ]
                            )
                        except KeyError as err:
                            pass
    
        if logsizeData:
            self.kafka_highcharts_format(logsizeData, logsizeList)
            logsizeList = sorted(logsizeList, key=lambda x:x['name'])

        if offsetsData:
            self.kafka_highcharts_format(offsetsData, offsetsList)
            offsetsList = sorted(offsetsList, key=lambda x:x['name'])

        if lagData:
            self.kafka_highcharts_format(lagData, lagList)
            lagList = sorted(lagList, key=lambda x:x['name'])

        return {
            'logsize': logsizeList, 
            'offsets': offsetsList, 
            'lag': lagList
        }

    def get_average_data(self, rangedata):
        """ Calculate average data """

        avgResult = {}

        for data in rangedata:
            metrics = json.loads(data[0])
            timestamp = data[1]

            for cluster, clusterData in metrics.iteritems():
                if cluster not in avgResult.keys():
                    avgResult[cluster] = {}

                for topicName, topicData in clusterData.iteritems():
                    if topicName not in avgResult[cluster].keys():
                        avgResult[cluster][topicName] = {}

                    for item, itemData in topicData.iteritems():
                        if item not in avgResult[cluster][topicName].keys():
                            avgResult[cluster][topicName][item] = {}

                        if item == 'logsize':
                            for partition, partitionData in itemData.iteritems():
                                if partition not in avgResult[cluster][topicName][item].keys():
                                    avgResult[cluster][topicName][item][partition] = 0

                            avgResult[cluster][topicName][item] = dict(
                                Counter(avgResult[cluster][topicName][item]) + \
                                Counter(itemData)
                            )
                        else:
                            for group, groupData in itemData.iteritems():
                                if group not in avgResult[cluster][topicName][item].keys():
                                    avgResult[cluster][topicName][item][group] = {}

                                for olKey, olData in groupData.iteritems():
                                    if olKey not in avgResult[cluster][topicName][item][group].keys():
                                        avgResult[cluster][topicName][item][group][olKey] = {}

                                    for partition, partitionData in olData.iteritems():
                                        if partition not in avgResult[cluster][topicName][item][group][olKey].keys():
                                            avgResult[cluster][topicName][item][group][olKey][partition] = 0

                                    avgResult[cluster][topicName][item][group][olKey] = dict(
                                        Counter(avgResult[cluster][topicName][item][group][olKey]) + \
                                        Counter(olData)
                                    )

        for cluster, clusterData in avgResult.iteritems():

            for topic, topicData in clusterData.iteritems():

                for item, itemData in topicData.iteritems():

                    if item == 'logsize':
                        for partition, value in itemData.iteritems():
                            avgResult[cluster][topic][item][partition] = value / len(rangedata)

                    else:
                        for group, groupData in itemData.iteritems():

                            for olKey, olData in groupData.iteritems():

                                for partition, value in olData.iteritems():
                                    avgResult[cluster][topic][item][group][olKey][partition] = value / len(rangedata)

        avgTimestamp = sum([x[1] for x in rangedata]) / len(rangedata)
        return (json.dumps(avgResult), avgTimestamp)

    @coroutine
    def writer(self):
        """ Persist cluster data in Redis """

        data = yield self.kafka_read_all_cluster()
        currentTimestamp = int(time.time())

        if data:
            self.redis.zadd('trees_kafka_keep', currentTimestamp, json.dumps(data))

        # Clean expired data
        expireData = self.redis.zrangebyscore(
            'trees_kafka_keep', 
            '-inf', 
            currentTimestamp - (self.cf.getint('kafka', 'keep_day') * 86400),
            withscores = True, 
            score_cast_func = int
        )
        for data in expireData:
            self.redis.zrem('trees_kafka_keep', data[0])

