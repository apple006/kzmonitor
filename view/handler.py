# -*-coding:utf-8-*-

from base import *

class IndexHandler(BaseHandler, RequestHandler):

    def get(self):
        self.render('base.html', ktrees=self.ktrees, ztrees=self.ztrees)

    @coroutine
    def post(self):
        method = self.get_argument('method')

        if method == 'kafka':
            cluster = self.get_argument('cluster')
            topic = self.get_argument('topic', None)
            group = self.get_argument('group', None)
            startTime = self.date_to_timestamp(self.get_argument('startTime'))
            endTime = self.date_to_timestamp(self.get_argument('endTime'))
            timeDiff = endTime - startTime

            # Load data from Redis for 1 minute ago
            keepCount = self.redis.zcount('trees_kafka_keep', '-inf', '+inf')
            keepIndex = (keepCount > 1 and [-2] or [-1])[0]
            lastMinuteData = self.redis.zrange('trees_kafka_keep', keepIndex, keepIndex)

            if not lastMinuteData:
                raise Return(self.write(json.dumps({'data': False})))
    
            lastMinuteData = json.loads(lastMinuteData[0])
            currMinuteData = yield self.kafka_read_all_cluster()
    
            realtime = """
                <span class="label label-success">In Messages/m</span> {0}
                <span class="label label-primary">Out Messages/m</span> {1}
            """
    
            # Calculate in/out flow delta data
            lastFlowCount = self.kafka_flow_counter(lastMinuteData[cluster], topic, group)
            currFlowCount = self.kafka_flow_counter(currMinuteData[cluster], topic, group)
            inflow, outflow = (currFlowCount[0] - lastFlowCount[0]), (currFlowCount[1] - lastFlowCount[1])
    
            if not topic and not group:
                realtime = realtime.format(inflow, outflow)
        
            elif topic and not group:
                logsize = sum(currMinuteData[cluster][topic]['logsize'].values())
    
                realtime = """
                    <span class="label label-info">Current Logsize</span> {}
                """.format(logsize) + realtime.format(inflow, outflow)
        
            elif topic and group:
                logsize = sum(currMinuteData[cluster][topic]['logsize'].values())
                offsets = sum(currMinuteData[cluster][topic]['group'][group]['offsets'].values())
                lag = sum(currMinuteData[cluster][topic]['group'][group]['lag'].values())
    
                realtime = """
                    <span class="label label-info">Current Logsize</span> {0}
                    <span class="label label-warning">Current Offsets</span> {1}
                    <span class="label label-danger">Current Lag</span> {2}
                """.format(logsize, offsets, lag) + realtime.format(inflow, outflow)

            if timeDiff > 1800 and timeDiff <= 3600:
                interval = 2 * 60

            elif timeDiff > 3600 and timeDiff <= 3 * 3600:
                interval = 10 * 60

            elif timeDiff > 3 * 3600 and timeDiff <= 12 * 3600:
                interval = 20 * 60

            elif timeDiff > 12 * 3600 and timeDiff <= 86400:
                interval = 3600

            elif timeDiff > 86400 and timeDiff <= 3 * 86400:
                interval = 2 * 3600

            else:
                interval = 6 * 3600

            if timeDiff <= 1800:
                rangeData = self.redis.zrangebyscore(
                    'trees_kafka_keep',
                    startTime,
                    endTime,
                    withscores = True,
                    score_cast_func = int
                )

                highchartData = self.kafka_highcharts_maker(rangeData, cluster, topic, group)

            else:
                avgData = []

                while startTime <= endTime:
                   #Load data from Redis for specified time range
                    rangeData = self.redis.zrangebyscore(
                        'trees_kafka_keep',
                        startTime, 
                        startTime + interval, 
                        withscores = True,
                        score_cast_func = int
                    )
                    if rangeData:
                        avgData.append(self.get_average_data(rangeData))

                    startTime += interval

                highchartData = self.kafka_highcharts_maker(avgData, cluster, topic, group)

            if highchartData:
                highchartData.update({'realtime': realtime})

            else:
                highchartData = {'realtime': realtime, 'logsize': [], 'offsets': [], 'lag': []}

            self.write(json.dumps({'data': highchartData}))

        if method == 'zookeeper':
            metric = self.get_argument('metric')
            server = self.get_argument('server')
    
            with Zookeeper(server) as client:
    
                if metric == 'getStat':
                    # Get client/server statistics data
    
                    mntr = yield client.get_mntr()
                    cons = yield client.get_cons()
                    self.write(json.dumps({'cons': cons, 'mntr': mntr}))
    
                if metric == 'getNode':
                    # Get a tree from zookeeper path.
    
                    path = self.get_argument('path')
                    path = os.path.join('/', path.lstrip('/'))
                    children = yield client.get_children(path)
                    trees = []
            
                    for child in children:
                        _, stat = yield client.get(os.path.join(path, child))
            
                        trees.append({
                            'name': child,
                            'path': os.path.join(path, child),
                            'leaf': (stat['numChildren'] and [True] or [False])[0]
                        })
    
                    self.write(json.dumps(sorted(trees, key=lambda x:x['name'])))
    
                if metric == 'getData':
                    # Get a zk node stat and data
                    path = self.get_argument('path')
                    data, stat = yield client.get(path)
                    if type(data) == str:
                        try:
                            data = data.decode('utf8')
                        except UnicodeDecodeError as err:
                            data = 'can not recognized'
                    self.write(json.dumps({'data': data, 'stat': stat}))

class LagApi(BaseHandler, RequestHandler):
    
    @coroutine
    def get(self):
        """ 
            Get Logsize Offsets and Lag

            if total parameter is provided
            return logsize=VALUE offsets=VALUE lag=VALUE

            otherwise
            return the original json data
        """

        cluster = self.get_argument('cluster')
        topic = self.get_argument('topic')
        group = self.get_argument('group')
        total = self.get_argument('total', None)

        currMinuteData = yield self.kafka_read_all_cluster()

        if cluster not in currMinuteData.keys():
            raise Return(self.write('Invalid cluster %s' % cluster))

        if topic not in currMinuteData[cluster].keys():
            raise Return(self.write('Topic %s is not exist' % topic))

        if group not in currMinuteData[cluster][topic]['group'].keys():
            raise Return(self.write('Group %s is not exist' % group))

        if total:
            logsize = sum(currMinuteData[cluster][topic]['logsize'].values())
            offsets = sum(currMinuteData[cluster][topic]['group'][group]['offsets'].values())
            lag = sum(currMinuteData[cluster][topic]['group'][group]['lag'].values())
            data = 'logsize=%s offsets=%s lag=%s' % (logsize, offsets, lag)
        else:
            data = currMinuteData[cluster][topic]['group'][group]
            data.update({'logsize': currMinuteData[cluster][topic]['logsize']})
            data = json.dumps(data)

        raise Return(self.write(data))
