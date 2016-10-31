# -*-coding:utf-8-*-

import os, sys
from tornado import web, ioloop, httpserver

basedir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../')
sys.path.insert(0, basedir)

from view.handler import IndexHandler, LagApi

app = web.Application(
    [
        (r'/', IndexHandler),
        (r'/topic/lag', LagApi)
    ],
    debug = True,
    static_path = os.path.join(basedir, 'static'),
    template_path = os.path.join(basedir, 'template')
)

from view.base import BaseHandler
# Auto load data for 1 minute
persist = BaseHandler()
ioloop.PeriodicCallback(persist.writer, 60 * 1000).start()

server = httpserver.HTTPServer(app)
server.listen(sys.argv[1])
ioloop.IOLoop.instance().start()