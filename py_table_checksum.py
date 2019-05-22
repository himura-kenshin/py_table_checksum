# -*- coding: utf-8 -*-  
from tornado.web import RequestHandler, Application
from tornado.ioloop import IOLoop
import tornado.options
from tornado.httpserver import HTTPServer
from crypt import Crypt
from hashlib import sha1

import sys
import json
import time
import logging
import signal
import datetime

import checksum


"""
http://127.0.0.1:8868/tablechecksum?mhost=192.168.1.141&mport=3306&muser=dev&mpasswd=a91abe3f3a90adb3473c8abb255db8fd&mdb=testdb&shost=192.168.1.141&sport=3306&suser=dev&spasswd=a91abe3f3a90adb3473c8abb255db8fd

"""


def get_clear_password(host, user, password):
    key = host + user
    s1 = sha1()
    s1.update(key.encode())
    key = s1.digest()
    return Crypt.decrypt(password, key[:16])

def sig_handler(sig, frame):
    logging.warning('Caught signal: %s', sig)
    tornado.ioloop.IOLoop.instance().add_callback(shutdown)

def shutdown():
    logging.info('Stopping http server')
    http_server.stop()  # 不接收新的 HTTP 请求

    logging.info('Will shutdown in %s seconds ...', 5)
    io_loop = tornado.ioloop.IOLoop.instance()

    deadline = time.time() + 5

    def stop_loop():
        now = time.time()
        if now < deadline:
            io_loop.add_timeout(now + 1, stop_loop)
        else:
            io_loop.stop()  # 处理完现有的 callback 和 timeout 后，可以跳出 io_loop.start() 里的循环
            logging.info('Shutdown')

    stop_loop()

class DateEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(obj, datetime.date):
            return obj.strftime("%Y-%m-%d")
        else:
            return json.JSONEncoder.default(self, obj)


class TableChecksumHandler(RequestHandler):
    def get(self):
        dic = dict(
        m_host = self.get_argument('mhost'),
        m_port = int(self.get_argument('mport')),
        m_user = self.get_argument('muser'),

        m_password = get_clear_password(self.get_argument('mhost'), self.get_argument('muser'),
                                                      self.get_argument('mpasswd')),
        m_db = self.get_argument('mdb'),
        s_host = self.get_argument('shost'),
        s_port = int(self.get_argument('sport')),
        s_user = self.get_argument('suser'),
        s_password = get_clear_password(self.get_argument('shost'), self.get_argument('suser'),
                                        self.get_argument('spasswd'))
        )

        check, diffs = checksum.do(dic)


        result = {
            'res': 1,
            'data': {
                'check': check,
                'diffs': diffs
            }
        }


        self.write(json.dumps(result,cls=DateEncoder))



if __name__ == '__main__':

    tornado.options.define('port', default=8871, type=int, help="this is the port >for application")

    app = Application(
        [

            (r"/tablechecksum", TableChecksumHandler),
        ]
    )

    if sys.platform == 'win32':
        app.listen(8868)
        tornado.ioloop.IOLoop.instance().start()
    else:

        tornado.options.parse_command_line()
        http_server = HTTPServer(app)
        http_server.bind(tornado.options.options.port)

        signal.signal(signal.SIGTERM, sig_handler)
        signal.signal(signal.SIGINT, sig_handler)

        http_server.start(1)
        IOLoop.current().start()

        logging.info("Exit...")

