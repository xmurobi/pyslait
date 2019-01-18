import json
import re
import websocket
import time
from goto import with_goto
from websocket import ABNF
from .data import SocketMessage
from .client import Client

class StreamClient(Client):
    reconnect_seconds = 3

    def __init__(self, endpoint='http://localhost:5994/'):
        super().__init__(endpoint=endpoint)
        self._handlers = {}
        self._controls = {}

    def _connect(self):
        ws = websocket.WebSocket()
        ws.connect(re.sub('^http', 'ws', self._url('ws')))
        return ws

    @with_goto
    def runsub(self, subMessages):
        label .begin
        self._subws = self._connect()

        try:
            for s in subMessages:
                if isinstance(s, SocketMessage) and s.Action == 'subscribe':
                    self._subws.send(s.toJSON())

            while True:
                r = self._subws.recv()

                if len(r) > 0:
                    msg = self.codec.loads(r, encoding='utf-8')

                    entries = msg.get('Entries')
                    if entries is not None:
                        self._dispatch_data(msg['Topic'],msg['Partition'],msg)
                    else:
                        a = msg.get('Action')
                        if a is not None:
                            self._dispatch_ctrl(msg['Topic'], msg['Partition'],msg)

        finally:
            self._subws.close()

            if self.reconnect_seconds > 0:
                time.sleep(self.reconnect_seconds)
                goto .begin

    def runpub(self, pubMessage):
        ws = self._connect()

        try:
            if isinstance(pubMessage, SocketMessage) and pubMessage.Action == 'publish':
                ws.send(self.codec.dumps(pubMessage), opcode=ABNF.OPCODE_BINARY)

                r = ws.recv()
                msg = self.codec.loads(r, encoding='utf-8')
                if msg.get('Action') == 'ready':
                    pass

        except Exception as exc:
            ws.close()

    def _dispatch_data(self, topic, partition, msg):
        self._handlers[topic][partition](self, msg)

    def _dispatch_ctrl(self, topic, partition, msg):
        self._controls[topic][partition](self, msg)

    def onData(self, topic, partition):
        def decorator(func):
            self.register(topic, partition, func)
            return func

        return decorator
    
    def onCtrl(self, topic, partition):
        def decorator(func):
            self.register(topic, partition, func, innerhandler="_controls")
            return func

        return decorator


    def register(self, topic, partition, func, innerhandler=None):
        if isinstance(topic, str) and isinstance(partition, str):
            innerhandler = "_handlers" if not innerhandler else innerhandler
            d = getattr(self,innerhandler)
            if isinstance(d, dict):
                if topic in d:
                    d[topic][partition] = func
                else:
                    d[topic] = {}
                    d[topic][partition] = func


    def deregister(self, topic, partition, innerhandler=None):
        if isinstance(topic, str) and isinstance(partition, str):
            innerhandler = "_handlers" if not innerhandler else innerhandler
            d = getattr(self,innerhandler)
            if isinstance(d, dict):
                del d[topic][partition]


    def publish(self, publication):
        pass

