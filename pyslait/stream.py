import json
import re
import websocket
import time
import logging
from goto import with_goto
from .data import SocketMessage
from .client import Client

logger = logging.getLogger(__name__)
logger.propagate = True

class Connection(object):

    def __init__(self, ws):
        self.ws = None
        self.reconnect_counts = 0
        self.reset(ws)

    def reset(self, ws):
        if self.ws:
            del self.ws
        self.ws = ws

    def retry(self, ws):
        self.reset(ws)
        self.reconnect_counts += 1

class StreamClient(Client):
    # try to reconnect every 3 seconds
    reconnect_seconds = 3
    # < 0 means reconnect forever, > 0 means 
    # action of reconnect has sup 
    max_reconnection_counts = -1    

    def __init__(self, endpoint='http://localhost:5994/', reconnection_seconds=3, max_reconnection_counts=-1):
        super().__init__(endpoint=endpoint)
        self._handlers = {}
        self._controls = {}
        self._connctions = {}
        self._publisher_ready = False
        self.reconnect_seconds = reconnection_seconds
        self.max_reconnection_counts = max_reconnection_counts



        self.pub_reconnect_counts = 0
        self.sub_reconnect_counts = 0
        self._subws = None
        self._pubws = None

    def _run_websocket(self, name, on_data=None, on_open=None, on_close=None):

        def _on_reconnect(ws, error):
            while True:
                if self._connctions.get(name):
                    logger.debug('Inside retry %s', self._connctions[name].reconnect_counts)

                try:
                    host = re.sub('^http', 'ws', self._url('ws'))
                    ws = websocket.WebSocketApp(host, 
                        on_open=on_open, 
                        on_data=on_data, 
                        on_close=on_close,
                        on_error=_on_reconnect) 

                    if ws is not None:
                        logger.info('WSConnected[%s] to hostname: %s' , name, host)
                        if not self._connctions.get(name):
                            self._connctions[name] = Connection(ws)
                        else:
                            self._connctions[name].retry(ws)

                        ws.run_forever()
                        return True

                except websocket.WebSocketException as e:
                    logger.error("WebSocketException: Failed to recreat connection to hos, please ensure network connection to host: %s, exc: %s", host, e)
                    return False
                except websocket.WebSocketConnectionClosedException as e:
                    logger.error("WebSocketConnectionClosedException:Failed to recreat connection to hos, please ensure network connection to host: %s, exc: %s", host, e)
                    return False
                except websocket.WebSocketTimeoutException as e:
                    logger.error("WebSocketTimeoutException: Failed to recreat connection to hos, please ensure network connection to host: %s, exc: %s", host, e)
                    return False
                except Exception as e:
                    logger.error("Exception: Failed to recreat connection to hos, please ensure network connection to host: %s, exc: %s", host, e)
                    return False

        while not _on_reconnect(None, None):
            if self.reconnect_seconds > 0:
                if self.max_reconnection_counts > 0 and self._connctions.get(name) and self._connctions[name].reconnect_counts >= self.max_reconnection_counts:
                    return
                
                time.sleep(self.reconnect_seconds)
            else:
                return


    def runsub(self, subMessages):

        def _on_data(ws, data, type, flag):
            if len(data) > 0:
                msg = self.codec.loads(data, encoding='utf-8')

                topic = msg.get('Topic')
                partition = msg.get('Partition')
                entries = msg.get('Entries')
                action = msg.get('Action')

                if topic is not None and partition is not None and entries is not None:
                    self._dispatch_data(topic, partition, msg)
                elif action is not None:
                    self._dispatch_ctrl(topic, partition, msg)

        def _on_open(ws):
            # Book topic/partitions
            for s in subMessages:
                if isinstance(s, SocketMessage) and s.Action == 'subscribe':
                    ws.send(s.toJSON())

        def _on_close(ws):
            self._dispatch_ctrl('__sub__','__all__','closed')

        self._run_websocket('__runsub__',on_data=_on_data, on_close=_on_close, on_open=_on_open)

        # try:
        #     if self._subws is None:
        #         self._subws = websocket.WebSocket()
            
        #     # connect to slait
        #     self._subws.connect(re.sub('^http', 'ws', self._url('ws')))

        #     # send message to book topic/partitions
        #     for s in subMessages:
        #         if isinstance(s, SocketMessage) and s.Action == 'subscribe':
        #             self._subws.send(s.toJSON())

        #     # loop to receive messages
        #     while True:
        #         r = self._subws.recv()

        #         try:
        #             if len(r) > 0:
        #                 msg = self.codec.loads(r, encoding='utf-8')

        #                 topic = msg.get('Topic')
        #                 partition = msg.get('Partition')
        #                 entries = msg.get('Entries')
        #                 action = msg.get('Action')

        #                 if topic is not None and partition is not None and entries is not None:
        #                     self._dispatch_data(topic, partition, msg)
        #                 elif action is not None:
        #                     self._dispatch_ctrl(topic, partition, msg)

        #         except Exception as exc:
        #             continue

        # finally:
        #     if self._subws is not None:
        #         self._subws.close()

        #     # reconnect or finish
        #     if self.reconnect_seconds > 0:
        #         time.sleep(self.reconnect_seconds)
        #         self.sub_reconnect_counts += 1

        #         if self.max_reconnection_counts > 0 and self.sub_reconnect_counts >= self.max_reconnection_counts:
        #             return


    @with_goto
    def runpub(self, msg):
        label .begin
        self._pubws = self._connect()

        try:
            if isinstance(msg, SocketMessage) and msg.Action == 'publish':                    
                self._pubws.send(msg.toJSON())
            else:
                return

            while True:
                r = self._pubws.recv()

                try:
                    if len(r) > 0:
                        msg = self.codec.loads(r, encoding='utf-8')

                        if msg.get('Action') == 'ready':
                            self._publisher_ready = True
                    elif not self._pubws.connected:
                        self._publisher_ready = False
                        break

                except Exception as exc:
                    # logger.exception(exc)
                    continue

        finally:
            self._pubws.close()

            # reconnect or finish
            if self.reconnect_seconds > 0:
                time.sleep(self.reconnect_seconds)
                self.pub_reconnect_counts += 1

                if self.max_reconnection_counts > 0 and self.pub_reconnect_counts >= self.max_reconnection_counts:
                    return

                goto .begin

    def _dispatch_data(self, topic, partition, msg):
        try:
            self._handlers[topic][partition](self, msg)
        except Exception as exc:
            return

    def _dispatch_ctrl(self, topic, partition, msg):
        try:
            self._controls[topic][partition](self, msg)
        except Exception as exc:
            return


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

    def onReady(self):
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
        """
        Published the data to slait server
        False if connection lost otherwise always True
        """
        if self._pubws is not None and self._pubws.connected and self._publisher_ready:

            return True
        else:
            return False

