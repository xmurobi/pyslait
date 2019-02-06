import numpy as np
import pandas as pd
import six
import jsonpickle
from datetime import datetime
from enum import Enum

def isiterable(something):
    return isinstance(something, (list, tuple, set))

class CtrlMessage(Enum):
    SUB_CLOSED = '__sub__closed__'
    PUB_CLOSED = '__pub__closed__'
    PUB_READY = '__pub__ready__'

class JsonObject(object):
    def toJSON(self):
        return jsonpickle.encode(self, unpicklable=False)


class TopicsResult(JsonObject):
    def __init__(self, topic=None, partitions=None, results=None, result_parser=None):
        """
        result_parser must be a callback function which 
        take bytes buffer and return array as parse result
        """
        if partitions is not None and not isiterable(partitions):
            partitions = [partitions]
        self.partitions = partitions
        self.topic = topic
        self._decode(results, result_parser)

    def __repr__(self):
        req = 'Topic=' + str(self.topic)
        if isiterable(self.partitions):
            req += ',Partitions[' 
            for p in self.partitions:
                req += str(p) + ','
            req += ']'
        return 'Request({})'.format(req)

    def _decode(self, results, result_parser=None):
        self.results = None

        if isiterable(results):
            if self.partitions and self.topic:
                def _parser(item):
                    if callable(result_parser):
                        l = result_parser(item['Data'])
                        l.insert(0, np.datetime64(item['Timestamp']))
                        return l
                    else:
                        return [np.datetime64(item['Timestamp']), item['Data']]
                
                for item in results:
                    if self.results is None:
                        self.results = np.array([_parser(item)])
                    else:
                        self.results = np.append(self.results, [_parser(item)], axis=0)
            else:
                self.results = np.array([item for item in results],dtype=np.dtype('U'))

class SocketMessage(JsonObject):
    """
    Data package for websocket
    """

    def __init__(self, action, topic=None, partitions=None, fromDate=None, entries=None):
        self.Action = action
        self.Topic = topic
        if partitions is not None and not isiterable(partitions):
            partitions = [partitions]
        self.Partitions = partitions
        self.From = fromDate
        self.Data = entries

    def __repr__(self):
        return 'SocketMessage(act={}, topic={}, partitions={}, from={}, entries={})'.format(
            self.Action, self.Topic, self.Partitions,self.From,self.Data
        )

    @staticmethod
    def handshakeSubscribers(topic=None, partitions=None, appendToList=None):
        return SocketMessage._buildConnectionMessage(isSub=True, topic=topic, partitions=partitions, appendToList=appendToList)

    @staticmethod
    def handshakePublisher(topic=None,partitions=None):
        return SocketMessage._buildConnectionMessage(isSub=False, topic=topic, partitions=partitions, appendToList=None)
    
    @staticmethod
    def _buildConnectionMessage(isSub=True, topic=None,partitions=None, appendToList=None):
        if not isinstance(appendToList,list):
            appendToList = []
        if not topic:
            return appendToList
        if not partitions or len(partitions) < 1:
            return appendToList
        if isinstance(partitions,str):
            partitions = [partitions]

        m = SocketMessage("subscribe" if isSub else "publish", topic=topic, partitions=partitions)
        appendToList.append(m)
        return appendToList        

