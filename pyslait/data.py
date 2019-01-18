import numpy as np
import pandas as pd
import six
import jsonpickle
from datetime import datetime

def isiterable(something):
    return isinstance(something, (list, tuple, set))

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

# class DataSet(object):

#     def __init__(self, array, key, reply):
#         self.array = array
#         self.key = key
#         self.reply = reply

#     @property
#     def timezone(self):
#         return self.reply['timezone']

#     @property
#     def symbol(self):
#         return self.key.split('/')[0]

#     @property
#     def timeframe(self):
#         return self.key.split('/')[1]

#     @property
#     def attribute_group(self):
#         return self.key.split('/')[2]

#     def df(self):
#         idxname = self.array.dtype.names[0]
#         df = pd.DataFrame(self.array).set_index(idxname)
#         index = pd.to_datetime(df.index, unit='s', utc=True)
#         tz = self.timezone
#         if tz.lower() != 'utc':
#             index = index.tz_convert(tz)
#         df.index = index
#         return df

#     def __repr__(self):
#         a = self.array
#         return 'DataSet(key={}, shape={}, dtype={})'.format(
#             self.key, a.shape, a.dtype,
#         )


# class QueryResult(object):

#     def __init__(self, result, reply):
#         self.result = {
#             key: DataSet(value, key, reply)
#             for key, value in six.iteritems(result)
#         }
#         self.reply = reply

#     @property
#     def timezone(self):
#         return self.reply['timezone']

#     def keys(self):
#         return list(self.result.keys())

#     def first(self):
#         return self.result[self.keys()[0]]

#     def all(self):
#         return self.result

#     def __repr__(self):
#         content = '\n'.join([
#             str(ds) for _, ds in six.iteritems(self.result)
#         ])
#         return 'QueryResult({})'.format(content)


# class QueryReply(object):

#     def __init__(self, reply):
#         results = decode_responses(reply['responses'])
#         self.results = [QueryResult(result, reply) for result in results]
#         self.reply = reply

#     @property
#     def timezone(self):
#         return self.reply['timezone']

#     def first(self):
#         return self.results[0].first()

#     def all(self):
#         datasets = {}
#         for result in self.results:
#             datasets.update(result.all())
#         return datasets

#     def keys(self):
#         keys = []
#         for result in self.results:
#             keys += result.keys()
#         return keys

#     def get_catkeys(self, catnum):
#         ret = set()
#         for key in self.keys():
#             elems = key.split('/')
#             ret.add(elems[catnum])
#         return list(ret)

#     def symbols(self):
#         return self.get_catkeys(0)

#     def timeframes(self):
#         return self.get_catkeys(1)

#     def by_symbols(self):
#         datasets = self.all()
#         ret = {}
#         for key, dataset in six.iteritems(datasets):
#             symbol = key.split('/')[0]
#             ret[symbol] = dataset
#         return ret

#     def __repr__(self):
#         content = '\n'.join([
#             str(res) for res in self.results
#         ])
#         return 'QueryReply({})'.format(content)
