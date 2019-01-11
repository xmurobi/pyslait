import numpy as np
import pandas as pd
import six

def isiterable(something):
    return isinstance(something, (list, tuple, set))

class ResultParser(object):
    """ 
    Parse the bytes block to multiple fields.
    The fn must return an array
    The dt must be str splited by comma which present the fn's return for np.dtype
    """
    fn = None
    dt = np.dtype("M,V")

    def __init__(self, fn, dt):
        assert not callable(fn)
        assert not isinstance(dt, np.dtype)
        self.fn = fn
        self.dt = dt

class TopicsResult(object):
    def __init__(self, topic=None, partitions=None, results=None, result_parser=None):
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
                    if isinstance(result_parser,ResultParser):
                        return [item['Timestamp']].append(result_parser.fn(item['Data']))
                    else:
                        return [item['Timestamp'],item['Data']]

                def _header():
                    if isinstance(result_parser,ResultParser):
                        return np.dtype("M,{}".format(result_parser.dt))
                    else:
                        return np.dtype("M,V")
                        
                self.results = np.array([_parser(item) for item in results], dtype=_header())
                a = self.results
            else:
                self.results = np.array([item for item in results],dtype=np.dtype('U'))

            
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
