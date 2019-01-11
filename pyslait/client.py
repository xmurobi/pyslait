from __future__ import absolute_import
import numpy as np
import pandas as pd
import re
import requests
import logging
import six
import json
import pytz
from datetime import datetime, timedelta

from .stream import StreamConn
from .data import TopicsResult

logger = logging.getLogger(__name__)


data_type_conv = {
    '<f4': 'f',
    '<f8': 'd',
    '<i4': 'i',
    '<i8': 'q',
}


def isiterable(something):
    return isinstance(something, (list, tuple, set))


# def get_rpc_client(codec='msgpack'):
#     if codec == 'msgpack':
#         return MsgpackRpcClient
#     return JsonRpcClient


def get_timestamp(value):
    if value is None:
        return None
    if isinstance(value, (int, np.integer)):
        return pd.Timestamp(value, unit='s')
    return pd.Timestamp(value)


class Params(object):
    pass
    # def __init__(self, symbols, timeframe, attrgroup,
    #              start=None, end=None,
    #              limit=None, limit_from_start=None):
    #     if not isiterable(symbols):
    #         symbols = [symbols]
    #     self.tbk = ','.join(symbols) + "/" + timeframe + "/" + attrgroup
    #     self.key_category = None  # server default
    #     self.start = get_timestamp(start)
    #     self.end = get_timestamp(end)
    #     self.limit = limit
    #     self.limit_from_start = limit_from_start
    #     self.functions = None

    # def set(self, key, val):
    #     if not hasattr(self, key):
    #         raise AttributeError()
    #     if key in ('start', 'end'):
    #         setattr(self, key, get_timestamp(val))
    #     else:
    #         setattr(self, key, val)
    #     return self

    # def __repr__(self):
    #     content = ('tbk={}, start={}, end={}, '.format(
    #         self.tbk, self.start, self.end,
    #     ) +
    #         'limit={}, '.format(self.limit) +
    #         'limit_from_start={}'.format(self.limit_from_start))
    #     return 'Params({})'.format(content)

class Client(object):
    codec = json
    mimetype = "application/json"

    def __init__(self, endpoint='http://localhost:5994/'):
        self._endpoint = endpoint
        self._session = requests.Session()

    def _url(self, path=""):
        url = self._endpoint
        if not url.endswith("/"):
            url += "/"
        return url + path  


    def list(self, topic=None, partition=None, fromDate=None, toDate=None, last=None, fn_detail_parser=None):
        isDetails = False

        if topic is not None and partition is None:
            u = self._url("topics/{}".format(topic))

        elif topic is not None and partition is not None:
            # 2 hours ago as default fromDate
            if fromDate is None:
                fromDate = datetime.now() - timedelta(hours=2)
            if toDate is None:
                toDate = datetime.now()

            fromDate = fromDate.replace(tzinfo=pytz.UTC)
            toDate = toDate.replace(tzinfo=pytz.UTC)
            path = "topics/{}/{}?from={}&to={}".format(topic,partition,fromDate.isoformat(),toDate.isoformat())

            # only the last n
            if last is not None and isinstance(last, int):
                path += "&last={}".format(last)

            u = self._url(path)

            isDetails = True
        else:
            u = self._url("topics")


        try:
            r = self._session.get(u,headers={"Content-Type": self.mimetype})
            r.raise_for_status()

            rj = r.json()
            if isDetails:
                if not callable(fn_detail_parser):
                    def _fn_parser(item):
                        item['Timestamp']
                        item['Data']
                    fn_detail_parser = _fn_parser

                return TopicsResult(topic=topic, partitions=partition, results=rj['Data'], raw_parser_fn=fn_detail_parser)
            else:
                return TopicsResult(topic=topic, partitions=partition, results=rj)
                
        except requests.exceptions.HTTPError as exc:
            logger.exception(exc)
            raise

    def create(self, topic, partitoins=None):

        pass        


    def delete(self):
        pass

    # def query(self, params):
    #     if not isiterable(params):
    #         params = [params]
    #     query = self.build_query(params)
    #     reply = self._request('DataService.Query', **query)
    #     return QueryReply(reply)

    # def write(self, recarray, tbk, isvariablelength=False):
    #     data = {}
    #     data['types'] = [
    #         recarray.dtype[name].str.replace('<', '')
    #         for name in recarray.dtype.names
    #     ]
    #     data['names'] = recarray.dtype.names
    #     data['data'] = [
    #         bytes(buffer(recarray[name])) if six.PY2
    #             else bytes(memoryview(recarray[name]))
    #         for name in recarray.dtype.names
    #     ]
    #     data['length'] = len(recarray)
    #     data['startindex'] = {tbk: 0}
    #     data['lengths'] = {tbk: len(recarray)}
    #     write_request = {}
    #     write_request['dataset'] = data
    #     write_request['is_variable_length'] = isvariablelength
    #     writer = {}
    #     writer['requests'] = [write_request]
    #     try:
    #         reply = self.rpc.call("DataService.Write", **writer)
    #     except requests.exceptions.ConnectionError:
    #         raise requests.exceptions.ConnectionError(
    #             "Could not contact server")
    #     reply_obj = self.rpc.codec.loads(reply.content, encoding='utf-8')
    #     resp = self.rpc.response(reply_obj)
    #     return resp

    # def build_query(self, params):
    #     reqs = []
    #     if not isiterable(params):
    #         params = [params]
    #     for param in params:
    #         req = {
    #             'destination': param.tbk,
    #         }
    #         if param.key_category is not None:
    #             req['key_category'] = param.key_category
    #         if param.start is not None:
    #             req['epoch_start'] = int(param.start.value / (10 ** 9))
    #         if param.end is not None:
    #             req['epoch_end'] = int(param.end.value / (10 ** 9))
    #         if param.limit is not None:
    #             req['limit_record_count'] = int(param.limit)
    #         if param.limit_from_start is not None:
    #             req['limit_from_start'] = bool(param.limit_from_start)
    #         if param.functions is not None:
    #             req['functions'] = param.functions
    #         reqs.append(req)
    #     return {
    #         'requests': reqs,
    #     }

    # def list_symbols(self):
    #     reply = self._request('DataService.ListSymbols')
    #     if 'Results' in reply.keys():
    #         return reply['Results']
    #     return []

    # def server_version(self):
    #     resp = requests.head(self.endpoint)
    #     return resp.headers.get('Marketstore-Version')

    # def stream(self):
    #     endpoint = re.sub('^http', 'ws',
    #                       re.sub(r'/rpc$', '/ws', self.endpoint))
    #     return StreamConn(endpoint)

    def __repr__(self):
        return 'Client("{}")'.format(self.endpoint)
