from __future__ import absolute_import

import json
import logging
import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytz
import requests
import six

from .data import TopicsResult
# from .stream import StreamConn

logger = logging.getLogger(__name__)


data_type_conv = {
    '<f4': 'f',
    '<f8': 'd',
    '<i4': 'i',
    '<i8': 'q',
}


def isiterable(something):
    return isinstance(something, (list, tuple, set))


def datestring(value):
    if isinstance(value, (int, np.integer)):
        value = datetime.fromtimestamp(value)
    elif value is None or not isinstance(value, datetime):
        value = datetime.now()

    # value = value.replace(tzinfo=pytz.UTC)
    return value.isoformat() + 'Z' if value.microsecond > 0 else ''

class Client(object):
    """
    Client use REST & websocket communicate with Slait
    """
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

    def list(self, topic=None, partition=None, fromDate=None, toDate=None, last=None, fn_entry_data_decoder=None):
        """
        1. List all topics
        2. List partitions under specific topic
        3. List data under specific topic and partition with from,to,last[optional] paramters
        Return TopicsResult object if query success
        """
        isDetails = False

        if topic is not None and partition is None:
            u = self._url("topics/{}".format(topic))

        elif topic is not None and partition is not None:
            # 2 hours ago as default fromDate
            if fromDate is None:
                fromDate = datetime.now() - timedelta(hours=2)
            if toDate is None:
                toDate = datetime.now()

            path = "topics/{}/{}?from={}&to={}".format(topic,partition,datestring(fromDate),datestring(toDate))

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
                if callable(fn_entry_data_decoder):
                    return TopicsResult(topic=topic, partitions=partition, results=rj['Data'], result_parser=fn_entry_data_decoder)
                else:
                    return TopicsResult(topic=topic, partitions=partition, results=rj['Data'])

            else:
                return TopicsResult(topic=topic, partitions=partition, results=rj)
                
        except requests.exceptions.HTTPError as exc:
            logger.exception(exc)
            raise

    def create(self, topic, partitions=None, entries=None, fn_entry_data_encoder=None):
        """
        1. Create topic
        2. Create topic and also with partition(s)
        3. Create entries under specific topic & partition(only one element in the list) and append to data stream
        Return True as server updated, else False
        """

        if not topic:
            return False
        else:
            data = {}
            path = ''

            # 2. Create topic and partitions
            if isinstance(partitions,list) and len(partitions) > 1:
                path = "topics"
                data['Topic'] = topic
                data['Partitions'] = partitions
            
            # 3. Append entries
            elif isinstance(partitions, list) and len(partitions) == 1 and isinstance(entries, list) and len(entries) > 0:
                path = "topics/{}/{}".format(topic,partitions[0])

                if not callable(fn_entry_data_encoder):
                    def _f(entries):
                        now = datestring(None)
                        ret = []
                        for en in entries:
                            ret.append({'Timestamp':now, 'Data':self.codec.dumps(en)})

                        return ret
                    fn_entry_data_encoder = _f

                data['Data'] = fn_entry_data_encoder(entries)

            # 1. Topic only
            else:
                path = "topics"
                data['Topic'] = topic

            try:
                u = self._url(path)

                if data.get('Data') != None:
                    r = self._session.put(u, data=self.codec.dumps(data), headers={"Content-Type": self.mimetype})
                elif data.get('Topic') != None:
                    r = self._session.post(u, data=self.codec.dumps(data), headers={"Content-Type": self.mimetype})
                else:
                    return False

                r.raise_for_status()
                return True

            except requests.exceptions.HTTPError as exc:
                logger.exception(exc)
                return False

    def delete(self, topic=None, partition=None):
        """
        1. Delete all topics(clear the slait)
        2. Delete all partitions under a specific topic
        3. Delete all entries under a specific topic and partition
        """
        path = None
        if not topic:
        # 1. delete all topics
            path = "topics"
        elif not partition:
        # 2. delete specific topic
            path = "topics/{}".format(topic)
        elif len(topic) > 0 and len(partition) > 0:
        # 3. delete all entries under partition
            path = "topics/{}/{}".format(topic, partition)
        
        try:
            u = self._url(path)
            r = self._session.delete(u)
            r.raise_for_status()
            return True
        except requests.exceptions.HTTPError as exc:
            return False

    def heartbeat(self):
        try:
            u = self._url("heartbeat")
            r = self._session.head(u)

            r.raise_for_status()
            return True
        except requests.exceptions.HTTPError as exc:
            return False

    # def stream(self):
    #     endpoint = re.sub('^http', 'ws', self._url('ws'))
    #     return StreamConn(endpoint)

    def __repr__(self):
        return 'Client("{}")'.format(self._endpoint)
