import pyslait
import json
import numpy as np
try:
    from unittest.mock import patch
except ImportError:
    from mock import patch
import imp
from datetime import datetime,timedelta

imp.reload(pyslait.client)

def test_init():
    pass

def test_client_init():
    c = pyslait.Client("http://127.0.0.1:5994/")
    assert c._endpoint == "http://127.0.0.1:5994/"

def test_rest():
    c = pyslait.Client("http://192.168.220.128:5994/")
    # c = pyslait.Client("http://127.0.0.1:5994/")

    # clear slait
    assert c.delete()

    # test create
    assert c.create("bars")
    assert c.create("lobs",partitions=["BTC","ETH"])
    assert c.create("lobs",partitions=["BTC"],entries=[{"A0":111},{"A1":112},{"B0":110},{"B1":108}])

    # test list
    assert len(c.list().results) == 2
    assert len(c.list(topic="bars").results) == 0
    assert len(c.list(topic="aabb").results) == 0
    assert len(c.list(topic="lobs",partition="BTC",fromDate=(datetime.now() - timedelta(hours=2)),toDate=datetime.now()).results) == 4
    assert c.list(topic="lobs",partition="BTC",fromDate=(datetime.now() - timedelta(hours=2)),toDate=datetime.now(),
        fn_entry_data_decoder=lambda x:[json.loads(x)]).results[1][1]["A1"] == 112

    # test delete
    assert c.delete(topic="aabb")
    assert c.delete(topic="lobs",partition="ETH")

    # test heartbeat
    assert c.heartbeat()

def test_socket():
    pass