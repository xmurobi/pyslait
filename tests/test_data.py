from ast import literal_eval
from pyslait import data
import numpy as np
import imp
imp.reload(data)

td1 = literal_eval(r"""
["bars","quotes","lobs"]
""")

td2 = literal_eval(r"""
{'Data':[{'Timestamp':'2009-11-10T23:00:00', 'Data':b'\x00\x00\x00\x01\x00\x00\x00\x11\x00\x00\x01\x01\x00\x00\x00\xa0\x06\x00\x00\xe4\xe9\x00\x00\x00'},
{'Timestamp':'2009-11-10T23:00:01', 'Data':b'\x00\x00\x00\x02\x00\x00\x00\x22\x00\x00\x02\x02\x00\x00\x00\xa0\x01\x00\x00\xe4\x00\x00\x00\x01'}]
}
""")


def test_results():
    q1 = data.TopicsResult(results=td1)
    assert q1.results[2] == 'lobs'

    q2 = data.TopicsResult(topic='bars',partitions='test',results=td2['Data'])
    assert q2.results[0][0] == np.datetime64("2009-11-10T23:00:00")

    def result_parser(item):
        vals = [item[i:i+4] for i in range(0, len(item), 4)]
        return [int.from_bytes(vals[0], byteorder='big'), int.from_bytes(vals[1], byteorder='big'), int.from_bytes(vals[2], byteorder='big'), int.from_bytes(vals[3], byteorder='big')]

    q2 = data.TopicsResult(topic="bars", partitions='test',
                           results=td2['Data'], result_parser=result_parser)
    assert (q2.results[0] == np.array(
        [np.datetime64("2009-11-10T23:00:00"), 1, 17, 257, 160])).all()
