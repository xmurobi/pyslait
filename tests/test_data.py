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

testdata1 = literal_eval(r"""
{'responses': [{'result': {'data': [b'\xf4\xe8^Z\x00\x00\x00\x000\xe9^Z\x00\x00\x00\x00l\xe9^Z\x00\x00\x00\x00\xa8\xe9^Z\x00\x00\x00\x00\xe4\xe9^Z\x00\x00\x00\x00',
     b'{\x14\xaeG\x01\xf4\xc5@H\xe1z\x14\xee\xe1\xc5@\x00\x00\x00\x00\x80\xfb\xc5@\x00\x00\x00\x00\x00\x06\xc6@{\x14\xaeG\x01\xfa\xc5@',
     b'{\x14\xaeG\x01\xf4\xc5@\x00\x00\x00\x00\x00\xf9\xc5@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\xfe\xc5@',
     b'\x85\xebQ\xb8^\xe0\xc5@H\xe1z\x14\xee\xe1\xc5@\x00\x00\x00\x00\x80\xfb\xc5@R\xb8\x1e\x85+\xf7\xc5@{\x14\xaeG\x01\xfa\xc5@',
     b'H\xe1z\x14\xee\xe1\xc5@\x00\x00\x00\x00\x00\xf9\xc5@\x00\x00\x00\x00\x00\x06\xc6@{\x14\xaeG\x01\xfa\xc5@\x85\xebQ\xb8\xfe\xfd\xc5@',
     b'iL\xd2F\xbf\xaf\n@\xfe\xe6\xff49\xfd\x0b@\xe1\x9b\xe8\xeb\xe01\x10@\xaf\xe4\x11y\x1e\xce\xfa?\xd7\xd2\x8a\x0c\xfe\x00\xf9?'],
    'length': 5,
    'lengths': {'BTC/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 5},
    'names': ['Epoch', 'Open', 'High', 'Low', 'Close', 'Volume'],
    'startindex': {'BTC/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 0},
    'types': ['i8', 'f8', 'f8', 'f8', 'f8', 'f8']}}],
 'timezone': 'UTC',
 'version': 'dev'}
""")  # noqa: E501

testdata2 = literal_eval(r"""
{'responses': [{'result': {'data': [b'l\xe9^Z\x00\x00\x00\x00\xa8\xe9^Z\x00\x00\x00\x00\xe4\xe9^Z\x00\x00\x00\x00 \xea^Z\x00\x00\x00\x00\\\xea^Z\x00\x00\x00\x00l\xe9^Z\x00\x00\x00\x00\xa8\xe9^Z\x00\x00\x00\x00\xe4\xe9^Z\x00\x00\x00\x00 \xea^Z\x00\x00\x00\x00\\\xea^Z\x00\x00\x00\x00',
     b'\x00\x00\x00\x00\x00\x88\x8f@)\\\x8f\xc2\xf5\x90\x8f@\xa4p=\n\xd7\x8f\x8f@\xcd\xcc\xcc\xcc\xcc\xa8\x8f@\x00\x00\x00\x00\x00\xb0\x8f@\x00\x00\x00\x00\x80\xfb\xc5@\x00\x00\x00\x00\x00\x06\xc6@{\x14\xaeG\x01\xfa\xc5@\x00\x00\x00\x00 \x02\xc6@\x00\x00\x00\x00\x00\x06\xc6@',
     b'\x00\x00\x00\x00\x00\xb0\x8f@fffff\xa2\x8f@\x00\x00\x00\x00\x00\xa8\x8f@\x00\x00\x00\x00\x00\xb0\x8f@\x00\x00\x00\x00\x00\xb0\x8f@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\x06\xc6@',
     b'\x00\x00\x00\x00\x00\x88\x8f@{\x14\xaeG\xe1\x84\x8f@\xa4p=\n\xd7\x8f\x8f@\xf6(\\\x8f\xc2\xa7\x8f@\x00\x00\x00\x00\x00\xb0\x8f@\x00\x00\x00\x00\x80\xfb\xc5@R\xb8\x1e\x85+\xf7\xc5@\\\x8f\xc2\xf5h\xf0\xc5@\x00\x00\x00\x00 \x02\xc6@\x00\x00\x00\x00\x00\x06\xc6@',
     b'\xd7\xa3p=\n\x99\x8f@\xa4p=\n\xd7\x8f\x8f@\x00\x00\x00\x00\x00\xa8\x8f@\x00\x00\x00\x00\x00\xb0\x8f@\x00\x00\x00\x00\x00\xb0\x8f@\x00\x00\x00\x00\x00\x06\xc6@{\x14\xaeG\x01\xfa\xc5@\x85\xebQ\xb8\x1e\x02\xc6@\x00\x00\x00\x00\x00\x06\xc6@\x00\x00\x00\x00\x00\x06\xc6@',
     b'f\r\x83\x9erg8@j\xa8\xcd\x0f\x8e\xdf<@\x7f\xc7\xa6Ku\xbcP@AG\xe5\x05\xdc\x1aU@\xdc\xb1d\xd0\x012+@\xe1\x9b\xe8\xeb\xe01\x10@\xaf\xe4\x11y\x1e\xce\xfa?\xa2\x9a\xa3\xd8\x1bb\x19@s!\xc1\x1a\x88\xa9/@\xbaI\x0c\x02+\x87\xf4?'],
    'length': 10,
    'lengths': {'BTC/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 5,
     'ETH/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 5},
    'names': ['Epoch', 'Open', 'High', 'Low', 'Close', 'Volume'],
    'startindex': {'BTC/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 5,
     'ETH/1Min/OHLCV:Symbol/Timeframe/AttributeGroup': 0},
    'types': ['i8', 'f8', 'f8', 'f8', 'f8', 'f8']}}],
 'timezone': 'America/New_York',
 'version': 'dev'}
""")  # noqa: E501


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
   #  assert reply.timezone == 'UTC'
   #  assert str(reply) == """QueryReply(QueryResult(DataSet(key=BTC/1Min/OHLCV, shape=(5,), dtype=[('Epoch', '<i8'), ('Open', '<f8'), ('High', '<f8'), ('Low', '<f8'), ('Close', '<f8'), ('Volume', '<f8')])))"""  # noqa
   #  assert reply.first().timezone == 'UTC'
   #  assert reply.first().symbol == 'BTC'
   #  assert reply.first().timeframe == '1Min'
   #  assert reply.first().attribute_group == 'OHLCV'
   #  assert reply.first().df().shape == (5, 5)
   #  assert list(reply.by_symbols().keys()) == ['BTC']
   #  assert reply.keys() == ['BTC/1Min/OHLCV']
   #  assert reply.symbols() == ['BTC']
   #  assert reply.timeframes() == ['1Min']

   #  reply = results.QueryReply(testdata2)
   #  assert str(reply.first().df().index.tzinfo) == 'America/New_York'
