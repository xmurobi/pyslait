
if __name__ == '__main__' and __package__ is None:
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    import pyslait

    sc = pyslait.StreamClient("http://192.168.220.128:5994/")

    @sc.onCtrl("lobs","BTC")
    def on_ctrl(streamClient, msg):
        print('Got ctrl cmd:', msg)

    @sc.onData("lobs","BTC")
    def on_data(streamClient, msg):
        print('Got data:', msg)

    sc.runsub(pyslait.SocketMessage.handshakeSubscribers(topic="lobs", partitions=["BTC","ETH"]))