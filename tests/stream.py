
def subscriber_test():
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    import pyslait

    # sc = pyslait.StreamClient("http://192.168.220.128:5994/")
    sc = pyslait.StreamClient("http://127.0.0.1:5994/")

    @sc.onCtrl("lobs","BTC")
    def on_ctrl(streamClient, msg):
        print('Got ctrl cmd:', msg)

    @sc.onData("lobs","BTC")
    def on_data(streamClient, msg):
        print('Got BTC data:', msg)

    @sc.onData("lobs","ETH")
    def on_data(streamClient, msg):
        print('Got ETH data:', msg)

    sc.runsub(pyslait.SocketMessage.handshakeSubscribers(topic="lobs", partitions=["BTC","ETH"]))


if __name__ == '__main__' and __package__ is None:
    subscriber_test()