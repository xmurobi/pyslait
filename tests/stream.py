slait_server = "http://192.168.220.128:5994/"
# slait_server = "http://127.0.0.1:5994/"

def subscriber_test():
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    import pyslait

    sc = pyslait.StreamClient(slait_server)

    @sc.onCtrl("lobs","BTC")
    def on_ctrl(streamClient, msg):
        print('Got[lobs/BTC] ctrl cmd:', msg)

    @sc.onData("lobs","BTC")
    def on_data(streamClient, msg):
        print('Got BTC data:', msg)

    @sc.onData("lobs","ETH")
    def on_data(streamClient, msg):
        print('Got ETH data:', msg)

    sc.runsub("lobs",["BTC","ETH"])

def publisher_test():
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    import pyslait
    from datetime import datetime, timedelta

    sc = pyslait.StreamClient(slait_server)

    def dgen():
        n = datetime.now()
        l = list()
        for i in range(1,5):
            d = dict()
            d['Data'] = str(i)
            d['Timestamp'] = pyslait.client.datestring(n+timedelta(seconds=5*i))
            l.append(d)

        return l

    @sc.onCtrl("lobs","BTC")
    def on_ctrl(streamClient, msg):
        if msg == '__pub__ready__':
            l = dgen()
            sc.publish("lobs", "BTC", l)
            print('Publish[lobs/BTC] :', l)

        else:
            print('Got[lobs/BTC] ctrl cmd:', msg)


    sc.runpub("lobs","BTC")

def subpub_test():
    from os import sys, path
    sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
    import threading

    s = threading.Thread(target=subscriber_test)
    p = threading.Thread(target=publisher_test)

    s.setDaemon(True)
    p.setDaemon(True)

    s.start()
    p.start()


if __name__ == '__main__' and not __package__:
    # subscriber_test()
    publisher_test()