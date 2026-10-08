# -*- coding: utf-8 -*-
from binance import Client
from binance import ThreadedWebsocketManager
from kafka_simple_producer import produce

def handle_kline(msg):
    k = msg['k']

    if k["x"]:  # Only produce when the candle has closed (k["x"] is True)
        produce(data=k)


twm = ThreadedWebsocketManager()
twm.start()

twm.start_kline_socket(
    symbol='SOLBTC',
    interval=Client.KLINE_INTERVAL_1MINUTE,
    callback=handle_kline
)

input("Press ENTER to exit\n")
twm.stop()






###################