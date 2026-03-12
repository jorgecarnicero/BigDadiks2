# -*- coding: utf-8 -*-
from binance import Client
from binance import ThreadedWebsocketManager
from kafka_simple_producer import produce

def handle_kline(msg):
    k = msg['k']

    if k["x"]:  # Solo producimos si ha cerrado la vela (lo indica el booleano de k["x"])
        produce(data=k)


twm = ThreadedWebsocketManager()
twm.start()

twm.start_kline_socket(
    symbol='SOLBTC',
    interval=Client.KLINE_INTERVAL_1MINUTE,
    callback=handle_kline
)

input("Pulsa ENTER para salir\n")
twm.stop()






###################