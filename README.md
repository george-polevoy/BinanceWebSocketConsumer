# BinanceWebSocketConsumer

This sample demonstrates consuming data from Binance web socket.

Data is not deserialized. Channel names are parsed with Utf8JsonParser for zero-allocation.

Handles reconnects and reports dropped messages.

If socket is disconnected, it reconnects, but it does not reconnect more often than once in reconnect interval seconds. If it was working more than reconnect interval before failing, the reconnect attempt will be instant, otherwise it will wait the rest of the reconnect interval. Current reconnect interval is 3 seconds.
