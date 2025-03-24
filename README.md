# BinanceWebSocketConsumer

This sample demonstrates consuming data from Binance web socket.

Data is not deserialized. Channel names are parsed with Utf8JsonParser for zero-allocation.

Handles reconnects and reports dropped messages.