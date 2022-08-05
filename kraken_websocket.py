from typing import Any, Dict, List, Union
from websockets import WebSocketClientProtocol
import websockets
import asyncio
import logging
import simplejson

logging.basicConfig(level=logging.INFO)


class Subscription:

    DEPTHS = (10, 25, 100, 500, 1000)
    INTERVALS = (1, 5, 15, 30, 60, 240, 1440, 10080, 21600)
    NAMES = ('book', 'ohlc', 'openOrders', 'ownTrades',
             'spread', 'ticker', 'trade', '*')

    def __init__(
        self, name: str, pair: List[str],
        event: str = 'subscribe', reqid: int = None
    ) -> None:

        assert name in self.NAMES

        self.event = event
        self.pair = pair if isinstance(pair, list) else [pair]
        self.reqid = reqid

        self.subscription = {
            'name': name
        }

    def set(
        self, depth: int = 10, interval: int = 1, ratecounter: bool = False,
        snapshot: bool = True, token: str = None
    ) -> None:

        assert depth in self.DEPTHS
        assert interval in self.INTERVALS

        self.subscription.update({
            'depth': depth,
            'interval': interval,
            'ratecounter': ratecounter,
            'snapshot': snapshot,
            'token': token
        })

    def to_json(self, as_str: bool = False) -> Union[Dict[str, Any], str]:
        subscription = {
            'event': self.event,
            'pair': self.pair,
            'subscription': self.subscription
        }

        if self.reqid:
            subscription.update({'reqid': self.reqid})

        if as_str:
            subscription = simplejson.dumps(subscription)

        return subscription


class KrakenWebSocket:
    def __init__(
        self, url: str = 'wss://ws.kraken.com',
        subscription: Subscription = Subscription('trade', 'XBT/USD')
    ) -> None:

        self.url = url
        self.subscription = subscription

    async def consumer_handler(self, ws: WebSocketClientProtocol) -> None:
        async for message in ws:
            self.log_message(message)

    async def consume(self) -> None:
        async with websockets.connect(self.url) as ws:
            await ws.send(self.subscription.to_json(as_str=True))
            await self.consumer_handler(ws)

    def log_message(self, message: str) -> None:
        logging.info(f'Message: {message}')

    def run(self) -> None:
        asyncio.run(self.consume())


kraken = KrakenWebSocket(subscription=Subscription('book', 'XBT/USD'))
kraken.run()

# s = Subscription('trade', 'XBT/USD')
# s.set()
# s.to_json(True)
