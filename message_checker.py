from asyncio.log import logger
from collections import defaultdict
import logging
from facade import Facade
import asyncio
import pika

class MessageChecker:
    def __init__(self, facade: Facade) -> None:
        self.facade = facade
        self.received_messages = defaultdict(list)

    async def dump_messages(self, wait_time=5, vhost=None):
        self.facade.consume_all_queues(self.on_message, vhost=vhost)

        await asyncio.sleep(wait_time)

        return self.received_messages
    
    def on_message(self, queue: str, method, properties, body):
        info = self.construct(queue, method, properties, body)
        self.received_messages[queue].append(info)
    
    @staticmethod
    def construct(queue: str, method, properties, body):
        return {
            'queue': queue,
            'method': method,
            'properties': properties,
            'body': body.decode('utf-8')
        }
