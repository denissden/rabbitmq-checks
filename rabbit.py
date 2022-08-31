import pika
import pika.spec
import pika.connection
import pika.channel
import logging
import asyncio
from threading import Thread
from typing import Callable

logger = logging.getLogger(__name__)

MessageHandlerType = Callable[[str, pika.spec.Basic.Deliver, pika.spec.BasicProperties, bytes], None]

class Rabbit:
    def __init__(self, params: pika.ConnectionParameters) -> None:
        self.params = params
        self.connection = pika.SelectConnection(parameters=params,
                                   on_open_callback=self._on_open,
                                   on_open_error_callback=self._on_open_error)
        
        self._async_loop = asyncio.get_event_loop()
        self._start_future = asyncio.Future()
    
    @classmethod
    async def create(cls, params: pika.ConnectionParameters, timeout=5):
        logger.info('Creating Rabbit on vhost \'%s\' with timeout %s', 
            params.virtual_host, 
            timeout)
        self = Rabbit(params)
        await self.start(timeout)
        return self
    
    async def start(self, timeout=5):
        logger.info('Starting Rabbit with timeout %s', timeout)
        t = Thread(target=self.connection.ioloop.start)
        t.start()
        t.join(0)
        await asyncio.wait_for(self._start_future, timeout)

    def stop(self):
        logger.info('Stopping Rabbit')
        self.connection.ioloop.stop()
    
    def consume(self, queue: str, handler: MessageHandlerType):
        logger.info('Consuming from queue \'%s\' on vhost \'%s\'', 
            queue, 
            self.params.virtual_host)
        self.channel.basic_consume(
            queue, 
            on_message_callback=self._wrap_on_message(
                queue=queue, 
                handler=handler))
    
    def purge(self, queue: str):
        self.channel.queue_purge(queue)
    
    def publish(self, exchange, routing_key, body, properties=None):
        self.channel.basic_publish(exchange, routing_key, body, properties)

    def _on_open(self, connection: pika.connection.Connection):
        logger.info('Rabbit connected')
        self.channel = connection.channel(on_open_callback=self._on_channel_open)
        print(self.channel)

    def _on_open_error(self, connection, error):
        logger.error('Could not open connection: %s %s', 
            connection, 
            error)
        connection.ioloop.stop()

        self._async_loop.call_soon_threadsafe(self._start_future.set_exception, error)

    def _on_channel_open(self, channel: pika.channel.Channel):
        logger.info('Channel created')
        channel.add_on_close_callback(self._on_channel_close)
        channel.add_on_cancel_callback(self._on_cancel_consume)

        self._async_loop.call_soon_threadsafe(self._start_future.set_result, True)

    def _wrap_on_message(self, queue: str, handler: MessageHandlerType):
        def on_message(channel: pika.channel.Channel, 
            method: pika.spec.Basic.Deliver, 
            properties: pika.spec.BasicProperties, 
            body):
            logger.info('Message # %s - %s: %s', 
                method.delivery_tag,
                method.routing_key,
                body.decode('utf-8'))
            
            try:
                handler(queue, method, properties, body)
            except Exception as e:
                logger.exception(e)
            
            channel.basic_ack(method.delivery_tag)
        
        return on_message
    
    def _on_cancel_consume(self, method):
        logger.info('Received cancel from broker')
        self.connection.close()

    def _on_channel_close(self, channel: pika.channel.Channel, ex):
        logger.info('Channel closed: %s', ex)
        self.connection.ioloop.stop()


async def main():
    params = pika.ConnectionParameters(
                    host='localhost',
                    port=5672,
                    virtual_host='/',
                    credentials=pika.PlainCredentials(
                        username='guest',
                        password='guest'
                    )
                )
    rabbit = await Rabbit.create(params)

    def handler(queue: str, method, properties, body):
        print(queue, '#', body.decode('utf-8'))

    rabbit.publish('message_exchange', 'email.send', 'email content')
    rabbit.publish('message_exchange', 'process.something', 'payload')
    rabbit.consume('q_process', handler)
    rabbit.consume('q_email', handler)
    await asyncio.sleep(5)
    rabbit.stop()

if __name__ == '__main__':
    asyncio.run(main())