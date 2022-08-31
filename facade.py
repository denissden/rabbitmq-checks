from venv import create
from rabbit import Rabbit, MessageHandlerType
from api import Api
import asyncio
import pika
import logging
import copy
from collections.abc import Iterable

logger = logging.getLogger(__name__)

class Facade:
    def __init__(self, rabbits: dict[str, Rabbit], api: Api) -> None:
        self.rabbits = rabbits
        self.api = api
    
    @classmethod
    async def create(cls,
        management_address,
        management_login,
        management_password,
        _rabbit_params: list[pika.ConnectionParameters] | pika.ConnectionParameters,
        rabbit_timeout = 5,
        ):
        logger.info('Creating Facade')
        api = Api(management_address, management_login, management_password)

        if isinstance(_rabbit_params, Iterable):
            logger.info('Rabbit params were an Iterable')
            rabbit_params = _rabbit_params
        else:
            logger.info('Rabbit params were a single instance. Using same params for all vhosts')
            rabbit_params = []
            for vhost in api.vhost_names():
                param = copy.copy(_rabbit_params)
                param.virtual_host = vhost
                rabbit_params.append(param)
        rabbits = await Facade.create_rabbits(rabbit_params, rabbit_timeout)
        
        
        self = Facade(rabbits, api)
        logger.info('Created Facade with %s Rabbits', len(rabbits))
        return self
    
    @staticmethod
    async def create_rabbits(
        rabbit_params: list[pika.ConnectionParameters],
        rabbit_timeout = 5):
        rabbits = {}
        for param in rabbit_params:
            rabbit = await Rabbit.create(param, rabbit_timeout)
            vhost = param.virtual_host
            rabbits[vhost] = rabbit

        return rabbits

    def stop(self):
        logger.info('Stopping Facade')
        for rabbit in self.rabbits.values():
            rabbit.stop()
    
    def consume_all_queues(self, handler: MessageHandlerType, vhost: str = None):
        queue_info = self.api.queue_info(vhost)
        for queue, v in queue_info:
            rabbit = self.rabbits[v]
            rabbit.consume(queue, handler=handler)
    
    def purge_all_queues(self, vhost: str = None):
        queue_info = self.api.queue_info(vhost)
        for queue, v in queue_info:
            rabbit = self.rabbits[v]
            rabbit.purge(queue)
    
    def publish(self, exchange, routing_key, body, vhost='/', properties=None):
        rabbit = self.rabbits[vhost]
        rabbit.publish(exchange, routing_key, body, properties)



async def main():
    logging.basicConfig(level=logging.INFO)

    params = pika.ConnectionParameters(
                    host='localhost',
                    port=5672,
                    virtual_host='/',
                    credentials=pika.PlainCredentials(
                        username='guest',
                        password='guest'
                    )
                )
    management = ('http://localhost:15672', 'guest', 'guest')
    facade = await Facade.create(*management, params)

    def on_message(queue: str, method, properties, body):
        print(queue, '#', body.decode('utf-8'))
    facade.consume_all_queues(on_message)

    await asyncio.sleep(5)
    facade.stop()

if __name__ == '__main__':
    asyncio.run(main())