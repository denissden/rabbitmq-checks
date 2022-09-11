from collections import defaultdict
import logging
from facade import Facade
from message_checker import MessageChecker
import asyncio
import pika
from itertools import product

logger = logging.getLogger(__name__)

async def check():
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
     # указываем лист [params] чтобы случайно не подключиться ко второму vhost, если он есть
    facade = await Facade.create(*management, [params])
    checker = MessageChecker(facade)

    #
    ##### TO_PUBLISH START #####
    #

    # все возможные ключи
    keys = [
        'order.placed',
        'order.billed',
        'order.payed',
        'order.cancelled',
        'order.packaged',
        'order.delivered',
        'order.cancelled.returned',
        'order.status.request',     
        'order.status.response',  
        'delivery.updateInfo',
    ]
    # все возможные города в хедерах
    city_headers = [
        { 'city': 'msk' }, 
        { 'city': 'spb' }, 
        { 'city': 'ekb' }, 
        { 'city': 'nsb' },
    ]   

    to_publish = []
    # добаляем все возможные комбинации ключей и городов
    to_publish += list(product(keys, city_headers))

    # order.packaged также имеет хедер order_size, добавляем его тут
    to_publish += [
        ('order.packaged', {'order-size': 'small', 'city': 'msk' }),
        ('order.packaged', {'order-size': 'small', 'city': 'spb' }),
        ('order.packaged', {'order-size': 'small', 'city': 'ekb' }),
        ('order.packaged', {'order-size': 'small', 'city': 'nsb' }),

        ('order.packaged', {'order-size': 'medium', 'city': 'msk' }),
        ('order.packaged', {'order-size': 'medium', 'city': 'spb' }),
        ('order.packaged', {'order-size': 'medium', 'city': 'ekb' }),
        ('order.packaged', {'order-size': 'medium', 'city': 'nsb' }),

        ('order.packaged', {'order-size': 'large', 'city': 'msk' }),
        ('order.packaged', {'order-size': 'large', 'city': 'spb' }),
        ('order.packaged', {'order-size': 'large', 'city': 'ekb' }),
        ('order.packaged', {'order-size': 'large', 'city': 'nsb' }),
    ]

    #
    ##### TO_PUBLISH END #####
    #

    # чистим очереди перед отправкой тестовых сообщений
    # если не указать vhost, очистятся все очереди во всех vhost-ах
    logger.info('Purging messages')
    _ = await checker.dump_messages(vhost='/', wait_time=1)

    id_ = 1
    logger.info('Sending test messages')
    for key, headers in to_publish:
        props = pika.BasicProperties(
            headers=headers
        )
        facade.publish('x_main', key, str(id_), properties=props)
        id_ += 1
    
    logger.info('Dumping messages for check')
    received = await checker.dump_messages(vhost='/', wait_time=1)
    filtered = defaultdict(set) # set потому что в данном случае не важен порядок
    for queue_name, queue_messages in received.items():
        for message in queue_messages:
            unique_identifier = int(message['body'])
            filtered[queue_name].add(unique_identifier)
    
    ### Если на ноде были загружены настройки решения, print выведет right_answer
    print(filtered)

    right_answer = {
        'q_notification': {1, 2, 3, 4, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52}, 
        'q_billing': {1, 2, 3, 4}, 
        'q_monitoring': {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52}, 
        'q_packaging_europe': {9, 10, 13, 14, 25, 26}, 
        'q_packaging_asia': {11, 12, 15, 16, 27, 28}, 
        'q_call_center': {13, 14, 15, 16, 17, 18, 19, 20, 33, 34, 35, 36, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52}, 
        'q_delivery_msk': {37, 41, 45, 13, 17, 49}, 
        'q_delivery_spb': {38, 42, 46, 14, 18, 50}, 
        'q_delivery_ekb': {39, 43, 47, 15, 19, 51}, 
        'q_delivery_nsb': {40, 44, 16, 48, 20, 52}, 
        'q_delivery_large': {49, 50, 51, 52}
    }

    ###
    print('=' * 30)
    errors = []
    for queue, ids in right_answer.items():
        if queue not in received:
            errors.append(('Queue does not exist!', queue))
            continue
        
        if len(ids) != len(filtered[queue]):
            errors.append(('Wrong amount of messages in', queue))
            continue
        
        if ids != filtered[queue]:
            errors.append(('Wrong messages in', queue))

    for error, queue in errors:
        print(error, queue)
    if not errors:
        print('Check success!')
    print('=' * 30)

    facade.stop()

    


if __name__ == '__main__':
    asyncio.run(check())