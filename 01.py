from collections import defaultdict
import logging
from facade import Facade
from message_checker import MessageChecker
import asyncio
import pika

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

    # exchange, routing_key, headers, unique_identifier
    to_publish = [
        ('message_exchange', 'process.file.add_page', {},   1),
        ('message_exchange', 'process.photo', {},           2),
        ('message_exchange', 'file.process.add_page', {},   3),
        ('message_exchange', 'image.process', {},           4),
        
        ('message_exchange', 'email.api.success', {},       5),
        ('message_exchange', 'event.api.user_created', {},  6),
        ('message_exchange', 'api.clear_cache', {},         7),
        ('message_exchange', 'api', {},                     8),

        ('message_exchange', 'email.get_messages', {},  9),
        ('message_exchange', 'email.send', {},          10),
        ('message_exchange', 'email.wrong_key', {},     11),


        ('files_exchange', 'doesntmatter', { 'File-Type': 'jpeg' },     12),
        ('files_exchange', 'doesntmatter', { 'File-Type': 'jpg' },      13),
        ('files_exchange', 'doesntmatter', { 'File-Type': 'pdf' },      14),
        ('files_exchange', 'doesntmatter', { 'File-Type': 'xml' },      15),
        ('files_exchange', 'doesntmatter', { 'File-Type': 'excel' },    16),
    ]

    right_answer = {
        'q_process': { 2, 1 },
        'q_backend': { 6, 5 },
        'q_email': { 9, 10 },
        'q_process_jpg': { 12, 13 },
        'q_process_pdf': { 14 }
    }

    # чистим очереди перед отправкой тестовых сообщений
    # если не указать vhost, очистятся все очереди во всех vhost-ах
    logger.info('Purging messages')
    _ = facade.purge_all_queues(vhost='/')
    await asyncio.sleep(1)

    logger.info('Sending test messages')
    for ex, key, headers, id_ in to_publish:
        props = pika.BasicProperties(
            headers=headers
        )
        facade.publish(ex, key, str(id_), properties=props)
    
    logger.info('Dumping messages for check')
    received = await checker.dump_messages(vhost='/', wait_time=1)
    filtered = defaultdict(set) # set потому что в данном случае не важен порядок
    for queue_name, queue_messages in received.items():
        for message in queue_messages:
            unique_identifier = int(message['body'])
            filtered[queue_name].add(unique_identifier)


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