# RabbitMQ Checks
Check if RabbitMQ bindings are correct.

### How it works
- Send batches of messages into multiple vhosts and exchanges
- Get a snapshot of messages in all queues
- Compare the snaphot with expected result

### Code insights
`MessageChecker` dumps all messages into a dictionary.

`Facade` makes `RabbitMQ` and `Management API` work together.

`Rabbit` connects to `RabbitMQ` via `AMQP` protocol using `pika`.

`Api` connects to `Management API` via `HTTP`.

```
    MessageChecker
            |
        Facade
        /      \
    Rabbit    Api
```