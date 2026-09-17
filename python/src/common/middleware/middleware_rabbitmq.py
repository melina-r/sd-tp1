import pika

from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareQueue,
)


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name, durable=True)
        self.queue_name = queue_name

    def send(self, message):
        try:
            self.channel.basic_publish(
                exchange="",
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent
                ),
            )
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except pika.exceptions.AMQPChannelError:
            raise MessageMiddlewareCloseError()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e

    def start_consuming(self, on_message_callback):
        def callback(channel, method, properties, body):
            ack = lambda: channel.basic_ack(delivery_tag=method.delivery_tag)
            nack = lambda: channel.basic_nack(delivery_tag=method.delivery_tag)
            on_message_callback(body, ack, nack)

        try:
            self.channel.basic_consume(
                queue=self.queue_name, on_message_callback=callback
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except pika.exceptions.AMQPChannelError:
            raise MessageMiddlewareCloseError()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except pika.exceptions.AMQPChannelError:
            raise MessageMiddlewareCloseError()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e

    def close(self):
        try:
            self.connection.close()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except pika.exceptions.AMQPChannelError:
            raise MessageMiddlewareCloseError()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        pass

    def send(self, message):
        pass

    def start_consuming(self, on_message_callback):
        pass

    def stop_consuming(self):
        pass

    def close(self):
        pass
