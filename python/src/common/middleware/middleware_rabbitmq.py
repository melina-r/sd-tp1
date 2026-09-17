import pika

from .middleware import (
    MessageMiddlewareCloseError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareQueue,
)


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    def __init__(self, host, queue_name):
        self.queue_name = queue_name
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=queue_name, durable=True)

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
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

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
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def close(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    def __init__(self, host, exchange_name, routing_keys):
        self.exchange_name = exchange_name
        self.routing_keys = routing_keys
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        self.channel = self.connection.channel()
        self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='topic')

        result = self.channel.queue_declare('', exclusive=True)
        self.queue_name = result.method.queue

        for binding_key in self.routing_keys:
            self.channel.queue_bind(
                exchange=self.exchange_name, queue=self.queue_name, routing_key=binding_key)

    def send(self, message):
        try:
            self.channel.basic_publish(exchange=self.exchange_name, routing_key=self.routing_keys[0], body=message)
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

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
        except Exception as e:
            raise MessageMiddlewareMessageError() from e


    def stop_consuming(self):
        try:
            self.channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareMessageError() from e

    def close(self):
        try:
            if self.connection and self.connection.is_open:
                self.connection.close()
        except pika.exceptions.AMQPConnectionError:
            raise MessageMiddlewareDisconnectedError()
        except Exception as e:
            raise MessageMiddlewareCloseError() from e
