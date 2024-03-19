from pika.exchange_type import ExchangeType
from .basicClient import BasicPikaClient
import json
import pika
import argparse
from constants import ROUTING_KEY, EXCHANGE


class Publisher(BasicPikaClient):
    """
        This is responsible to produce the processed message after handling a specific request
        *The producer doesn't really depend on the name of the queue but the channel and the routing key

    """

    def __init__(self, exchange=EXCHANGE, routing_key=ROUTING_KEY.TO_BE.value):
        super().__init__()
        self.exchange = exchange
        self.routing_key = routing_key
        self.declare_exchange()

    def declare_exchange(self):
        print(f"Trying to declare exchange ({self.exchange})...")
        self.channel.exchange_declare(
            exchange=self.exchange, exchange_type=ExchangeType.direct)

    def build_and_push(self):
        """
            This builds the message that needs to be sent to the queue
        """
        message = {
            'status': "done"
        }
        self.publish_message(message)

    def publish_message(self, body):
        """
            This publishes a message to a specific exchange
        """

        """
            TODO: Explore on other parameters that needs to be added here

            https://www.rabbitmq.com/amqp-0-9-1-reference.html#basic.publish 
        """
        self.channel.basic_publish(
            exchange=self.exchange, routing_key=self.routing_key, body=json.dumps(
                body),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Make the message persistent
                headers={'x-retries': 0}
            )
        )
        super().close_channel()
