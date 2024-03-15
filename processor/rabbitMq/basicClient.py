import ssl
import pika
from dotenv import load_dotenv
import os

"""
TODO: https://www.rabbitmq.com/production-checklist.html

Need to check against this list, if this setup meets all the criteria

"""

load_dotenv()


class BasicPikaClient:

    def __init__(self):

        # SSL Context for TLS configuration of Amazon MQ for RabbitMQ
        AMQP_USER = os.environ.get('AMQP_USER')
        AMQP_PASS = os.environ.get('AMQP_PASS')
        AMQP_HOST = os.environ.get('AMQP_HOST')
        AMQP_PORT = os.environ.get('AMQP_PORT')

        # url = "amqp://guest:guest@localhost:5672/%2F"
        url = f"amqps://{AMQP_USER}:{AMQP_PASS}@{AMQP_HOST}:{AMQP_PORT}/%2F"
        # print(url)

        parameters = pika.URLParameters(url)
        parameters.heartbeat = 300
        parameters.blocked_connection_timeout = 180

        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        ssl_context.set_ciphers('ECDHE+AESGCM:!ECDSA')

        parameters.ssl_options = pika.SSLOptions(
            context=ssl_context) if AMQP_PORT == "5671" else None

        self.connection = pika.BlockingConnection(parameters)
        # print("connection", self.connection)
        self.channel = self.connection.channel()
        # print("channel",self.channel)

    def close_channel(self):
        self.channel.close()
