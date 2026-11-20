import ssl
import pika
from dotenv import load_dotenv
import os

"""
TODO: https://www.rabbitmq.com/production-checklist.html

Need to check against this list, if this setup meets all the criteria

"""

load_dotenv()


def amqp_ssl_options(host):
    """
    TLS settings for the broker connection.

    With AMQP_CA_CERT (the CA certificate's PEM text, e.g. from an ECS secret) or AMQP_CA_FILE (a path) set, the
    broker's certificate and hostname are checked against that CA, with TLS 1.2 or later: a broker that can't prove
    it holds the certificate for AMQP_HOST is refused, so credentials and job results never go to an impostor.
    Without either, the connection stays as before, encrypted but unverified (Amazon MQ, until the cutover to the
    self-managed broker), and a warning is logged.
    """
    ca_cert = os.environ.get("AMQP_CA_CERT") or None
    ca_file = os.environ.get("AMQP_CA_FILE") or None
    if ca_cert or ca_file:
        context = ssl.create_default_context(cafile=ca_file, cadata=ca_cert)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        return pika.SSLOptions(context, server_hostname=host)

    print("WARNING: AMQP_CA_CERT / AMQP_CA_FILE not set; the broker's TLS certificate is not verified")
    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    context.set_ciphers("ECDHE+AESGCM:!ECDSA")
    return pika.SSLOptions(context)


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

        parameters.ssl_options = amqp_ssl_options(AMQP_HOST) if AMQP_PORT == "5671" else None

        self.connection = pika.BlockingConnection(parameters)
        # print("connection", self.connection)
        self.channel = self.connection.channel()
        # print("channel",self.channel)

    def close_channel(self):
        self.channel.close()
