from typing import Dict
import pika
import json

class Publisher:

    def __init__(self) -> None:
        self.__host = "localhost"
        self.__port = 5672
        self.__username = "guest"
        self.__pasword = "guest"
        self.__exchange = "data_exchange"
        self.__routingkey = ""
        self.__channel = self.__create_channel()
    
    def __create_channel(self):
        connection_parameters = pika.ConnectionParameters(
            host=self.__host,
            port=self.__port,
            credentials=pika.PlainCredentials(
                username=self.__username,
                password=self.__pasword
            )
        )

        channel = pika.BlockingConnection(connection_parameters).channel()

        return channel
    
    def send_msg(self, msg: Dict):
        self.__channel.basic_publish(
            exchange=self.__exchange,
            routing_key=self.__routingkey,
            body=json.dumps(msg),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )

publisher = Publisher()
publisher.send_msg({"MARCOS": "VINICIUS"})