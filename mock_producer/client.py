from json import dumps
from logging import INFO, basicConfig, getLogger
from random import randint
from time import sleep
from typing import NotRequired, Self, TypedDict

from paho.mqtt.client import Client as MQTTClient
from paho.mqtt.enums import CallbackAPIVersion

basicConfig(level=INFO)
logger = getLogger("Device3")


class Message(TypedDict):
    message_type: str
    device_id: str
    value: NotRequired[int]


class ProducerClient:
    device_id: str
    broker_address: str
    client: MQTTClient
    is_connected: bool

    def __init__(self: Self, broker_address: str, device_id: str) -> None:
        self.broker_address = broker_address
        self.client = MQTTClient(CallbackAPIVersion.VERSION2, device_id)
        self.device_id = device_id
        self.is_connected = False

    def connect(self: Self) -> None:
        while not self.is_connected:
            try:
                self.client.connect(self.broker_address)
                logger.info("Connected successfully")
                self.is_connected = True
            except Exception as e:
                logger.error("MQTT Connection Error: %s" % e)
                sleep(5)

    def _send_message(self: Self, message_type: str, value: int | None = None) -> None:
        message = Message(message_type=message_type, device_id=self.device_id)
        if value:
            message["value"] = value
        self.client.publish("connect", dumps(message), retain=False)

    def send_update_message(self: Self) -> None:
        self._send_message("update", randint(0, 100))

    def send_register_message(self: Self) -> None:
        self._send_message("registration")

    def send_deregister_message(self: Self) -> None:
        self._send_message("deregistration")

    def disconnect(self: Self) -> None:
        self.client.disconnect()
