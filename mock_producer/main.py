import os
from logging import INFO, basicConfig, getLogger
from pathlib import Path
from time import sleep, time

import environ
from client import ProducerClient

env = environ.Env(
    # set casting, default value
    DEBUG=(bool, False)
)

BASE_DIR = Path(__file__).resolve().parent
environ.Env.read_env(os.path.join(BASE_DIR, ".env"))

basicConfig(level=INFO)
logger = getLogger("Device3")

broker_address = env("BROKER_ADDRESS")

device_id = "device3"
is_registered = False

client = ProducerClient(broker_address, device_id)
client.connect()

if client.is_connected:
    logger.info("Connected to MQTT broker")
else:
    logger.error("Failed to connect to MQTT broker")

while True:
    try:
        if not is_registered:
            client.send_register_message()
            is_registered = True
        update_end_time = time() + 120
        while time() < update_end_time:
            client.send_update_message()
            sleep(5)
        client.send_deregister_message()
        is_registered = False
        sleep(60)
    except KeyboardInterrupt:
        break
    except Exception as e:
        logger.error("Exception: %s" % e)
        if not client.is_connected:
            logger.error("Failed to reconnect to MQTT broker")

client.disconnect()
