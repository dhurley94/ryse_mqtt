import asyncio
import configparser
import signal
import sys

import aiomqtt

from ble_gatt import Device
from ha_helper import HAHelper

import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

logger = logging.getLogger("ryse_mqtt")

class RyseMqtt(object):
    def __init__(self, config):
        self.queue = asyncio.Queue()
        self.config = config
        self.ha = HAHelper(**config['home_assistant'])
        self.devices = []
        self.last_availability_state = {}  # Track availability per device
        self.address_name_map = {}
        self.shutdown_event = asyncio.Event()
        for friendly_name, address in config['ryse'].items():
            # Explicitly convert fast parameter to boolean
            fast_mode = config['fast'].get(friendly_name)
            fast_bool = fast_mode is not None and str(fast_mode).lower() in ('true', '1', 'yes')
            self.devices.append(Device(address, self.queue, fast_bool))
            self.address_name_map[address] = friendly_name
            self.last_availability_state[address] = None

    async def queue_drain(self):
        while True:
            try:
                async with aiomqtt.Client(**self.config['mqtt']) as mqtt_client:
                    while True:
                        address, event = await self.queue.get()
                        logger.info("Got BT message: %s, %s", address, event)
                        self.queue.task_done()
                        if event == "discover":
                            await mqtt_client.publish(self.ha.discovery_topic(address), self.ha.discovery_payload(address, self.address_name_map[address]))
                        elif event in ("online", "offline"):
                            # Only publish if the availability state changed for this specific device
                            if self.last_availability_state.get(address) != event:
                                await mqtt_client.publish(self.ha.availability_topic(address), event)
                                self.last_availability_state[address] = event
                        elif isinstance(event, int):
                            await mqtt_client.publish(self.ha.position_topic(address), event)
                        else:
                            await mqtt_client.publish(self.ha.state_topic(address), event)
            except aiomqtt.exceptions.MqttCodeError as e:
                logger.error(f"MQTT connection lost in queue_drain: {e}")
                logger.info("Attempting to reconnect in 5 seconds...")
                await asyncio.sleep(5)  # Wait before trying to reconnect

    async def listen(self):
        while True:
            try:
                async with aiomqtt.Client(**self.config['mqtt'], ) as client:
                    await client.subscribe("ryse/+/+/set_position")
                    await client.subscribe("ryse/+/+/set")
                    async for message in client.messages:
                        for device in self.devices:
                            if device.address in str(message.topic):
                                try:
                                    await device.cmd(int(message.payload.decode('utf-8')))
                                except ValueError as e:
                                    logger.error("Invalid position value for %s: %s", device.address, e)
                                except Exception as e:
                                    logger.error("Error sending command to %s: %s", device.address, e)
            except aiomqtt.exceptions.MqttCodeError as e:
                logger.error(f"MQTT connection lost: {e}")
                logger.info("Attempting to reconnect in 5 seconds...")
                await asyncio.sleep(5)  # Wait before trying to reconnect

    async def shutdown(self):
        """Gracefully shutdown all devices and connections."""
        logger.info("Initiating graceful shutdown...")
        self.shutdown_event.set()

        # Disconnect all BLE devices
        for device in self.devices:
            if device.client.is_connected:
                try:
                    logger.info("Disconnecting device %s", device.address)
                    await device.client.disconnect()
                except Exception as e:
                    logger.error("Error disconnecting device %s: %s", device.address, e)

        logger.info("Shutdown complete")

    def run(self):
        loop = asyncio.get_event_loop()

        # Setup signal handlers for graceful shutdown
        def signal_handler(signum, _frame):
            logger.info("Received signal %s", signum)
            loop.create_task(self.shutdown())

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            bt_tasks = []
            logger.info("Found BT Devices: %s", self.devices)
            mqtt = loop.create_task(self.listen())
            for device in self.devices:
               bt_tasks.append(loop.create_task(device.connection_loop()))
            drain = loop.create_task(self.queue_drain())
            tasks = asyncio.gather(
                mqtt, drain, *bt_tasks
            )
            loop.run_until_complete(tasks)
        except (KeyboardInterrupt, SystemExit):
            logger.info("Shutting down...")
            loop.run_until_complete(self.shutdown())
        except Exception as e:
            import traceback
            logger.error("Fatal error: %s", e)
            logger.error(traceback.format_exc())
            loop.run_until_complete(self.shutdown())


if __name__ == "__main__":
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = "config.conf"

    config = configparser.ConfigParser()
    config.read(config_path)
    ryse_mqtt = RyseMqtt(config)
    ryse_mqtt.run()
