import asyncio

from aiomqtt import Client

from misc import logger


class PowerMixin:
    _capabilities = []

    def __init__(self,
                 manager,
                 client: Client,
                 *_,
                 **__):
        self.manager = manager
        self.client = client
        self.power_task = None

    async def set_power(self, state: bool):
        power_ports = getattr(self, 'power_ports', [])
        has_switched = False
        for power_port in power_ports:
            for power_feed in power_port['link_peers']:
                try:
                    power_panel = power_feed['power_panel']
                    powerfeed_id = int(power_feed['name'])
                    pdu = [device
                           for device in self.manager.devices.values()
                           if device.name == power_panel['name']][0]
                    has_switched = await pdu.write_powerfeed(powerfeed_id, state)
                except Exception as e:
                    logger.exception(self.name)
                    await self._handle_exception(e)
        return has_switched

    async def async_power_off(self, wait):
        await asyncio.sleep(wait)
        await self.set_power(False)

    async def async_power_on(self, wait):
        await asyncio.sleep(wait)
        await self.set_power(True)

    async def async_power_cycle(self, wait, off_wait=None):
        # off_wait = Dauer der stromlosen Phase, ohne Angabe wie wait
        await asyncio.sleep(wait)
        await self.set_power(False)
        await asyncio.sleep(off_wait if off_wait is not None else wait)
        await self.set_power(True)

    def power_on(self, wait=30):
        self._cancel_existing_power_task()
        self.power_task = asyncio.create_task(self.async_power_on(wait))

    def power_off(self, wait=30):
        self._cancel_existing_power_task()
        self.power_task = asyncio.create_task(self.async_power_off(wait))

    def power_cycle(self, wait=10, off_wait=None):
        self._cancel_existing_power_task()
        self.power_task = asyncio.create_task(
            self.async_power_cycle(wait, off_wait))

    def _cancel_existing_power_task(self):
        if self.power_task and not self.power_task.done():
            self.power_task.cancel()
