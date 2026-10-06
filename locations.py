import asyncio
import random
from typing import Any, Callable
from misc import logger
from functools import cached_property


class LocationState:
    OFFLINE = 0
    PARTIAL = 1
    ONLINE = 2


class KNXState:
    UNDEFINED = -1
    OFF = 0
    ON = 1


class Location:
    def __init__(self, manager, **kwargs):
        self.manager = manager
        self.set_data(kwargs)

        self._state = {
            'is_online': LocationState.OFFLINE,
            'knx_switch': KNXState.UNDEFINED
        }
        self.has_calendar_event = False
        self.last_calendar_method: str | None = None
        self.devices = [dev for dev in self.manager.devices.values(
        ) if dev.location != None and dev.location['id'] == self.id]

    def set_data(self, data: dict[str, Any]):
        self.id = data['id']
        self.name = data['name']
        for key, value in data.items():
            if key != 'tags':
                setattr(self, key, value)

    def __contains__(self, item: dict):
        return getattr(self.manager, item['type'])[item['id']].is_located(self)

    @cached_property
    def tags(self) -> list:
        return [tag for tag in self.manager.tags.values()
                if {'type': 'tags', 'id': tag.id} in self]

    @property
    def is_online(self):
        if len(self.devices) == 0:
            return LocationState.OFFLINE

        num_online_devices = sum(
            [device.is_online == LocationState.ONLINE for device in self.devices])
        if num_online_devices == 0:
            return LocationState.OFFLINE
        elif num_online_devices == len(self.devices):
            return LocationState.ONLINE
        else:
            return LocationState.PARTIAL

    @property
    def knx_state(self):
        return self._state['knx_switch']

    async def set_knx_state(self, value):
        if self._state['knx_switch'] != value:
            self._state['knx_switch'] = value
            await self.manager.location_event(self.id, 'knx_state', self.knx_state)

    def __getattr__(self, __name: str) -> Callable:
        async def method(from_knx=False, **kwargs):
            if from_knx and self.has_calendar_event and self.last_calendar_method == 'shutdown':
                return
            logger.debug('%s location %s', __name, self.name)
            elements = [tag for tag in self.tags
                        if tag.description == self.manager.config['group_by_tag_description']['value']]
            async with asyncio.TaskGroup() as tg:
                for element in elements:
                    tg.create_task(getattr(element, __name)(**kwargs))
                    await asyncio.sleep(random.random())
        return method

    async def fetch(self):
        await self.manager.location_event(self.id, 'is_online', self.is_online)
        await self.manager.location_event(self.id, 'knx_state', self.knx_state)

    async def calendar_edge(self, edge, method_name):
        if edge == 'start':
            self.has_calendar_event = True
        else:
            self.has_calendar_event = False
        self.last_calendar_method = method_name

    async def knx_switch(self, **kwargs):
        logger.error('KNX %s %s', self.name, kwargs)
        logger.error('KNX %s %s', self.name, kwargs['state'])
        if kwargs['state']:
            # just a debugging log (change: DA 4.6. 2025)
            logger.debug('KNX ON signal received for %s', self.name)
            await self.set_knx_state(KNXState.ON)
            if self.has_calendar_event and self.last_calendar_method == 'shutdown':
                return
            else:
                # prevent knx from waking devices (change: DA 4.6. 2025)
                logger.info('KNX ON signal received for %s - will be ignored', self.name)
                return # await self.wake(from_knx=True)
        else:
            await self.shutdown()
            await self.set_knx_state(KNXState.OFF)

    async def cancel(self, **__):
        for device in self.devices:
            await device.cancel()

    async def scram(self, **__):
        logger.error('BMZ Scram %s', self.name)
        elements = [tag for tag in self.tags
                    if tag.description == 'E-Nummer']  # TODO make this configurable
        for element in elements:
            await element.scram()

    async def unscram(self, **__):
        logger.error('BMZ Unscram %s', self.name)
        elements = [tag for tag in self.tags
                    if tag.description == 'E-Nummer']  # TODO make this configurable
        for element in elements:
            await element.unscram()
