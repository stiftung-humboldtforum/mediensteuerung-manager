import asyncio
import random
from typing import Any, Callable
from devices.device import Device
from devices.state import DeviceState

from misc import logger


class TagState:
    OFFLINE = 0
    PARTIAL = 1
    ONLINE = 2


class Tag:
    def __init__(self, manager, **kwargs):
        self.manager = manager
        self.set_data(kwargs)

        self._state = {
            'is_online': TagState.OFFLINE
        }
        self.has_calendar_event = False
        self.last_calendar_method: str | None = None

    def set_data(self, data: dict[str, Any]):
        self.id = data['id']
        self.name = data['name']
        self.description = data['description']
        for key, value in data.items():
            setattr(self, key, value)
        self.devices: list[Device] = [
            device for device in self.manager.devices.values() if device.id in self]

    def __contains__(self, device_id: int):
        return self.manager.devices[device_id].is_tagged(self)

    def is_located(self, location):
        if location is None:
            return False
        return any([device.location['id'] == location.id for device in self.devices if device.location != None])

    @property
    def is_online(self):
        if len(self.devices) == 0:
            return TagState.OFFLINE

        num_online_devices = sum(
            [device.is_online == TagState.ONLINE for device in self.devices])
        if num_online_devices == 0:
            return TagState.OFFLINE
        elif num_online_devices == len(self.devices):
            return TagState.ONLINE
        else:
            return TagState.PARTIAL

    @property
    def network_switches(self) -> list[Device]:
        return [device for device in self.devices if device.role['name'] == 'Netzwerkswitch']

    @property
    def pdus(self) -> list[Device]:
        return [device for device in self.devices if device.role['name'] == 'PDU']

    @property
    def display_devices(self) -> list[Device]:
        return [device for device in self.devices if device.role['name'] in ['Monitor', 'Projektor']]

    @property
    def computers(self) -> list[Device]:
        return [device for device in self.devices if device.role['name'] in ['Medienstation', 'PC']]

    @property
    def other_devices(self) -> list[Device]:
        return [device for device in self.devices if device not in [*self.network_switches, *self.pdus, *self.display_devices, *self.computers]]

    async def call(self, devices, method_name):
        devices = [d for d in devices if method_name in d.capabilities]
        async with asyncio.TaskGroup() as tg:
            for device in devices:
                tg.create_task(getattr(device, method_name)())
                await asyncio.sleep(random.random())
        if len(devices):
            logger.debug('%s %s for %s', self.name,
                         method_name, [d.name for d in devices])

    async def wait_for(self, devices, *states, timeout=None):
        if timeout is None:
            timeout = max([max(device.timeouts.values())
                          for device in devices])
        try:
            async with asyncio.timeout(timeout):
                async with asyncio.TaskGroup() as tg:
                    [tg.create_task(d.wait_for(*states)) for d in devices]
        except Exception as e:
            logger.exception(e)

    async def call_and_wait_for(self, devices, method_name, *states):
        await self.call(devices, method_name)
        try:
            timeout = max([device.timeouts[method_name]
                          for device in devices if method_name in device.timeouts])
        except:
            timeout = 300
        await self.wait_for(devices, *states, timeout=timeout)

    async def wake(self, **__):
        method_name = 'wake'
        state = DeviceState.ON
        if len(self.pdus):
            await self.call_and_wait_for(self.pdus, method_name, state)
            logger.debug('Tag %s PDUs are ON', self.name)
        if len(self.network_switches):
            await self.call_and_wait_for(self.network_switches, method_name, state)
            logger.debug('Tag %s Network Switches are ON', self.name)
        if len(self.display_devices):
            await self.call_and_wait_for(self.display_devices, method_name, state)
            logger.debug('Tag %s Display Devices are ON', self.name)
        if len(self.computers):
            await self.call(self.computers, method_name)
            logger.debug('Tag %s Waking computers', self.name)
        if len(self.other_devices):
            await self.call(self.other_devices, method_name)
            logger.debug('Tag %s Waking other devices', self.name)

    async def shutdown(self, **__):
        method_name = 'shutdown'
        if len(self.computers):
            await self.call_and_wait_for(self.computers, method_name, DeviceState.OFF)
            logger.debug('Tag %s Computers are OFF', self.name)
        if len(self.display_devices):
            await self.call_and_wait_for(self.display_devices, method_name, DeviceState.OFF, DeviceState.PARTIAL)
            logger.debug('Tag %s Display devices are OFF', self.name)
        if len(self.other_devices):
            await self.call(self.other_devices, method_name)
            logger.debug('Tag %s Shutdown other devices', self.name)
        if len(self.network_switches):
            await self.call(self.network_switches, method_name)
            logger.debug('Tag %s Shutdown Network Switches', self.name)
        if len(self.pdus):
            await self.call(self.pdus, method_name)
            logger.debug('Tag %s Shutdown PDUs', self.name)

    async def cancel(self, **__):
        for device in self.devices:
            await device.cancel()

    def __getattr__(self, __name: str) -> Callable:
        async def method(from_knx=False, **kwargs):
            if from_knx and self.has_calendar_event and self.last_calendar_method == 'shutdown':
                return
            logger.debug('%s tag %s', __name, self.name)
            for device in self.devices:
                if __name in device.capabilities:
                    await getattr(device, __name)(**kwargs)
        return method

    async def fetch(self):
        await self.manager.tag_event(self.id, 'is_online', self.is_online)

    async def calendar_edge(self, edge, method_name):
        if edge == 'start':
            self.has_calendar_event = True
        else:
            self.has_calendar_event = False
        self.last_calendar_method = method_name

    async def scram(self, **__):
        logger.error('BMZ Scram %s', self.name)
        logger.error('BMZ Scram %s devices=%d computers=%d displays=%d mutable=%d',
                 self.name, len(self.devices), len(self.computers),
                 len(self.display_devices),
                 len([d for d in self.computers if 'mute' in d._capabilities]))
        mutable = [
            device for device in self.computers if 'mute' in device._capabilities]
        await self.call(mutable, 'mute')
        other = [
            device for device in self.computers if 'mute' not in device._capabilities]

        await self.call_and_wait_for(other, 'shutdown', DeviceState.OFF)
        await self.call(self.display_devices, 'shutdown')

    async def unscram(self, **__):
        logger.error('BMZ Unscram %s', self.name)
        unmutable = [
            device for device in self.devices if 'unmute' in device._capabilities]
        other = [
            device for device in self.devices if 'unmute' not in device._capabilities]
        await self.call(unmutable, 'unmute')
        await self.call_and_wait_for(self.display_devices, 'wake', DeviceState.ON)
        await self.call_and_wait_for(other, 'wake', DeviceState.ON)
