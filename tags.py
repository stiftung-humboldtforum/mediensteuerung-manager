import asyncio
import random
from typing import Any, Callable
from devices.device import Device
from devices.state import DeviceState

from misc import logger


def _role_name(device: Device):
    # device.role is the NetBox role dict (or None if the device has no role).
    role = getattr(device, 'role', None)
    return role['name'] if isinstance(role, dict) else None


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
        return [device for device in self.devices if _role_name(device) == 'Netzwerkswitch']

    @property
    def pdus(self) -> list[Device]:
        return [device for device in self.devices if _role_name(device) == 'PDU']

    @property
    def display_devices(self) -> list[Device]:
        return [device for device in self.devices if _role_name(device) in ['Monitor', 'Projektor']]

    @property
    def computers(self) -> list[Device]:
        return [device for device in self.devices if _role_name(device) in ['Medienstation', 'PC']]

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

    @property
    def _av_devices(self) -> list[Device]:
        # Everything a fire alarm should act on: all tagged devices EXCEPT the
        # power/network infrastructure (PDUs, network switches), which stays up.
        infra = [*self.pdus, *self.network_switches]
        return [device for device in self.devices if device not in infra]

    async def _scram_call(self, devices, method_name):
        """Fire-safety dispatch. Filters on `_capabilities` (NOT the public
        `capabilities` property, which returns [] for 'ctrl mon' control-only
        devices to hide them from the UI — they must NOT be skipped during an
        alarm). Each device is dispatched independently with its exception
        swallowed+logged, so one flaky device can never abort the alarm for the
        rest — a plain TaskGroup cancels the whole batch (and, via Location's
        sequential loop, every later element) on the first raise. Returns the
        devices actually dispatched to."""
        targets = [d for d in devices if method_name in d._capabilities]

        async def _dispatch(device):
            try:
                await getattr(device, method_name)()
            except Exception:
                logger.exception('Tag %s scram-%s failed for %s',
                                 self.name, method_name, device.name)
        await asyncio.gather(*[_dispatch(d) for d in targets])
        if targets:
            logger.debug('Tag %s scram-%s for %s', self.name,
                         method_name, [d.name for d in targets])
        return targets

    async def _scram_power(self, devices, state):
        """Last-resort power control for devices that can be neither muted nor
        shut down (reboot-only signage players, wake-only, driverless): toggle
        their PDU feed if they have one. Best-effort, per-device guarded."""
        switched = []
        for device in devices:
            try:
                if await device.set_power(state):
                    switched.append(device)
            except Exception:
                logger.exception('Tag %s scram power=%s failed for %s',
                                 self.name, state, device.name)
        if switched:
            logger.debug('Tag %s scram power=%s for %s', self.name,
                         state, [d.name for d in switched])
        return switched

    async def scram(self, **__):
        logger.error('BMZ Scram %s', self.name)
        # Classify by CAPABILITY, not by NetBox role: role classification
        # (computers/display_devices) misses the live roles ('Video Player',
        # 'Projektion', 'Medienstation *' variants …) entirely. Mute what can
        # mute; shut down what can shut down; and for devices that can do
        # NEITHER (reboot-only signage, wake-only, driverless) cut their PDU
        # feed as a last resort — else they keep playing through the alarm.
        # Infrastructure (PDUs, switches) is deliberately left up.
        av = self._av_devices
        mutable = [d for d in av if 'mute' in d._capabilities]
        shutdownable = [d for d in av
                        if 'mute' not in d._capabilities and 'shutdown' in d._capabilities]
        uncontrollable = [d for d in av
                          if 'mute' not in d._capabilities and 'shutdown' not in d._capabilities]
        muted = await self._scram_call(mutable, 'mute')
        shut = await self._scram_call(shutdownable, 'shutdown')
        powercut = await self._scram_power(uncontrollable, False)
        logger.error('BMZ Scram %s av=%d muted=%d shutdown=%d powercut=%d unreached=%d',
                     self.name, len(av), len(muted), len(shut), len(powercut),
                     len(uncontrollable) - len(powercut))
        # Wait (bounded) only for devices we actually told to shut down; an
        # explicit timeout avoids wait_for's max([]) ValueError when a dispatched
        # device happens to carry an empty timeouts dict.
        if shut:
            await self.wait_for(shut, DeviceState.OFF, DeviceState.PARTIAL, timeout=300)

    async def unscram(self, **__):
        logger.error('BMZ Unscram %s', self.name)
        av = self._av_devices
        unmutable = [d for d in av if 'unmute' in d._capabilities]
        wakeable = [d for d in av
                    if 'unmute' not in d._capabilities and 'wake' in d._capabilities]
        powerable = [d for d in av
                     if 'unmute' not in d._capabilities and 'wake' not in d._capabilities]
        await self._scram_call(unmutable, 'unmute')
        woken = await self._scram_call(wakeable, 'wake')
        await self._scram_power(powerable, True)
        if woken:
            await self.wait_for(woken, DeviceState.ON, timeout=300)
