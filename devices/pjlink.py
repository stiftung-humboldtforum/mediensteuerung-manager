import asyncio
import os

from aiopjlink import PJLink as PJLinkInterface, Power

from misc import logger, memoize

from .device import Device, DeviceState
from .icmpable import ping_address

WATCH_INTERVAL = 10
WAKE_INTERVAL = 30
SHUTDOWN_INTERVAL = 30

initial_state = {
    'errors': {},
    'lamps': [],
    'ires': '',
    'warming': False,
    'cooling': False
}


class PJLink(Device):
    _capabilities = ['wake', 'shutdown']

    def __init__(self, *args, max_time_to_wake: float = 900, max_time_to_shutdown=900, connection_timeout=10, **kwargs):
        super().__init__(*args, **kwargs)
        self._state['should_wake'] = False
        self._state['should_shutdown'] = False
        self._reset_state()

        self.timeouts['wake'] = max_time_to_wake
        self.timeouts['shutdown'] = max_time_to_shutdown
        self.connection_timeout = connection_timeout

        self.event.append(self.online_event)

        ip = getattr(self, 'primary_ip')
        address = ip['address'].split('/')[0]
        self.ip = address

        self._interface = None
        self.update_methods.append(('PJLink watch', self._watch))

    async def _get_interface(self):
        async with self.lock:
            _interface = PJLinkInterface(
                address=self.ip,
                password=os.environ['PJLINK_PASSWORD'],
                timeout=self.connection_timeout)
        return _interface

    def _reset_state(self):
        for key, value in initial_state.items():
            self._state[key] = value

    async def online_event(self, _, event_type, value):
        if event_type == 'is_online':
            await self.set_should_wake(self.should_wake and value != DeviceState.ON)
            await self.set_should_shutdown(self.should_shutdown and value not in [DeviceState.OFF, DeviceState.PARTIAL])
            if value != DeviceState.ON:
                self._reset_state()

    async def _set_power_state(self, power_state: Power.State):
        match power_state:
            case Power.State.ON:
                await self.set_is_online(DeviceState.ON)
                self._state['warming'] = False
                self._state['cooling'] = False
            case Power.State.WARMING:
                await self.set_is_online(DeviceState.PARTIAL)
                self._state['warming'] = True
                self._state['cooling'] = False
            case Power.State.COOLING:
                await self.set_is_online(DeviceState.PARTIAL)
                self._state['warming'] = False
                self._state['cooling'] = True
            case Power.State.OFF:
                await self.set_is_online(DeviceState.PARTIAL)
                self._state['warming'] = False
                self._state['cooling'] = False
            case _:
                await self.set_is_online(DeviceState.OFF)
                self._state['warming'] = False
                self._state['cooling'] = False

    @memoize(WATCH_INTERVAL)
    async def _watch(self):
        if self._interface is None:
            self._interface = await self._get_interface()
        if await ping_address(self.ip):
            async with self._interface as interface:
                power_state = await interface.power.get()
                await self._set_power_state(power_state)
                await self._watch_status(interface)
        else:
            await self.set_is_online(DeviceState.OFF)

    async def _update_errors(self, errors):
        has_error_event = False
        for key, value in errors.items():
            error_name = key.value
            error_value = value.name.lower()
            if error_name not in self._state['errors'] or self._state['errors'][error_name] != error_value:
                has_error_event = True
                self._state['errors'][error_name] = error_value
        if has_error_event:
            await self.event('errors', self._state['errors'])

    async def _update_lamps(self, lamps):
        new_lamps = [(hours, int(state.value)) for hours, state in lamps]
        if new_lamps != self._state['lamps']:
            self._state['lamps'] = new_lamps
            await self.event('lamps', self._state['lamps'])
            
    async def _update_class(self, interface):
        try:
            interface_class = await interface.info.pjlink_class()
            self._state['class'] = interface_class.value
        except:
            self._state['class'] = 1

    async def _update_ires(self, interface):
        try:
            x, y = await interface.sources.resolution()
            ires = f'{x}x{y}'
            if ires != self._state['ires']:
                self._state['ires'] = ires
                await self.event('ires', self._state['ires'])
        except:
            pass

    async def _watch_status(self, interface):
        if 'class' not in self._state:
            await self._update_class(interface)
        try:
            errors = await interface.errors.query()
        except:
            errors = self._state['errors']
        try:
            lamps = await interface.lamps.status()
        except:
            lamps = []
    
        try:
            await self._update_lamps(lamps)
        except Exception as e:
            await self._handle_exception(e)

        try:
            await self._update_errors(errors)
        except Exception as e:
            await self._handle_exception(e)

        if self._state['class'] == 2:
            await self._update_ires(interface)

    async def _wake(self):
        async def inner():
            async with self._interface as interface:
                logger.debug(
                    'Authentication succeeded, set_power on')
                await interface.power.turn_on()
        async with asyncio.timeout(self.timeouts['wake']):
            while self.should_wake:
                if self.is_online in [DeviceState.OFF, DeviceState.PARTIAL]:
                    await self._try_method(inner)
                    await asyncio.sleep(WAKE_INTERVAL)
                elif self.is_online == DeviceState.ON:
                    await self.set_should_wake(False)

    async def _shutdown(self):
        async def inner():
            async with self._interface as interface:
                logger.debug(
                    'Authentication succeeded, set_power off')
                await interface.power.turn_off()
        async with asyncio.timeout(self.timeouts['shutdown']):
            while self.should_shutdown:
                if self.is_online == DeviceState.ON:
                    logger.debug('Try shutdown %s', self.name)
                    await self._try_method(inner)
                    self.power_off(300)
                    await asyncio.sleep(SHUTDOWN_INTERVAL)
                elif self.is_online in [DeviceState.OFF, DeviceState.PARTIAL]:
                    await self.set_should_shutdown(False)

    @property
    def should_wake(self) -> bool:
        return self._state['should_wake']

    async def set_should_wake(self, value: bool):
        self._state['should_wake'] = value
        await self.event('should_wake', value)

    @property
    def should_shutdown(self) -> bool:
        return self._state['should_shutdown']

    async def set_should_shutdown(self, value: bool):
        self._state['should_shutdown'] = value
        await self.event('should_shutdown', value)

    async def wake(self, *_, **__):
        self._cancel_existing_power_task()
        await self.cancel()
        logger.debug('Waking %s', self.name)
        has_pdu = await self.set_power(True)

        async def inner():
            if has_pdu:
                logger.debug('PDU switched. Deferring wake. %s', self.name)
                await asyncio.sleep(10)
            await self.set_should_wake(self.is_online in [DeviceState.OFF, DeviceState.PARTIAL])
            await self._wake()

        if 'wake' in self.tasks:
            self.tasks['wake'].cancel()
        task = asyncio.create_task(self._try_method(
            inner, error_cb=self.set_should_wake(False)))
        self.tasks['wake'] = task
        task.add_done_callback(self._delete_task('wake'))

    async def shutdown(self, *_, **__):
        await self.cancel()
        logger.debug('Shutting down %s', self.name)
        await self.set_should_shutdown(True)
        if 'shutdown' in self.tasks:
            self.tasks['shutdown'].cancel()
        task = asyncio.create_task(self._try_method(
            self._shutdown, error_cb=self.set_should_shutdown(False)))
        self.tasks['shutdown'] = task

        def power_off_done(_):
            self.power_off()
            self._delete_task('shutdown')
        task.add_done_callback(power_off_done)

    async def fetch(self):
        await super().fetch()
        await self.event('should_wake', self.should_wake)
        await self.event('errors', self._state['errors'])
        await self.event('lamps', self._state['lamps'])
        await self.event('ires', self._state['ires'])
