import asyncio
import json
import time

from misc import logger, memoize

from .device import DeviceState
from .wolable import WOLable

PING_INTERVAL = 5
PING_MAX_INTERVAL = 30
SHUTDOWN_INTERVAL = 30

initial_state = {
    'temperatures': {},
    'fans': {},
    'boot_time': None,
    'uptime': None,
    'display': None,
    'errors': {},
    'is_muted': 1
}


class Computer(WOLable):
    _capabilities = ['wake', 'shutdown', 'reboot']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, should_icmp=False, **kwargs)
        self.probe_address = f'manager/{self.name}'
        self._state['should_shutdown'] = False
        self._state['should_reboot'] = False
        for key, val in initial_state.items():
            self._state[key] = val
        self.timeouts['shutdown'] = 900
        self.timeouts['reboot'] = 900
        self.last_ping_time = 0
        self.update_methods.append(('MQTT Watch online', self._watch_online))
        self.power_task = None
        self.probe_topic = f'probe/{self.name}/+'

        ip = getattr(self, 'primary_ip')
        address = ip['address'].split('/')[0]
        self.ip = address

    def __getattr__(self, __name):
        if __name.startswith('on_'):
            name = '_'.join(__name.split('_')[1:])

            async def method(args):
                method.__name__ = __name
                payload = json.loads(args)
                try:
                    if name not in self._state:
                        return
                    result = payload['data']['result']
                    if self._state[name] != result:
                        self._state[name] = result
                        await self.event(name, self._state[name])
                except Exception as e:
                    if 'error' in payload:
                        raise Exception(payload['error']['message'],
                                        *payload['error']['errors'])
                    else:
                        await self._handle_exception(e)
            return method
        else:
            return getattr(self, __name)

    async def on_connect(self):
        await self.client.subscribe(self.probe_topic)

    async def on_temperatures(self, args):
        try:
            payload = json.loads(args)
            if 'data' in payload:
                self._state['temperatures'] = payload['data']['result']
                await self.event('temperatures', self._state['temperatures'])
            elif 'error' in payload:
                raise Exception(payload['error']['message'],
                                *payload['error']['errors'])
        except Exception as e:
            await self._handle_exception(e)

    async def on_fans(self, args):
        try:
            payload = json.loads(args)
            if 'data' in payload:
                self._state['fans'] = json.loads(args)['data']['result']
                await self.event('fans', self._state['fans'])
            elif 'error' in payload:
                raise Exception(payload['error']['message'],
                                *payload['error']['errors'])
        except Exception as e:
            await self._handle_exception(e)

    async def on_shutdown(self, _):
        pass
        # await self.set_is_online(DeviceState.PARTIAL)
        # try:
        #     async with asyncio.timeout(60):
        #         while await ping_address(self.ip):
        #             await asyncio.sleep(5)
        # except Exception as e:
        #     logger.exception(e)
        #     return
        # await self.set_is_online(DeviceState.OFF)
        # await self.set_should_shutdown(False)
        # await self.client.unsubscribe(self.probe_topic)

    async def online_event(self, _, event_type, value):
        if event_type == 'is_online':
            await self.set_should_wake(self.should_wake and value != DeviceState.ON)
            await self.set_should_shutdown(self.should_shutdown and value != DeviceState.OFF)
            await self.set_should_reboot(self.should_reboot and value != DeviceState.ON)
            if value != DeviceState.ON:
                for key, val in initial_state.items():
                    self._state[key] = val
                await self.event('boot_time', self._state['boot_time'])
                await self.event('uptime', self._state['uptime'])
                await self.event('temperatures', self._state['temperatures'])
                await self.event('fans', self._state['fans'])
                await self.event('errors', self._state['errors'])

    @property
    def should_shutdown(self) -> bool:
        return self._state['should_shutdown']

    async def set_should_shutdown(self, value: bool):
        self._state['should_shutdown'] = value
        await self.event('should_shutdown', value)

    @property
    def should_reboot(self) -> bool:
        return self._state['should_reboot']

    async def set_should_reboot(self, value: bool):
        self._state['should_reboot'] = value
        await self.event('should_reboot', value)

    @memoize(PING_INTERVAL)
    async def _watch_online(self):
        is_online = time.time() - self.last_ping_time < PING_MAX_INTERVAL
        if is_online:
            if not self.should_reboot:
                await self.set_is_online(DeviceState.ON)
        else:
            await self.set_is_online(DeviceState.OFF)
            await self.client.unsubscribe(self.probe_topic)

    # @memoize(SHUTDOWN_INTERVAL, immediate_key='should_shutdown')
    # @timeout('shutdown')
    async def _shutdown(self):
        async with asyncio.timeout(self.timeouts['shutdown']):
            while self.should_shutdown:
                if self.is_online == DeviceState.ON:
                    self.last_ping_time = 0
                    await self.client.publish(f'{self.probe_address}/shutdown', qos=1)
                    await asyncio.sleep(SHUTDOWN_INTERVAL)
                elif self.is_online == DeviceState.OFF:
                    await self.set_should_shutdown(False)
                    await self.client.unsubscribe(self.probe_topic)

    # @memoize(SHUTDOWN_INTERVAL, immediate_key='should_reboot')
    # @timeout('reboot')
    async def _reboot(self):
        async with asyncio.timeout(self.timeouts['reboot']):
            while self.should_reboot:
                if self.is_online == DeviceState.ON:
                    await self.client.publish(f'{self.probe_address}/reboot', qos=1)
                    await asyncio.sleep(SHUTDOWN_INTERVAL)

    async def on_connected(self, *_):
        await self.set_is_online(DeviceState.ON)
        self.last_ping_time = time.time()

    async def on_ping(self, *_):
        if not self.should_reboot:
            await self.set_is_online(DeviceState.ON)
        self.last_ping_time = time.time()

    async def on_is_muted(self, args):
        try:
            payload = json.loads(args)
            if 'data' in payload:
                self._state['is_muted'] = payload['data']['result']
                await self.event('is_muted', self._state['is_muted'])
            elif 'error' in payload:
                raise Exception(payload['error']['message'],
                                *payload['error']['errors'])
        except Exception as e:
            await self._handle_exception(e)

    async def on_unmute(self, _):
        self._state['is_muted'] = False
        await self.event('is_muted', self._state['is_muted'])

    async def on_mute(self, _):
        self._state['is_muted'] = True
        await self.event('is_muted', self._state['is_muted'])

    async def on_mpv_file_pos_sec(self, _):
        pass

    async def shutdown(self, *_, **__):
        await self.cancel()
        logger.debug('Shutting down %s', self.name)
        await self.set_should_shutdown(self.is_online == DeviceState.ON)
        if 'shutdown' in self.tasks:
            self.tasks['shutdown'].cancel()
        task = asyncio.create_task(self._try_method(
            self._shutdown, error_cb=self.set_should_shutdown(False)))
        self.tasks['shutdown'] = task

        def power_off_done(_):
            logger.debug('%s power_off_done', self.name)
            self.power_cycle(wait=10)
            self._delete_task('shutdown')
        task.add_done_callback(power_off_done)

    async def reboot(self, *_, **__):
        await self.cancel()
        logger.debug('Reboot %s', self.name)
        await self.set_should_reboot(self.is_online == DeviceState.ON)
        if 'reboot' in self.tasks:
            self.tasks['reboot'].cancel()
        task = asyncio.create_task(self._try_method(
            self._reboot, error_cb=self.set_should_reboot(False)))
        self.tasks['reboot'] = task
        task.add_done_callback(self._delete_task('reboot'))

    async def mute(self, *_, **__):
        logger.debug('Mute %s', self.name)
        await self.client.publish(f'{self.probe_address}/mute', qos=1)

    async def unmute(self, *_, **__):
        logger.debug('Unmute %s', self.name)
        await self.client.publish(f'{self.probe_address}/unmute', qos=1)

    async def fetch(self):
        await super().fetch()
        await self.event('should_shutdown', self.should_shutdown)
        await self.event('should_reboot', self.should_reboot)
        await self.event('is_muted', self._state['is_muted'])
        await self.event('errors', self._state['errors'])
        await self.event('display', self._state['display'])
