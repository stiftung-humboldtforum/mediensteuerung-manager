from devices.icmpable import ping_address
from .device import DeviceState
from .wolable import WOLable
from misc import logger, memoize
import asyncio
import json

from pywebostv import connection, controls
import wsaccel
wsaccel.patch_ws4py()


store = {}


class LGWebOSTV(WOLable):
    _capabilities = ['wake', 'shutdown']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, should_icmp=False, **kwargs)
        self._state['should_shutdown'] = False
        self._state['is_connected'] = False
        self._state['is_registered'] = False
        self.update_methods.append(('ping', self.ping))
        self.update_methods.append(('register_client', self.register_client))
        self.loop = asyncio.get_event_loop()
        self.ip = getattr(self, 'primary_ip')['address'].split('/')[0]
        self.init_client()

    async def online_event(self, _, event_type, value):
        if event_type == 'is_online':
            await self.set_should_wake(self.should_wake and not value not in [DeviceState.PARTIAL, DeviceState.ON])
            await self.set_should_shutdown(self.should_shutdown and value not in [DeviceState.OFF, DeviceState.PARTIAL])
            if value == DeviceState.PARTIAL and not self.is_connected:
                await self.try_connect()

    def init_client(self):
        self.webosclient = connection.WebOSClient(self.ip, secure=True)
        logger.debug('webosclient created')
        self.webosclient.closed = self.on_close
        self.webosclient.opened = self.on_open
        self.syscontrol = controls.SystemControl(self.webosclient)

    async def try_connect(self):
        if not self.is_connected:
            logger.debug('try_connect start, not connected')
            try:
                logger.debug('try_connect webosclient connect')
                async with asyncio.timeout(10):
                    self.webosclient.connect()
            except Exception as e:
                self.webosclient.close()
                logger.exception(e)

    @memoize(10)
    async def ping(self):
        if self.is_online != DeviceState.ON:
            if await ping_address(self.ip):
                if not self.is_connected:
                    await self.set_is_online(DeviceState.PARTIAL)
            else:
                logger.debug('fail')
                await self.set_is_online(DeviceState.OFF)

    @memoize(10)
    async def register_client(self):
        if self.is_connected and not self.is_registered:
            logger.debug('register...')
            try:
                with open('/opt/weboscreds.json', 'r+') as f:
                    store = json.loads(f.read())
                    list(self.webosclient.register(store, timeout=1))
                    f.seek(0)
                    f.write(json.dumps(store))
                    f.truncate()
                self.is_registered = True
                logger.debug('registered!')
            except Exception as e:
                await self._handle_exception(e)
                self.init_client()

    @property
    def is_connected(self):
        return self._state['is_connected']

    @is_connected.setter
    def is_connected(self, value):
        self._state['is_connected'] = value

    @property
    def is_registered(self):
        return self._state['is_registered']

    @is_registered.setter
    def is_registered(self, value):
        logger.debug(value)
        is_online = DeviceState.ON if value else DeviceState.PARTIAL
        self._state['is_registered'] = value
        self.loop.create_task(self.set_is_online(is_online))
        self.loop.create_task(self.event('is_registered', value))

    @property
    def should_shutdown(self) -> bool:
        return self._state['should_shutdown']

    async def set_should_shutdown(self, value: bool):
        self._state['should_shutdown'] = value
        await self.event('should_shutdown', value)

    def on_open(self, *_, **__):
        logger.debug('try_connect webosclient connected')
        self.is_connected = True

    def on_close(self, *_, **__):
        self.is_connected = False
        self.init_client()

    def on_shutdown_received(self, status, payload):
        logger.debug('%s, %s', status, payload)
        self.loop.create_task(self.set_should_shutdown(status))

    async def shutdown(self, *_, **__):
        self.syscontrol.power_off(callback=self.on_shutdown_received)

    async def fetch(self):
        await super().fetch()
        await self.event('should_wake', self.should_wake)
        await self.event('should_shutdown', self.should_shutdown)
