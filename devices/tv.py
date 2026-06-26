from devices.icmpable import ping_address
from .device import DeviceState
from .wolable import WOLable
from misc import logger, memoize
import asyncio
import json

from aiowebostv import WebOsClient

CREDS_PATH = '/opt/weboscreds.json'


class LGWebOSTV(WOLable):
    _capabilities = ['wake', 'shutdown']

    def __init__(self, *args, **kwargs):
        super().__init__(*args, should_icmp=False, **kwargs)
        self._state['should_shutdown'] = False
        self._state['is_connected'] = False
        self._state['is_registered'] = False
        self.update_methods.append(('ping', self.ping))
        self.update_methods.append(('register_client', self.register_client))
        self.loop = asyncio.get_running_loop()
        self.ip = getattr(self, 'primary_ip')['address'].split('/')[0]
        # aiowebostv client is created lazily inside the event loop on connect.
        self.client: WebOsClient | None = None
        # Guards against two concurrent connect attempts (online_event and the
        # periodic register_client can both fire) creating duplicate clients.
        self._connecting = False

    async def online_event(self, _, event_type, value):
        if event_type == 'is_online':
            await self.set_should_wake(self.should_wake and not value not in [DeviceState.PARTIAL, DeviceState.ON])
            await self.set_should_shutdown(self.should_shutdown and value not in [DeviceState.OFF, DeviceState.PARTIAL])
            if value == DeviceState.PARTIAL and not self.is_connected:
                await self.try_connect()

    def _load_client_key(self):
        try:
            with open(CREDS_PATH) as f:
                return (json.load(f) or {}).get('client_key')
        except (FileNotFoundError, ValueError):
            return None

    def _save_client_key(self, client_key):
        if not client_key:
            return
        data = {}
        try:
            with open(CREDS_PATH) as f:
                data = json.load(f) or {}
        except (FileNotFoundError, ValueError):
            data = {}
        if data.get('client_key') != client_key:
            data['client_key'] = client_key
            with open(CREDS_PATH, 'w') as f:
                json.dump(data, f)

    def _set_online(self, connected):
        # aiowebostv pairs as part of connect(), so is_connected and
        # is_registered move together. is_registered's setter drives
        # set_is_online; only flip it on an actual change.
        if connected:
            self.is_connected = True
            if not self.is_registered:
                self.is_registered = True
        else:
            if self.is_registered:
                self.is_registered = False
            self.is_connected = False

    async def _disconnect_client(self):
        if self.client is not None:
            try:
                await self.client.disconnect()
            except Exception:
                pass
            self.client = None

    async def _connect(self):
        # connect() opens the websocket AND performs pairing (registration):
        # with a stored client_key it reconnects silently, otherwise the TV
        # shows a pairing prompt the first time.
        self.client = WebOsClient(self.ip, client_key=self._load_client_key())
        try:
            async with asyncio.timeout(15):
                await self.client.connect()
        except Exception as e:
            await self._disconnect_client()
            logger.exception(e)
            return
        self._save_client_key(self.client.client_key)
        self._set_online(True)

    async def try_connect(self):
        if self.is_connected or self._connecting:
            return
        logger.debug('try_connect start, not connected')
        self._connecting = True
        try:
            await self._connect()
        finally:
            self._connecting = False

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
        # Reconcile our state with the real aiowebostv link and (re)connect
        # while the device is reachable. Skip while a connect is in flight so
        # we don't tear down the half-open client try_connect is building.
        if self._connecting:
            return
        if self.client is not None and self.client.is_connected():
            self._set_online(True)
            return
        if self.is_connected or self.is_registered:
            self._set_online(False)
        await self._disconnect_client()
        if self.is_online == DeviceState.PARTIAL:
            await self.try_connect()

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

    async def shutdown(self, *_, **__):
        try:
            # The link may be briefly down while the TV is still physically on;
            # try to (re)connect before giving up so the command isn't dropped.
            if self.client is None or not self.client.is_connected():
                await self.try_connect()
            if self.client is not None and self.client.is_connected():
                await self.client.power_off()
                await self.set_should_shutdown(True)
            else:
                logger.warning('%s shutdown requested but not connected', self.name)
        except Exception as e:
            await self._handle_exception(e)

    async def fetch(self):
        await super().fetch()
        await self.event('should_wake', self.should_wake)
        await self.event('should_shutdown', self.should_shutdown)
