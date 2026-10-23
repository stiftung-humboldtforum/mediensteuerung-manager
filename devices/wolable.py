import asyncio
import ipaddress

from wakeonlan import send_magic_packet

from misc import logger

from .device import DeviceState
from .icmpable import ICMPable

WAKE_INTERVAL = 60


def _parse_wol_targets(cidrs):
    """['192.0.2.3/24', ...] -> [(broadcast_ip, source_ip), ...].
    Ungültige Einträge werden übersprungen und geloggt."""
    targets = []
    for entry in cidrs or []:
        try:
            iface = ipaddress.IPv4Interface(entry)
        except ValueError:
            logger.warning('Ungültiger wol_broadcast_targets-Eintrag: %r', entry)
            continue
        targets.append((str(iface.network.broadcast_address), str(iface.ip)))
    return targets


class WOLable(ICMPable):
    _capabilities = ['wake']

    def __init__(self, *args, max_time_to_wake: float = 900, **kwargs):
        super().__init__(*args, **kwargs)
        self._state['should_wake'] = False
        self.timeouts['wake'] = max_time_to_wake
        self.event.append(self.online_event)

    async def online_event(self, _, event_type, value):
        if event_type == 'is_online':
            await self.set_should_wake(self.should_wake and value != DeviceState.ON)

    @property
    def should_wake(self) -> bool:
        return self._state['should_wake']

    async def set_should_wake(self, value: bool):
        self._state['should_wake'] = value
        await self.event('should_wake', value)

    async def _wake(self):
        async with asyncio.timeout(self.timeouts['wake']):
            cfg = self.manager.config.get('wol_broadcast_targets') or {}
            targets = _parse_wol_targets(cfg.get('value'))
            while self.should_wake:
                logger.info('WoL-DEBUG %s: is_online=%s should_wake=%s targets=%s macs=%s',
                            self.name, self.is_online, self.should_wake, targets,
                            [i.get('mac_address') for i in getattr(self, 'interfaces', [])])
                if not self.is_online == DeviceState.ON:
                    interfaces = getattr(self, 'interfaces')
                    for interface in interfaces:
                        if self.is_online == DeviceState.ON:
                            await self.set_should_wake(False)
                            break
                        mac_address: str = interface['mac_address']
                        if mac_address:
                            if targets:
                                for broadcast_ip, source_ip in targets:
                                    try:
                                        send_magic_packet(mac_address,
                                                          ip_address=broadcast_ip,
                                                          interface=source_ip)
                                        logger.info('WoL-SEND ok %s -> %s via %s',
                                                    mac_address, broadcast_ip, source_ip)
                                    except Exception as e:
                                        logger.error('WoL-SEND FAIL %s -> %s via %s: %r',
                                                     mac_address, broadcast_ip, source_ip, e)
                            else:
                                send_magic_packet(mac_address)
                        else:
                            await self.set_should_wake(False)
                    await asyncio.sleep(WAKE_INTERVAL)
                elif self.is_online == DeviceState.ON:
                    await self.set_should_wake(False)

    async def wake(self, *_, **__):
        try:
            await self.cancel()
        except:
            pass
        self._cancel_existing_power_task()
        has_pdu = await self.set_power(True)
        logger.debug('Waking %s', self.name)

        async def inner():
            if has_pdu:
                await asyncio.sleep(5)
            await self.set_should_wake(True)
            await self._wake()

        if 'wake' in self.tasks:
            self.tasks['wake'].cancel()
        task = asyncio.create_task(self._try_method(
            inner, error_cb=self.set_should_wake(False)))
        self.tasks['wake'] = task
        task.add_done_callback(self._delete_task('wake'))

    async def fetch(self):
        await super().fetch()
        await self.event('should_wake', self.should_wake)
