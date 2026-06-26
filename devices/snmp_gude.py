import os
import asyncio
from functools import cached_property
from typing import Sequence

from pysnmp.hlapi.v3arch.asyncio import (
    CommunityData,
    ContextData,
    Integer32,
    ObjectIdentity,
    ObjectType,
    SnmpEngine,
    UdpTransportTarget,
    get_cmd,
    set_cmd,
)

from misc import logger, memoize

from .device import DeviceState
from .icmpable import ICMPable

PDU_COMMUNITYSTRING = os.environ['PDU_COMMUNITYSTRING']

SNMP_PORT = 161
SNMP_TIMEOUT = 5
SNMP_RETRIES = 2
WRITE_POWERFEEDS_TIMEOUT = 900


def get_num_powerfeeds(device_model):
    device_model = device_model.split(' ')[1]
    match device_model:
        case '1104-1' | '1105-1' | '1105-2':
            return 1
        case '8031-1' | '8031-2':
            return 8
        case '8801-3':
            return 11
        case '8041-1' | '8041-2' | '8045-1' | '8045-2':
            return 12
        case '8291-1':
            return 21
        case '8080' | '8082' | '8084' | '8081' | '8083':
            return 24
        case _:
            raise NotImplementedError(device_model)


gude_oid = '1.3.6.1.4.1.28507'


def get_port_state_oid(device_model):
    device_model = device_model.split(' ')[1]
    match device_model:
        case '1104-1':
            return gude_oid + '.68.1.3.1.2.1.3.'
        case '1105-1' | '1105-2':
            return gude_oid + '.69.1.3.1.2.1.3.'
        case '8031-1' | '8031-2':
            return gude_oid + '.81.1.3.1.2.1.3.'
        case '8041-1' | '8041-2':
            return gude_oid + '.85.1.3.1.2.1.3.'
        case '8045-1' | '8045-2':
            return gude_oid + '.87.1.3.1.2.1.3.'
        case '8291-1':
            return gude_oid + '.98.1.3.1.2.1.3.'
        case _:
            raise NotImplementedError(device_model)


class GudePDU(ICMPable):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.model = getattr(self, 'device_type')['model']
        try:
            self._state['powerfeeds'] = [-1] * self.num_powerfeeds
        except Exception as e:
            logger.exception(e)
            asyncio.ensure_future(self._handle_exception(e))
            return
        self.update_methods.append(
            ('Watch Powerfeeds', self._watch_powerfeeds))
        self.event.append(self.online_event)
        ip = getattr(self, 'primary_ip')
        address = ip['address'].split('/')[0]
        self.snmp_host = address
        # pysnmp's asyncio HLAPI is call-based (no aiosnmp-style connection
        # context manager): each get_cmd/set_cmd takes the engine + per-call
        # transport target. One reusable SNMPv2c engine + community per device.
        self.snmp_engine = SnmpEngine()
        self.snmp_community = CommunityData(PDU_COMMUNITYSTRING, mpModel=1)
        self._snmp_target = None

    async def _target(self):
        # Built lazily (UdpTransportTarget.create is async) and reused across
        # GET/SET so the ~10s poller doesn't churn a new UDP socket each call.
        if self._snmp_target is None:
            self._snmp_target = await UdpTransportTarget.create(
                (self.snmp_host, SNMP_PORT),
                timeout=SNMP_TIMEOUT, retries=SNMP_RETRIES)
        return self._snmp_target

    @cached_property
    def num_powerfeeds(self):
        return get_num_powerfeeds(self.model)

    @cached_property
    def port_state_oid(self):
        return get_port_state_oid(self.model)

    @cached_property
    def port_state_oids(self):
        return [
            f'{self.port_state_oid}{i+1}' for i in range(self.num_powerfeeds)]

    async def _snmp_get(self, oids):
        """SNMP GET; returns the values as ints in the same order as `oids`."""
        error_indication, error_status, error_index, var_binds = await get_cmd(
            self.snmp_engine,
            self.snmp_community,
            await self._target(),
            ContextData(),
            *[ObjectType(ObjectIdentity(oid)) for oid in oids],
        )
        self._raise_on_snmp_error('GET', error_indication, error_status,
                                  error_index, var_binds)
        return [int(var_bind[1]) for var_bind in var_binds]

    async def _snmp_set(self, messages):
        """SNMP SET of (oid, int) pairs; returns the echoed values as ints,
        in the same order as `messages`."""
        error_indication, error_status, error_index, var_binds = await set_cmd(
            self.snmp_engine,
            self.snmp_community,
            await self._target(),
            ContextData(),
            *[ObjectType(ObjectIdentity(oid), Integer32(value))
              for oid, value in messages],
        )
        self._raise_on_snmp_error('SET', error_indication, error_status,
                                  error_index, var_binds)
        return [int(var_bind[1]) for var_bind in var_binds]

    @staticmethod
    def _raise_on_snmp_error(op, error_indication, error_status, error_index,
                             var_binds):
        if error_indication:
            raise RuntimeError(f'SNMP {op} failed: {error_indication}')
        if error_status:
            at = '?'
            if error_index and int(error_index) <= len(var_binds):
                at = var_binds[int(error_index) - 1][0]
            raise RuntimeError(
                f'SNMP {op} error: {error_status.prettyPrint()} at {at}')

    async def online_event(self, _, event_type, value):
        if event_type == 'is_online':
            if value == DeviceState.ON:
                await self.lock.acquire()
                try:
                    await self._read_powerfeeds()
                except Exception as e:
                    logger.exception(self.name)
                    await self._handle_exception(e)
                    await self.set_is_online(DeviceState.PARTIAL)
                self.lock.release()

    async def _read_powerfeeds(self):
        values = await self._snmp_get(self.port_state_oids)
        powerfeeds = [value == 1 for value in values]

        changed = not all([a == b for a, b in zip(
            powerfeeds, self._state['powerfeeds'])])
        if changed:
            self._state['powerfeeds'] = powerfeeds
            await self.event('powerfeeds', self._state['powerfeeds'])

    @memoize(10)
    async def _watch_powerfeeds(self):
        if self.is_online == DeviceState.ON:
            await self.lock.acquire()
            try:
                await self._read_powerfeeds()
            except Exception as e:
                await self._handle_exception(e)
            self.lock.release()

    async def _write_powerfeeds(self, powerfeeds=None):
        if powerfeeds == None:
            return
        messages: Sequence = [(f'{self.port_state_oid}{i+1}', 1 if value else 0)
                              for i, value in enumerate(powerfeeds)]
        async with asyncio.timeout(WRITE_POWERFEEDS_TIMEOUT):
            while any([powerfeeds[i] != self._state['powerfeeds'][i] for i in range(self.num_powerfeeds)]):
                async with self.lock:
                    try:
                        values = await self._snmp_set(messages)
                        self._state['powerfeeds'] = [value == 1 for value in values]
                        logger.debug('%s powerfeeds %s', self.name,
                                     self._state['powerfeeds'])
                        await self.event('powerfeeds', self._state['powerfeeds'])
                    except Exception as e:
                        await self._handle_exception(e)
                        await asyncio.sleep(5)

    async def write_powerfeed(self, id=None, value=None):
        if id is None or value is None:
            return

        logger.debug('name=%s, id=%s, value=%s', self.name, id, value)
        powerfeeds = [*self._state['powerfeeds']]
        powerfeeds[id] = value
        if '_write_powerfeeds' in self.tasks and not self.tasks['_write_powerfeeds'].done():
            # Cancel the in-flight write; it now uses `async with self.lock`, which
            # releases the lock on cancellation — so do NOT release it here.
            self.tasks['_write_powerfeeds'].cancel()
        task = asyncio.create_task(self._try_method(
            self._write_powerfeeds, powerfeeds=powerfeeds))
        self.tasks['_write_powerfeeds'] = task
        task.add_done_callback(self._delete_task('_write_powerfeeds'))
        return self._state['powerfeeds'][id] != value

    async def fetch(self):
        await super().fetch()
        if self.is_online == DeviceState.ON:
            await self.event('powerfeeds', self._state['powerfeeds'])
