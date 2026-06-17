import os
import asyncio
from functools import cached_property
from typing import Sequence

import aiosnmp

from misc import logger, memoize

from .device import DeviceState
from .icmpable import ICMPable

PDU_COMMUNITYSTRING = os.environ['PDU_COMMUNITYSTRING']

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
        self.snmp_client = aiosnmp.Snmp(
            host=address, community=PDU_COMMUNITYSTRING, timeout=5, retries=2)

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

    async def online_event(self, _, event_type, value):
        if event_type == 'is_online':
            if value == DeviceState.ON:
                await self.lock.acquire()
                try:
                    async with self.snmp_client as client:
                        await self._read_powerfeeds(client)
                except Exception as e:
                    logger.exception(self.name)
                    await self._handle_exception(e)
                    await self.set_is_online(DeviceState.PARTIAL)
                self.lock.release()

    async def _read_powerfeeds(self, client):
        res = await client.get(self.port_state_oids)
        powerfeeds = [x.value == 1 for x in res]

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
                async with self.snmp_client as client:
                    await self._read_powerfeeds(client)
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
                await self.lock.acquire()
                try:
                    async with self.snmp_client as client:
                        res = await client.set(messages)
                        self._state['powerfeeds'] = [x.value == 1 for x in res]
                        logger.debug('%s powerfeeds %s', self.name,
                                     self._state['powerfeeds'])
                        await self.event('powerfeeds', self._state['powerfeeds'])
                except Exception as e:
                    await self._handle_exception(e)
                    await asyncio.sleep(5)
                self.lock.release()

    async def write_powerfeed(self, id=None, value=None):
        if id is None or value is None:
            return

        while not self.is_ready:
            await asyncio.sleep(1)
        logger.debug('name=%s, id=%s, value=%s', self.name, id, value)
        powerfeeds = [*self._state['powerfeeds']]
        powerfeeds[id] = value
        if '_write_powerfeeds' in self.tasks and not self.tasks['_write_powerfeeds'].done():
            self.tasks['_write_powerfeeds'].cancel()
            self.lock.release()
        task = asyncio.create_task(self._try_method(
            self._write_powerfeeds, powerfeeds=powerfeeds))
        self.tasks['_write_powerfeeds'] = task
        task.add_done_callback(self._delete_task('_write_powerfeeds'))
        return self._state['powerfeeds'][id] != value

    async def fetch(self):
        await super().fetch()
        if self.is_online == DeviceState.ON:
            await self.event('powerfeeds', self._state['powerfeeds'])
