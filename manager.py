import os
import asyncio
import time
from typing import Any

import requests
import yaml

from mqtt_client import Client
from misc import get_config, logger, timed, get_device_class
from tags import Tag
from locations import Location
import devices
from devices import Device, ICMPable


class Api:
    def __init__(self):
        self.token = None
        self.api_url = f'https://{os.environ["API_HOSTNAME"]}:443'

    def login(self):
        auth_data = {
            'username': (None, os.environ['API_SYSTEM_USERNAME']),
            'password': (None, os.environ['API_SYSTEM_PASSWORD'])
        }
        response = requests.request(
            'POST',
            f'{self.api_url}/auth/jwt/login',
            files=auth_data,
            verify=os.environ['API_ROOT_CA'])
        self.token = response.json()['access_token']

    def get(self, path):
        headers = {
            'authorization': f'Bearer {self.token}'
        }
        response = requests.get(
            f'{self.api_url}{path}',
            headers=headers,
            verify=os.environ['API_ROOT_CA'],
            timeout=120)
        if response.status_code == 401:
            self.login()
            return self.get(path)
        else:
            return response


class Manager:
    def __init__(self, client: Client):
        self.client = client
        self.api = Api()
        self.tasks: dict[str, asyncio.Task] = dict()
        self.lock = asyncio.Lock()
        self.healthcheck()

    async def setup(self, initial=False):
        self.config = get_config()
        self.device_map = self.config['device_map']['value']
        await self.lock.acquire()
        try:
            response = self.api.get('/api/')
            try:
                response = response.json()
            except Exception as e:
                logger.exception(e)
                logger.error(response)
                exit(1)
            devices = response['devices']
            tags = response['tags']
            locations = response['locations']
            if initial:
                self.devices: dict[int, Device] = {}
                self.tags: dict[int, Tag] = {}
                self.locations: dict[int, Location] = {}
            await self.subscribe_devices(devices)
            await self.subscribe_tags(tags)
            await self.subscribe_locations(locations)
        except Exception as e:
            logger.exception(e)
            await self.setup()
        self.lock.release()

    def delete_task(self, task_name):
        def wrap(_):
            try:
                del self.tasks[task_name]
            except Exception:
                pass
        return wrap

    def healthcheck(self):
        if int(time.time()) % 30 == 0: 
            with open('/tmp/health', 'w') as f:
                f.write(str(time.time()))

    @timed(.125)
    async def update_devices(self):
        for device in self.devices.values():
            if device.name not in self.tasks:
                task = asyncio.create_task(device.update())
                self.tasks[device.name] = task
                task.add_done_callback(self.delete_task(device.name))
        self.healthcheck()

    async def start(self):
        while True:
            await self.lock.acquire()
            await self.update_devices()
            self.lock.release()

    async def idle(self):
        while not all([device.is_idle() for device in self.devices.values()]):
            await asyncio.sleep(1)

    async def on_message(self, topic, payload):
        ...
        # logger.info('%s %s', topic, payload)

    async def subscribe_devices(self, devices):
        if isinstance(devices, list):
            for device in devices:
                await self.subscribe_device(device)
        elif isinstance(devices, dict):
            await self.subscribe_device(devices)

    async def subscribe_device(self, device):
        device_id = device['id']
        device_name = device['name']
        device_class_name = get_device_class(self.device_map, device)
        device_class = getattr(
            devices,
            device_class_name,
            Device
        )
        if device_id in self.devices and device_class != type(self.devices[device_id]):
            await self.devices[device_id].cancel()
            del self.devices[device_id]
        if device_id not in self.devices:
            self.devices[device_id] = device_class(
                self, self.client, self.device_event, **device)
            act = 'Subscribed'
        else:
            self.devices[device_id].set_data(device)
            act = 'Updated'
        await self.devices[device_id].setup()
        logger.debug(f'{act} device: %s %s %s',
                     device_class.__name__, device_id, device_name)

    async def subscribe_tags(self, tags):
        if isinstance(tags, list):
            for tag in tags:
                await self.subscribe_tag(tag)
        elif isinstance(tags, dict):
            await self.subscribe_tag(tags)

    async def subscribe_tag(self, tag):
        tag_id = tag['id']
        if tag_id not in self.tags:
            self.tags[tag_id] = Tag(self, **tag)
            act = 'Subscribed'
        else:
            self.tags[tag_id].set_data(tag)
            act = 'Updated'
        logger.debug(f'{act} tag: %s %s',
                     tag_id, tag['name'])

    async def subscribe_locations(self, locations):
        if isinstance(locations, list):
            for location in locations:
                await self.subscribe_location(location)
        elif isinstance(locations, dict):
            await self.subscribe_location(locations)

    async def subscribe_location(self, location):
        location_id = location['id']
        if location_id not in self.locations:
            self.locations[location_id] = Location(self, **location)
            act = 'Subscribed'
        else:
            self.locations[location_id].set_data(location)
            act = 'Updated'
        logger.debug(f'{act} location: %s %s',
                     location_id, location['name'])

    def make_event(self, target: int, event_type: str, payload: Any):
        return {
            'data': {
                'event': {
                    'target': target,
                    'type': event_type,
                    'value': payload
                }
            }
        }

    async def device_event(self, target: int, event_type: str, payload: Any):
        event = self.make_event(target, event_type, payload)
        await self.client.publish_json('manager/device_event', event)
        if event_type == 'is_online':
            tags = [tag for tag in self.tags.values() if target in tag]
            for tag in tags:
                await self.tag_event(tag.id, event_type, tag.is_online)
            location = [
                location for location in self.locations.values() if {'id': target, 'type': 'devices'} in location]
            if len(location):
                await self.location_event(location[0].id, event_type, location[0].is_online)

    async def tag_event(self, target: int, event_type: str, payload: Any):
        event = self.make_event(target, event_type, payload)
        await self.client.publish_json('manager/tag_event', event)

    async def location_event(self, target: int, event_type: str, payload: Any):
        event = self.make_event(target, event_type, payload)
        await self.client.publish_json('manager/location_event', event)

    async def device_method(self, method_name, kwargs):
        device = kwargs['data']
        params = kwargs.get('params', {})
        device_id = device['id']
        if device_id not in self.devices:
            logger.error('Device with id "%s" not subscribed', device_id)
            return
        task_name = f'{self.devices[device_id].name}_{method_name}'
        task = asyncio.create_task(
            self.devices[device_id]._try_method(
                getattr(self.devices[device_id], method_name), **params)
        )
        self.tasks[task_name] = task
        task.add_done_callback(self.delete_task(method_name))

    async def tag_method(self, method_name, kwargs):
        tag = kwargs['data']
        params = kwargs.get('params', {})
        tag_id = tag['id']
        if tag_id not in self.tags:
            logger.error('Tag with id "%s" not subscribed', tag_id)
            return
        task_name = f'{self.tags[tag_id].name}'
        task = asyncio.create_task(
            getattr(self.tags[tag_id], method_name)(**params)
        )
        try:
            self.tasks[task_name].cancel()
        except:
            pass
        self.tasks[task_name] = task
        task.add_done_callback(self.delete_task(task_name))

    async def location_method(self, method_name, kwargs):
        location = kwargs['data']
        params = kwargs.get('params', {})
        location_id = location['id']
        if location_id not in self.locations:
            logger.error('Location with id "%s" not subscribed', location_id)
            return
        task_name = f'{self.locations[location_id].name}'
        task = asyncio.create_task(
            getattr(self.locations[location_id], method_name)(**params)
        )
        try:
            self.tasks[task_name].cancel()
        except:
            pass
        self.tasks[task_name] = task
        task.add_done_callback(self.delete_task(task_name))
