import asyncio
from typing import Any, Callable

from aiomqtt import Client
from devices.mixins import ErrorMixin, EventMixin, PowerMixin, CalendarMixin

from locations import Location
from .state import DeviceState


class Device(EventMixin, ErrorMixin, PowerMixin, CalendarMixin):
    _capabilities = []

    def __init__(self,
                 manager,
                 client: Client,
                 callback: Callable,
                 **kwargs):
        super().__init__(manager, client, callback, **kwargs)
        self.manager = manager
        self.client = client
        self.set_data(kwargs)
        self._state: dict[str, Any] = {}
        self._state['is_initialized'] = False
        self._state['is_online'] = DeviceState.OFF
        self._offline_counter = 0
        self.timeouts: dict[str, float] = {}
        self.start_times: dict[str, float] = {}
        self.update_methods: list[tuple[str, Callable]] = []
        self.tasks: dict[str, asyncio.Task] = dict()
        self.power_task = None
        self.lock = asyncio.Lock()

    def set_data(self, data: dict[str, Any]):
        self.id: int = data['id']
        self.tags = data['tags']
        self.location = data['location']
        try:
            # geändert: DA Update Netbox
            # self.role = data['device_role']['name']
            self.role = data['role']['name']
        except:
            self.role = ''
        for key, value in data.items():
            setattr(self, key, value)
        try:
            self.name = data['primary_ip']['dns_name']
        except Exception:
            self.name = data['name']

    async def setup(self):
        pass

    async def cancel(self, *_, **__):
        for key in self._state:
            if key.startswith('should'):
                await getattr(self, f'set_{key}')(False)
        [task.cancel() for task in self.tasks.values()]
        try:
            self.lock.release()
        except:
            pass

    def __getattr__(self, __name: str) -> Callable:
        async def method(*_, **__):
            await self.error(f'[{self.name}]: Method "{__name}" not implemented for "{self.__class__.__name__}"')
        return method

    async def wait_for(self, *states):
        while self._state['is_online'] not in states:
            await asyncio.sleep(1)

    @property
    def is_initialized(self):
        return self._state['is_initialized']

    @property
    def capabilities(self):
        if 'ctrl mon' in [tag['name'] for tag in self.tags]:
            return []
        else:
            return self._capabilities

    async def on_capabilities(self, args):
        try:
            self._capabilities = args.split(',')
            await self.event('capabilities', self.capabilities)
        except:
            pass

    @property
    def is_online(self):
        return self._state['is_online']

    async def set_is_online(self, value):
        self._state['is_initialized'] = True
        if value == DeviceState.OFF and self._offline_counter < 3:
            self._offline_counter += 1
        else:
            self._offline_counter = 0
            if self._state['is_online'] != value:
                self._state['is_online'] = value
                await self.event('is_online', value)

    def is_tagged(self, tag):
        return tag.name in [tag['name'] for tag in self.tags]

    def is_located(self, location: Location):
        if self.location is not None:
            return location.id == self.location['id']
        else:
            return False

    def is_idle(self):
        should = [val for key, val in self._state.items()
                  if key.startswith('should')]
        return not any(should)

    def _delete_task(self, task_name):
        def wrap(_):
            if task_name in self.tasks:
                self.tasks[task_name].cancel()
                del self.tasks[task_name]
        return wrap

    async def update(self):
        for name, method in self.update_methods:
            task_name = self.name + name
            if task_name not in self.tasks:
                task: asyncio.Task = asyncio.create_task(
                    self._try_method(method, error_cb=self._delete_task(task_name)))
                self.tasks[task_name] = task
                task.add_done_callback(self._delete_task(task_name))

    async def fetch(self, *_, **__):
        await self.event('class', self.__class__.__name__)
        await self.event('capabilities', self.capabilities)
        await self.event('is_online', self.is_online)
