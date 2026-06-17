import os
import shutil
import time
import logging
import threading
import asyncio
import random
from functools import reduce
from typing import Callable, Any

import yaml


if not os.path.isfile('./config/config.yml'):
    shutil.copyfile('./config/base_config.yml', './config/config.yml')


logger = logging.getLogger()
FORMAT = '%(asctime)s.%(msecs)03d:%(levelname)s:%(filename)s:%(lineno)s:%(funcName)s %(message)s'
logging.basicConfig(level=logging.DEBUG, format=FORMAT,
                    datefmt='%y-%m-%d %H:%M:%S')


def recursive_get(d, *keys):
    return reduce(lambda c, k: c.get(k, {}), keys, d)


def compare_fields(a: str | list[dict[str, str]], b: str):
    if type(a) == list:
        return b in [tag['name'] for tag in a]
    else:
        return a == b

def get_config():
    config = {}
    for item in yaml.load(open('./config/config.yml'), Loader=yaml.Loader):
        config[item['slug']] = item
    return config


def get_device_class(device_map, device) -> str:
    for device_class, all_filters in device_map.items():
        for filter in all_filters:
            match = all([
                compare_fields(recursive_get(
                    device, *field_name.split('.')), value)
                for field_name, value in filter.items()
            ])
            if match:
                return device_class
    return 'ICMPable'


class BroadcastEvent(list):
    def __init__(self, _id):
        super().__init__()
        self._id = _id

    async def __call__(self, *args, **kwargs):
        for callback in self:
            await callback(self._id, *args, **kwargs)


def run_in_thread(fn):
    async def run(*k, **kw):
        t = threading.Thread(target=fn, args=k, kwargs=kw, daemon=True)
        t.start()
        return t
    return run


def timed(interval: float) -> Callable:
    def decorator(func) -> Callable:
        async def wrapper(*args, **kwargs) -> Any:
            start_time = time.perf_counter()
            result: Any = await func(*args, **kwargs)
            end_time = time.perf_counter()
            time_taken = end_time - start_time
            sleep_time = interval - time_taken
            if sleep_time < 0:
                logger.error(
                    'Timed call "%s" took too long: %.2f(s)/%.2f(s)', func.__name__, time_taken, interval)
            await asyncio.sleep(max(0, sleep_time))
            return (func.__name__, result)
        wrapper.__name__ = 'timed_' + func.__name__
        return wrapper
    return decorator


last_called_dict = {}


def memoize(interval: float, immediate_key='') -> Callable:
    def decorator(func):
        async def wrapper(self, *args, **kwargs) -> tuple[str, Any]:
            func_hash = self.name + func.__name__
            if func_hash not in last_called_dict:
                last_called_dict[func_hash] = {
                    'time': time.time() + random.random() * interval,
                    'result': None,
                    'immediate': False,
                    'is_running': False
                }
            last_called = last_called_dict[func_hash]
            if last_called['is_running']:
                return last_called['result']
            now = time.time()
            immediate = False
            if immediate_key:
                immediate = getattr(self, immediate_key)
            is_immediate = immediate != last_called['immediate'] and immediate
            if now - last_called['time'] > interval or (is_immediate and not last_called['immediate']):
                last_called['immediate'] = immediate
                last_called['time'] = now
                try:
                    last_called['is_running'] = True
                    result: Any = await func(self, *args, **kwargs)
                    last_called['result'] = result
                except:
                    raise
                finally:
                    last_called['is_running'] = False
            return (func.__name__, last_called['result'])
        wrapper.__name__ = 'memoize_' + func.__name__
        return wrapper
    return decorator


is_timeout_dict = {}


class MethodCanceled(Exception):
    pass


def timeout(method_key: str):
    def decorator(func) -> Callable:
        async def wrapper(self, *args, **kwargs) -> Any:
            func_hash = self.name + method_key
            if func_hash not in is_timeout_dict:
                is_timeout_dict[func_hash] = True
            timeout_sec = self.timeouts[method_key]
            start_time = self.start_times[method_key]
            current_time = time.time()
            running_time = current_time - start_time
            should = getattr(self, f'should_{method_key}')
            is_timeout = should and running_time > timeout_sec
            if is_timeout != is_timeout_dict[func_hash]:
                is_timeout_dict[func_hash] = is_timeout
                if is_timeout:
                    logger.debug('Timeout %s %s after %.2f(s)',
                                 method_key, self.name, running_time)
                    logger.debug('%s set_should_%s False',
                                 self.name, method_key)
                    await getattr(self, f'set_should_{method_key}')(False)
                    raise MethodCanceled(
                        method_key, self.name, 'after %.2f(s)' % running_time)
            if should and not is_timeout:
                result = await func(self, *args, **kwargs)
                logger.debug(f'{method_key} %s since %.2f(s)',
                             self.name,
                             running_time)
                return (func.__name__, result)
        wrapper.__name__ = 'timeout_' + func.__name__
        return wrapper
    return decorator
