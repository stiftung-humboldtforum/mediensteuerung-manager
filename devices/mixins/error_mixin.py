import traceback
import json
import time
import asyncio
from types import CoroutineType, FunctionType
from typing import Coroutine, Callable

from aiomqtt import Client

from misc import logger


class ErrorMixin:
    def __init__(self,
                 manager,
                 client: Client,
                 *_,
                 **kwargs):
        self.manager = manager
        self.client = client
        try:
            self.name = kwargs['primary_ip']['dns_name']
        except Exception:
            self.name = kwargs['name']

    async def _handle_exception(self, e):
        # Use the exception's own traceback (works outside an active except frame,
        # where sys.exc_info() would be (None, None, None) -> IndexError below).
        tb_info = traceback.extract_tb(e.__traceback__)
        if tb_info:
            _, _, func, text = tb_info[-1]
        else:
            func, text = '?', ''
        error_name = f'[{self.name}]: {func}: {type(e).__name__} {text}'
        logger.debug('%s %s', error_name, e)
        await self.error(error_name, e.args)

    async def error(self, message: str, errors: tuple = ()):
        json_payload = json.dumps({
            'error': {
                'message': message,
                'errors': errors,
                'time': time.time() * 1000
            }
        })
        await self.client.publish('manager/device_event', json_payload)

    async def _try_method(self, method, error_cb: Callable | Coroutine | None = None, **kwargs):
        try:
            if self.timeouts.values():
                timeout = max(self.timeouts.values())
            else:
                timeout = 60
            async with asyncio.timeout(timeout):
                await method(**kwargs)
                if error_cb and type(error_cb) == CoroutineType:
                    error_cb.close()
        except Exception as e:
            try:
                self.lock.release()
            except:
                pass
            await self._handle_exception(e)
            if error_cb:
                if type(error_cb) == CoroutineType:
                    await error_cb
                elif type(error_cb) == FunctionType:
                    error_cb()
