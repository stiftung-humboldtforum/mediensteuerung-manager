import asyncio
import os

import requests
from requests.auth import HTTPDigestAuth

from .icmpable import ICMPable


class BrightSign(ICMPable):
    _capabilities = ['reboot']

    async def reboot(self, *_, **__):
        ip = self.primary_ip['address'].split('/')[0]
        username = os.environ.get('BRIGHTSIGN_USERNAME', 'admin')
        password = os.environ.get('BRIGHTSIGN_PASSWORD', 'avm')
        # requests is blocking — run off the event loop with a timeout so an
        # unreachable player can't stall the whole manager loop.
        await asyncio.to_thread(
            lambda: requests.put(
                f'http://{ip}/api/v1/control/reboot',
                auth=HTTPDigestAuth(username, password),
                timeout=10))
