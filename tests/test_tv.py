"""Unit-Test: Fehlerberichte der LG-webOS-TVs gehen über den MQTT-Client.

Pinnt den Fix vom 2026-10-23 (Gesamt-Review R6): Seit dem Wechsel auf
aiowebostv (60542c3) überschrieb LGWebOSTV `self.client` (den MQTT-Client) mit
dem webOS-Client; jeder Fehlerbericht eines TVs scheiterte dann mit
AttributeError und brach Abschaltfolgen ab.

Kein TV, kein Broker nötig. Im Manager-Image ausführen:

    pip install pytest && python -m pytest tests/test_tv.py -v
"""
import asyncio
from unittest import mock

from devices.tv import LGWebOSTV


def test_fehlerbericht_geht_ueber_mqtt():
    async def run():
        mqtt = mock.Mock()
        mqtt.publish = mock.AsyncMock()
        tv = LGWebOSTV(mock.Mock(), mqtt, lambda *_: None, id=1, tags=[], location=None, name='tv-1',
                       primary_ip={'address': '192.0.2.10/24', 'dns_name': 'tv-1.example'})
        await tv.error('[tv-1]: test')
        mqtt.publish.assert_awaited_once()
        assert mqtt.publish.await_args.args[0] == 'manager/device_event'
    asyncio.run(run())
