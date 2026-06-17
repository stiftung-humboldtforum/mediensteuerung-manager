import os
import json
import asyncio
import ssl
import time

import asyncclick as click
import uvloop

from misc import logger
from mqtt_client import Client
from manager import Manager

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


@click.command()
@click.option('--ca_certificate', default='/opt/tls/ca_certificate.pem')
@click.option('--client_certificate', default='/opt/tls/client_certificate.pem')
@click.option('--client_key', default='/opt/tls/client_key.pem')
async def main(ca_certificate, client_certificate, client_key):
    ssl_context = ssl.create_default_context(cafile=ca_certificate)
    ssl_context.load_cert_chain(
        client_certificate, client_key)
    loop = asyncio.get_event_loop()
    async with Client(
            os.environ['MQTT_HOSTNAME'],
            client_id='manager',
            port=8883,
            keepalive=60,
            tls_context=ssl_context,
            max_concurrent_outgoing_calls=2000
    ) as client:
        client.pending_calls_threshold = 500
        await client.subscribe('api/#', qos=1)
        await client.subscribe('calendar/#')
        await client.subscribe('knx/switch/#')
        await client.subscribe('fac/#')
        await client.subscribe('probe/#')
        manager = Manager(client)
        await manager.setup(initial=True)
        manager_task = loop.create_task(manager.start())
        async with client.messages() as messages:
            async for message in messages:
                # logger.debug(message.topic.value)
                await manager.on_message(message.topic, message.payload)
                if message.topic.matches('probe/#'):
                    topic_parts = message.topic.value.split('/')
                    if len(topic_parts) != 3:
                        logger.error('Malformed probe topic: %r (payload=%r)',
                                     message.topic.value, message.payload[:200])
                        continue
                    _, fqdn, device_method = topic_parts
                    try:
                        device_id = [id for id, dev
                                     in manager.devices.items()
                                     if dev.name == fqdn][0]
                        method = getattr(
                            manager.devices[device_id], f'on_{device_method}')
                        await method(message.payload.decode())
                    except IndexError as e:
                        message = f'Device not subscribed: {fqdn}'
                        json_payload = json.dumps({
                            'error': {
                                'message': message,
                                'time': time.time() * 1000
                            }
                        })
                        await client.publish('manager/device_event', json_payload)
                        logger.error(message)
                    continue
                try:
                    payload = json.loads(message.payload)
                except:
                    payload = {}
                if message.topic.matches('api/data-refresh'):
                    await manager.setup()
                try:
                    if message.topic.matches('api/subscribe_devices'):
                        await manager.subscribe_devices(payload)
                        continue
                    if message.topic.matches('api/device/+'):
                        method_name = message.topic.value.split('/')[2]
                        await manager.device_method(method_name, payload)
                        continue
                    if message.topic.matches('api/tag/+'):
                        method_name = message.topic.value.split('/')[2]
                        await manager.tag_method(method_name, payload)
                        continue
                    if message.topic.matches('api/location/+'):
                        method_name = message.topic.value.split('/')[2]
                        await manager.location_method(method_name, payload)
                        continue
                    if message.topic.matches('calendar/#'):
                        edge = message.topic.value.split('/')[1]
                        type = message.topic.value.split('/')[2]
                        method_name = message.topic.value.split('/')[3]
                        logger.debug('Calendar event: %s %s %s %s',
                                     edge, type, method_name, payload)
                        if method_name != 'clear':
                            await getattr(manager, f'{type}_method')(method_name, payload)
                        await getattr(manager, f'{type}s')[payload['data']['id']].calendar_edge(edge, method_name)
                        continue
                    if message.topic.matches('knx/switch/#'):
                        location_id = int(message.topic.value.split('/')[2])
                        await manager.location_method('knx_switch', {'data': {'id': location_id}, 'params': payload})
                    if message.topic.matches('fac/#'):
                        method_name = message.topic.value.split('/')[1]
                        location_ids = message.topic.value.split('/')[2]
                        for location_id in location_ids.split(','):
                            location_id = int(location_id)
                            await manager.location_method(method_name, {'data': {'id': location_id}})
                except Exception as e:
                    logger.exception(e)
        await manager_task


if __name__ == '__main__':
    main()
