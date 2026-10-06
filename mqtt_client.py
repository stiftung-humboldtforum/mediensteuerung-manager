import json

from aiomqtt import Client as BaseClient


class Client(BaseClient):
    async def publish_json(self, topic: str, payload: object | None, **kwargs):
        if payload is not None:
            json_payload = json.dumps(payload)
        else:
            json_payload = None
        await self.publish(topic, json_payload, **kwargs)
