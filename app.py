import os
import json
import aiohttp
import aiobotocore
import asyncio
from aiokafka import KafkaError
from collections import deque
from kafkahelpers import make_pair

loop = asyncio.get_event_loop()

QUEUE = os.environ.get("QUEUE", "platform.upload.buckit")
RESPONSE_QUEUE = os.environ.get("RESPONSE_QUEUE", "platform.upload.validation")
BOOT = os.environ.get("KAFKAMQ", "kafka:29092").split(",")
GROUP = os.environ.get("GROUP", "buckit")
BUCKET_MAP_FILE = os.environ.get("BUCKET_MAP_FILE")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")

try:
    with open(BUCKET_MAP_FILE, "rb") as f:
        BUCKET_MAP = json.load(f)
except Exception:
    BUCKET_MAP = {}


reader, writer = make_pair(QUEUE, GROUP, BOOT)
produce_queue = deque()


async def fetch(doc):
    async with aiohttp.ClientSession() as session:
        async with session.get(doc["url"]) as response:
            return await response.read()


async def store(payload, doc):
    session = aiobotocore.get_session(loop)
    async with session.create_client(
        "s3",
        region_name=AWS_REGION,
        aws_secret_key=AWS_SECRET_KEY,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
    ) as client:
        bucket = BUCKET_MAP[doc["service"]]
        await client.put_object(Bucket=bucket, Key=doc["payload_id"], Body=payload)
    session.close()


async def consumer(client):
    data = await client.getmany()
    for msg in (msgs for tp, msgs in data.items()):
        doc = json.loads(msg.value)
        payload = await fetch(doc)
        await store(payload, doc)
        produce_queue.append({"validation": "handoff"})

    await asyncio.sleep(0.5)


def make_producer(queue=None):
    queue = produce_queue if queue is None else queue

    async def producer(client):
        if not queue:
            await asyncio.sleep(0.1)
        else:
            item = queue.popleft()
            try:
                await client.send_and_wait(
                    RESPONSE_QUEUE, json.dumps(item).encode("utf-8")
                )
            except KafkaError:
                queue.append(item)
                raise

    return producer


if __name__ == "__main__":
    loop.create_task(reader.run(consumer))
    loop.create_task(writer.run(make_producer()))
    loop.run_forever()
