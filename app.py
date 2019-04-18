from collections import deque
from kafkahelpers import make_pair, make_producer
import aiobotocore
import aiohttp
import asyncio
import json
import os

import metrics

loop = asyncio.get_event_loop()

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AWS_SECRET_KEY = os.environ.get("AWS_SECRET_KEY")
BOOT = os.environ.get("KAFKAMQ", "kafka:29092").split(",")
BUCKET_MAP_FILE = os.environ.get("BUCKET_MAP_FILE")
GROUP = os.environ.get("GROUP", "buckit")
QUEUE = os.environ.get("QUEUE", "platform.upload.buckit")
RESPONSE_QUEUE = os.environ.get("RESPONSE_QUEUE", "platform.upload.validation")

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
    session = aiobotocore.get_session(loop=loop)
    async with session.create_client(
        "s3",
        region_name=AWS_REGION,
        aws_secret_key=AWS_SECRET_KEY,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
    ) as client:
        bucket = BUCKET_MAP[doc["service"]]
        with metrics.s3_write_time.time():
            await client.put_object(Bucket=bucket, Key=doc["payload_id"], Body=payload)
        metrics.payload_size.observe(len(payload))
        metrics.bucket_counter(bucket).inc()
    session.close()


async def consumer(client):
    data = await client.getmany()
    for msgs in (msgs for tp, msgs in data.items()):
        for msg in msgs:
            doc = json.loads(msg.value)
            payload = await fetch(doc)
            await store(payload, doc)
            produce_queue.append(
                {"validation": "handoff", "payload_id": doc["payload_id"]}
            )
    await asyncio.sleep(0.5)


async def handoff(client, item):
    await client.send_and_wait(RESPONSE_QUEUE, json.dumps(item).encode("utf-8"))


if __name__ == "__main__":
    loop.create_task(reader.run(consumer))
    c = make_producer(handoff, produce_queue)
    loop.create_task(writer.run(c))
    metrics.start()
    loop.run_forever()
