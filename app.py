from collections import deque
from kafkahelpers import make_pair, make_producer
import aiobotocore
import aiohttp
import asyncio
from contextvars import ContextVar
import json
import os
import logging

import metrics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

loop = asyncio.get_event_loop()

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
BOOT = os.environ.get("KAFKAMQ", "kafka:29092").split(",")
BUCKET_MAP_FILE = os.environ.get("BUCKET_MAP_FILE")
GROUP = os.environ.get("GROUP", "buckit")
QUEUE = os.environ.get("QUEUE", "platform.upload.buckit")
RESPONSE_QUEUE = os.environ.get("RESPONSE_QUEUE", "platform.upload.validation")
REQUEST_ID = ContextVar("request_id")
REQUEST_ID.set("-1")

try:
    with open(BUCKET_MAP_FILE, "rb") as f:
        BUCKET_MAP = json.load(f)
except Exception:
    BUCKET_MAP = {}


produce_queue = deque()


async def fetch(doc):
    async with aiohttp.ClientSession() as session:
        async with session.get(doc["url"]) as response:
            return await response.read()


async def store(payload, bucket):
    session = aiobotocore.get_session(loop=loop)
    async with session.create_client(
        "s3",
        region_name=AWS_REGION,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
    ) as client:
        size = len(payload)
        logger.info("Storing %s bytes into bucket '%s'", size, bucket)
        with metrics.s3_write_time.time():
            await client.put_object(Bucket=bucket, Key=REQUEST_ID.get(), Body=payload)
        metrics.payload_size.observe(size)
        metrics.bucket_counter.labels(bucket).inc()


async def consumer(client):
    data = await client.getmany()
    for msgs in (msgs for tp, msgs in data.items()):
        for msg in msgs:
            doc = json.loads(msg.value)
            REQUEST_ID.set(doc["payload_id"])
            url = doc["url"]
            payload = await fetch(url)
            bucket = BUCKET_MAP[doc["category"]]
            await store(payload, bucket)
            produce_queue.append(
                {"validation": "handoff", "payload_id": doc["payload_id"]}
            )
    await asyncio.sleep(0.5)


async def handoff(client, item):
    await client.send_and_wait(RESPONSE_QUEUE, json.dumps(item).encode("utf-8"))


if __name__ == "__main__":
    reader, writer = make_pair(QUEUE, GROUP, BOOT)
    loop.create_task(reader.run(consumer))
    c = make_producer(handoff, produce_queue)
    loop.create_task(writer.run(c))
    metrics.start()
    loop.run_forever()
