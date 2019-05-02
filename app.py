import aiobotocore
import aiohttp
import asyncio
import json
import logging
import os
from collections import deque
from contextvars import ContextVar
from functools import partial

from kafkahelpers import make_pair, make_producer
import metrics

logging.basicConfig(level=logging.INFO)


class ContextFilter(logging.Filter):
    def filter(record):
        record.request_id = REQUEST_ID.get()


logger = logging.getLogger(__name__)
logger.addFilter(ContextFilter())

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


@metrics.time(metrics.fetch_time)
async def fetch(url):
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            return await response.read()


@metrics.time(metrics.s3_write_time)
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
        await client.put_object(Bucket=bucket, Key=REQUEST_ID.get(), Body=payload)
        metrics.payload_size.observe(size)
        metrics.bucket_counter.labels(bucket).inc()


def unpack(msg):
    with metrics.json_loads_time.time():
        doc = json.loads(msg.value)
    REQUEST_ID.set(doc["payload_id"])
    return doc["url"], BUCKET_MAP[doc["category"]]


async def consumer(client, fetcher=fetch, storer=store, produce_queue=None):
    async for msg in client:
        try:
            url, bucket = unpack(msg.value)
        except Exception:
            logger.exception("Failed to unpack msg.value")
            continue

        try:
            payload = await fetch(url)
        except Exception:
            logger.exception("Failed to fetch '%s'.", url)
            continue

        try:
            await store(payload, bucket)
        except Exception:
            logger.exception("Failed to store to '%s'", bucket)
            continue

        produce_queue.append({"validation": "handoff", "payload_id": REQUEST_ID.get()})


async def handoff(client, item):
    await client.send_and_wait(RESPONSE_QUEUE, json.dumps(item).encode("utf-8"))


if __name__ == "__main__":
    reader, writer = make_pair(QUEUE, GROUP, BOOT)
    produce_queue = deque()
    loop.create_task(reader.run(partial(consumer, produce_queue=produce_queue)))
    c = make_producer(handoff, produce_queue)
    loop.create_task(writer.run(c))
    metrics.start()
    loop.run_forever()
