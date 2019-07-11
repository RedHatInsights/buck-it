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
from buckit import metrics

logging.basicConfig(level=logging.INFO)


def context_filter(record):
    record.request_id = REQUEST_ID.get()
    return True

def spam_filter(record):
    return not "GET /metrics" in record.msg


logger = logging.getLogger(__name__)
logger.addFilter(context_filter)
access_logger = logging.getLogger("aiohttp.access")
access_logger.addFilter(spam_filter)

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


def unpack(v, mapping=BUCKET_MAP):
    with metrics.json_loads_time.time():
        doc = json.loads(v)
    REQUEST_ID.set(doc["request_id"])
    return doc["url"], mapping[doc["service"]], doc


async def consumer(
    client, unpacker=unpack, fetcher=fetch, storer=store, produce_queue=None
):
    async for msg in client:
        try:
            url, bucket, doc = unpacker(msg.value)
        except Exception:
            logger.exception("Failed to unpack msg.value")
            continue

        try:
            payload = await fetcher(url)
        except Exception:
            logger.exception("Failed to fetch '%s'.", url)
            continue

        try:
            await storer(payload, bucket)
        except Exception:
            logger.exception("Failed to store to '%s'", bucket)
            continue

        produce_queue.append({"validation": "success", **doc})


async def handoff(client, item):
    await client.send_and_wait(RESPONSE_QUEUE, json.dumps(item).encode("utf-8"))


def main():
    reader, writer = make_pair(QUEUE, GROUP, BOOT)
    produce_queue = deque()
    loop.create_task(reader.run(partial(consumer, produce_queue=produce_queue)))
    c = make_producer(handoff, produce_queue)
    loop.create_task(writer.run(c))
    metrics.start()
    loop.run_forever()


if __name__ == "__main__":
    main()