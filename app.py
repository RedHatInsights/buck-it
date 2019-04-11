import os
import json
import aiohttp
import aiobotocore
import aiokafka
import asyncio
from kafkahelpers import ReconnectingClient

loop = asyncio.get_event_loop()

QUEUE = os.environ.get("QUEUE", "platform.upload.buckit")
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

kafka_client = aiokafka.AIOKafkaConsumer(
    QUEUE, loop=loop, bootstrap_servers=BOOT, group_id=GROUP
)
client = ReconnectingClient(kafka_client, "reader")


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

    await asyncio.sleep(0.5)


if __name__ == "__main__":
    loop.create_task()
    loop.run_forever()
