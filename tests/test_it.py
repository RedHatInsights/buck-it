import asyncio
import collections
import json
from functools import partial
import pytest
from buckit import app, metrics


class Message:
    def __init__(self, value):
        self.value = json.dumps(value)


def test_unpack():
    orig_doc = {
        "payload_id": "testing",
        "url": "https://localhost:8080",
        "category": "testing",
        "request_id": "test",
        "service": "testing",
    }
    v = json.dumps(orig_doc)
    url, bucket, doc = app.unpack(v, mapping={"testing": None})
    assert url == "https://localhost:8080"
    assert bucket is None
    assert doc == orig_doc


def _unpacker(v):
    return "test_url", "test_bucket"


async def _fetcher(url):
    return "test_data"


async def _storer(payload, bucket, doc):
    pass


async def make_iter(q):
    try:
        while True:
            item = q.get_nowait()
            print("inside_make_iter:", item)
            yield item
    except asyncio.QueueEmpty:
        pass


_unpack_with_mapping = partial(app.unpack, mapping={"test": None})


@pytest.mark.asyncio
async def test_consumer():
    q = collections.deque()
    client = asyncio.Queue()
    orig_doc = {"request_id": "test", "service": "test", "url": "test", "category": "test"}
    msg = Message(orig_doc)
    await client.put(msg)
    await app.consumer(
        make_iter(client),
        unpacker=_unpack_with_mapping,
        fetcher=_fetcher,
        storer=_storer,
        produce_queue=q,
    )
    assert q[0] == {"validation": "success", **orig_doc}


@pytest.mark.asyncio
async def test_fetcher():
    q = collections.deque()
    client = asyncio.Queue()
    orig_doc = {"request_id": "test", "service": "test", "url": "https://www.google.com", "category": "test"}
    msg = Message(orig_doc)
    await client.put(msg)
    await app.consumer(
        make_iter(client),
        unpacker=_unpack_with_mapping,
        storer=_storer,
        produce_queue=q,
    )
    assert q[0] == {"validation": "success", **orig_doc}


@pytest.mark.parametrize(
    "test_input,expected",
    [
        ("application/json", "application/json"),
        ("application/json; charset=utf8", "application/json"),
    ],
)
def test_parse_content_type(test_input, expected):
    assert metrics._parse_content_type(test_input) == expected
