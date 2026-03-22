import asyncio
import time
from collections.abc import AsyncIterable, Iterable
from typing import Literal

import fastapi.routing
import pytest
from fastapi import APIRouter, FastAPI
from fastapi.responses import EventSourceResponse
from fastapi.sse import ServerSentEvent, get_sse_data_type, get_sse_variants
from fastapi.testclient import TestClient
from pydantic import BaseModel, ValidationError


class Item(BaseModel):
    name: str
    description: str | None = None


class Message(BaseModel):
    text: str


items = [
    Item(name="Plumbus", description="A multi-purpose household device."),
    Item(name="Portal Gun", description="A portal opening device."),
    Item(name="Meeseeks Box", description="A box that summons a Meeseeks."),
]


app = FastAPI()


@app.get("/items/stream", response_class=EventSourceResponse)
async def sse_items() -> AsyncIterable[Item]:
    for item in items:
        yield item


@app.get("/items/stream-sync", response_class=EventSourceResponse)
def sse_items_sync() -> Iterable[Item]:
    yield from items


@app.get("/items/stream-no-annotation", response_class=EventSourceResponse)
async def sse_items_no_annotation():
    for item in items:
        yield item


@app.get("/items/stream-sync-no-annotation", response_class=EventSourceResponse)
def sse_items_sync_no_annotation():
    yield from items


@app.get("/items/stream-dict", response_class=EventSourceResponse)
async def sse_items_dict():
    for item in items:
        yield {"name": item.name, "description": item.description}


@app.get("/items/stream-sse-event", response_class=EventSourceResponse)
async def sse_items_event():
    yield ServerSentEvent(data="hello", event="greeting", id="1")
    yield ServerSentEvent(data={"key": "value"}, event="json-data", id="2")
    yield ServerSentEvent(comment="just a comment")
    yield ServerSentEvent(data="retry-test", retry=5000)


@app.get("/items/stream-mixed", response_class=EventSourceResponse)
async def sse_items_mixed() -> AsyncIterable[Item]:
    yield items[0]
    yield ServerSentEvent(data="custom-event", event="special")
    yield items[1]


@app.get("/items/stream-string", response_class=EventSourceResponse)
async def sse_items_string():
    yield ServerSentEvent(data="plain text data")


@app.post("/items/stream-post", response_class=EventSourceResponse)
async def sse_items_post() -> AsyncIterable[Item]:
    for item in items:
        yield item


@app.get("/items/stream-raw", response_class=EventSourceResponse)
async def sse_items_raw():
    yield ServerSentEvent(raw_data="plain text without quotes")
    yield ServerSentEvent(raw_data="<div>html fragment</div>", event="html")
    yield ServerSentEvent(raw_data="cpu,87.3,1709145600", event="csv")


router = APIRouter()


@router.get("/events", response_class=EventSourceResponse)
async def stream_events():
    yield {"msg": "hello"}
    yield {"msg": "world"}


app.include_router(router, prefix="/api")


@pytest.fixture(name="client")
def client_fixture():
    with TestClient(app) as c:
        yield c


def test_async_generator_with_model(client: TestClient):
    response = client.get("/items/stream")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/event-stream; charset=utf-8"
    assert response.headers["cache-control"] == "no-cache"
    assert response.headers["x-accel-buffering"] == "no"

    lines = response.text.strip().split("\n")
    data_lines = [line for line in lines if line.startswith("data: ")]
    assert len(data_lines) == 3
    assert '"name":"Plumbus"' in data_lines[0] or '"name": "Plumbus"' in data_lines[0]
    assert (
        '"name":"Portal Gun"' in data_lines[1]
        or '"name": "Portal Gun"' in data_lines[1]
    )
    assert (
        '"name":"Meeseeks Box"' in data_lines[2]
        or '"name": "Meeseeks Box"' in data_lines[2]
    )


def test_sync_generator_with_model(client: TestClient):
    response = client.get("/items/stream-sync")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/event-stream; charset=utf-8"

    data_lines = [
        line for line in response.text.strip().split("\n") if line.startswith("data: ")
    ]
    assert len(data_lines) == 3


def test_async_generator_no_annotation(client: TestClient):
    response = client.get("/items/stream-no-annotation")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/event-stream; charset=utf-8"

    data_lines = [
        line for line in response.text.strip().split("\n") if line.startswith("data: ")
    ]
    assert len(data_lines) == 3


def test_sync_generator_no_annotation(client: TestClient):
    response = client.get("/items/stream-sync-no-annotation")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/event-stream; charset=utf-8"

    data_lines = [
        line for line in response.text.strip().split("\n") if line.startswith("data: ")
    ]
    assert len(data_lines) == 3


def test_dict_items(client: TestClient):
    response = client.get("/items/stream-dict")
    assert response.status_code == 200
    data_lines = [
        line for line in response.text.strip().split("\n") if line.startswith("data: ")
    ]
    assert len(data_lines) == 3
    assert '"name"' in data_lines[0]


def test_post_method_sse(client: TestClient):
    """SSE should work with POST (needed for MCP compatibility)."""
    response = client.post("/items/stream-post")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/event-stream; charset=utf-8"
    data_lines = [
        line for line in response.text.strip().split("\n") if line.startswith("data: ")
    ]
    assert len(data_lines) == 3


def test_sse_events_with_fields(client: TestClient):
    response = client.get("/items/stream-sse-event")
    assert response.status_code == 200
    text = response.text

    assert "event: greeting\n" in text
    assert 'data: "hello"\n' in text
    assert "id: 1\n" in text

    assert "event: json-data\n" in text
    assert "id: 2\n" in text
    assert 'data: {"key": "value"}\n' in text

    assert ": just a comment\n" in text

    assert "retry: 5000\n" in text
    assert 'data: "retry-test"\n' in text


def test_mixed_plain_and_sse_events(client: TestClient):
    response = client.get("/items/stream-mixed")
    assert response.status_code == 200
    text = response.text

    assert "event: special\n" in text
    assert 'data: "custom-event"\n' in text
    assert '"name"' in text


def test_string_data_json_encoded(client: TestClient):
    """Strings are always JSON-encoded (quoted)."""
    response = client.get("/items/stream-string")
    assert response.status_code == 200
    assert 'data: "plain text data"\n' in response.text


def test_server_sent_event_null_id_rejected():
    with pytest.raises(ValueError, match="null"):
        ServerSentEvent(data="test", id="has\0null")


def test_server_sent_event_negative_retry_rejected():
    with pytest.raises(ValueError):
        ServerSentEvent(data="test", retry=-1)


def test_server_sent_event_float_retry_rejected():
    with pytest.raises(ValueError):
        ServerSentEvent(data="test", retry=1.5)  # type: ignore[arg-type]


def test_raw_data_sent_without_json_encoding(client: TestClient):
    """raw_data is sent as-is, not JSON-encoded."""
    response = client.get("/items/stream-raw")
    assert response.status_code == 200
    text = response.text

    # raw_data should appear without JSON quotes
    assert "data: plain text without quotes\n" in text
    # Not JSON-quoted
    assert 'data: "plain text without quotes"' not in text

    assert "event: html\n" in text
    assert "data: <div>html fragment</div>\n" in text

    assert "event: csv\n" in text
    assert "data: cpu,87.3,1709145600\n" in text


def test_data_and_raw_data_mutually_exclusive():
    """Cannot set both data and raw_data."""
    with pytest.raises(ValueError, match="Cannot set both"):
        ServerSentEvent(data="json", raw_data="raw")


def test_sse_on_router_included_in_app(client: TestClient):
    response = client.get("/api/events")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/event-stream; charset=utf-8"
    data_lines = [
        line for line in response.text.strip().split("\n") if line.startswith("data: ")
    ]
    assert len(data_lines) == 2


# Keepalive ping tests


keepalive_app = FastAPI()


@keepalive_app.get("/slow-async", response_class=EventSourceResponse)
async def slow_async_stream():
    yield {"n": 1}
    # Sleep longer than the (monkeypatched) ping interval so a keepalive
    # comment is emitted before the next item.
    await asyncio.sleep(0.3)
    yield {"n": 2}


@keepalive_app.get("/slow-sync", response_class=EventSourceResponse)
def slow_sync_stream():
    yield {"n": 1}
    time.sleep(0.3)
    yield {"n": 2}


def test_keepalive_ping_async(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(fastapi.routing, "_PING_INTERVAL", 0.05)
    with TestClient(keepalive_app) as c:
        response = c.get("/slow-async")
    assert response.status_code == 200
    text = response.text
    # The keepalive comment ": ping" should appear between the two data events
    assert ": ping\n" in text
    data_lines = [line for line in text.split("\n") if line.startswith("data: ")]
    assert len(data_lines) == 2


def test_keepalive_ping_sync(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(fastapi.routing, "_PING_INTERVAL", 0.05)
    with TestClient(keepalive_app) as c:
        response = c.get("/slow-sync")
    assert response.status_code == 200
    text = response.text
    assert ": ping\n" in text
    data_lines = [line for line in text.split("\n") if line.startswith("data: ")]
    assert len(data_lines) == 2


def test_no_keepalive_when_fast(client: TestClient):
    """No keepalive comment when items arrive quickly."""
    response = client.get("/items/stream")
    assert response.status_code == 200
    # KEEPALIVE_COMMENT is ": ping\n\n".
    assert ": ping\n" not in response.text


# ---------------------------------------------------------------------------
# Generic ServerSentEvent[T] tests
# ---------------------------------------------------------------------------


def test_get_sse_data_type_parameterized():
    """get_sse_data_type returns the type argument for ServerSentEvent[T]."""
    assert get_sse_data_type(ServerSentEvent[Item]) is Item


def test_get_sse_data_type_bare():
    """get_sse_data_type returns None for bare ServerSentEvent."""
    assert get_sse_data_type(ServerSentEvent) is None


def test_get_sse_data_type_non_sse():
    """get_sse_data_type returns None for unrelated types."""
    assert get_sse_data_type(Item) is None
    assert get_sse_data_type(str) is None
    assert get_sse_data_type(None) is None


def test_get_sse_data_type_subclass_no_type_param():
    """get_sse_data_type returns None for a plain ServerSentEvent subclass."""

    class MyEvent(ServerSentEvent):
        pass

    assert get_sse_data_type(MyEvent) is None


def test_generic_sse_construction_validates_data():
    """ServerSentEvent[Item] requires data to be an Item."""
    item = Item(name="Foo", description=None)
    evt = ServerSentEvent[Item](data=item, event="update")
    assert evt.data == item
    assert evt.event == "update"


def test_generic_sse_rejects_wrong_type():
    """ServerSentEvent[Item] rejects data that is not an Item."""
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        ServerSentEvent[Item](data="not an item")


def test_generic_sse_rejects_none_data():
    """ServerSentEvent[Item] rejects None as data (use Item | None if optional)."""
    import pytest
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        ServerSentEvent[Item]()


def test_generic_sse_optional_data_allows_none():
    """ServerSentEvent[Item | None] accepts None as data."""
    evt = ServerSentEvent[Item | None]()
    assert evt.data is None


def test_bare_sse_still_accepts_none_data():
    """Bare ServerSentEvent (T=Any) still accepts None (backward compat)."""
    evt = ServerSentEvent()
    assert evt.data is None


# App-level test for generic SSE streaming and OpenAPI schema

_generic_app = FastAPI()


@_generic_app.get("/stream", response_class=EventSourceResponse)
async def _stream_typed() -> AsyncIterable[ServerSentEvent[Item]]:
    for i, item in enumerate(items):
        yield ServerSentEvent[Item](data=item, event="item", id=str(i + 1))


def test_generic_sse_streams_correctly():
    with TestClient(_generic_app) as c:
        response = c.get("/stream")
    assert response.status_code == 200
    assert response.headers["content-type"] == "text/event-stream; charset=utf-8"
    data_lines = [
        line for line in response.text.split("\n") if line.startswith("data: ")
    ]
    assert len(data_lines) == 3
    import json

    first = json.loads(data_lines[0][len("data: ") :])
    assert first["name"] == "Plumbus"


def test_generic_sse_openapi_has_content_schema():
    with TestClient(_generic_app) as c:
        response = c.get("/openapi.json")
    assert response.status_code == 200
    schema = response.json()
    sse_schema = schema["paths"]["/stream"]["get"]["responses"]["200"]["content"][
        "text/event-stream"
    ]["itemSchema"]
    assert sse_schema.get("required") == ["data"]
    data_prop = sse_schema["properties"]["data"]
    assert data_prop.get("contentMediaType") == "application/json"
    content_schema = data_prop.get("contentSchema", {})
    # Should reference Item (either inline or via $ref)
    assert "$ref" in content_schema or content_schema.get("title") == "Item"


def test_bare_sse_openapi_has_no_content_schema():
    """Bare ServerSentEvent return type produces no contentSchema (backward compat)."""
    bare_app = FastAPI()

    @bare_app.get("/stream", response_class=EventSourceResponse)
    async def _bare_stream() -> AsyncIterable[ServerSentEvent]:
        yield ServerSentEvent(comment="ping")

    with TestClient(bare_app) as c:
        response = c.get("/openapi.json")
    assert response.status_code == 200
    schema = response.json()
    sse_schema = schema["paths"]["/stream"]["get"]["responses"]["200"]["content"][
        "text/event-stream"
    ]["itemSchema"]
    assert "required" not in sse_schema
    assert "contentSchema" not in sse_schema["properties"]["data"]


# ---------------------------------------------------------------------------
# ServerSentEvent[Data, Event] — Event type parameter tests
# ---------------------------------------------------------------------------


def test_get_sse_variants_single_with_event():
    """get_sse_variants returns correct mapping for ServerSentEvent[Data, Literal]."""
    result = get_sse_variants(ServerSentEvent[Item, Literal["item"]])
    assert result is not None
    assert len(result) == 1
    key = next(iter(result))
    assert key == Literal["item"]
    assert result[key] == frozenset({Item})


def test_get_sse_variants_single_data_only():
    """get_sse_variants maps to str|None event key for ServerSentEvent[Data]."""
    result = get_sse_variants(ServerSentEvent[Item])
    assert result is not None
    assert len(result) == 1
    event_type = next(iter(result))
    assert event_type == (str | None)
    assert result[event_type] == frozenset({Item})


def test_get_sse_variants_bare_returns_none():
    """Bare ServerSentEvent returns None from get_sse_variants."""
    assert get_sse_variants(ServerSentEvent) is None


def test_get_sse_variants_union():
    """get_sse_variants groups data types by event type for union annotations."""
    annotation = (
        ServerSentEvent[Item, Literal["item"]]
        | ServerSentEvent[Message, Literal["message"]]
    )
    result = get_sse_variants(annotation)
    assert result is not None
    assert len(result) == 2
    item_key = Literal["item"]
    msg_key = Literal["message"]
    assert result[item_key] == frozenset({Item})
    assert result[msg_key] == frozenset({Message})


def test_get_sse_variants_shared_event_merges_data_types():
    """Union members sharing the same Event literal merge their Data types."""
    annotation = (
        ServerSentEvent[Item, Literal["update"]]
        | ServerSentEvent[Message, Literal["update"]]
    )
    result = get_sse_variants(annotation)
    assert result is not None
    assert len(result) == 1
    assert result[Literal["update"]] == frozenset({Item, Message})


def test_get_sse_variants_plain_model():
    """Plain model T is treated as SSEVariant(T, NoneType)."""
    result = get_sse_variants(Item)
    assert result is not None
    assert result[type(None)] == frozenset({Item})


def test_sse_event_literal_validates_wrong_value():
    """ServerSentEvent[Item, Literal['item']] rejects event='wrong'."""
    item = Item(name="Foo")
    with pytest.raises(ValidationError):
        ServerSentEvent[Item, Literal["item"]](data=item, event="wrong")


def test_sse_event_literal_accepts_correct_value():
    """ServerSentEvent[Item, Literal['item']] accepts event='item'."""
    item = Item(name="Foo")
    evt = ServerSentEvent[Item, Literal["item"]](data=item, event="item")
    assert evt.event == "item"


def test_sse_event_literal_rejects_none():
    """ServerSentEvent[Item, Literal['item']] rejects None event (validate_default)."""
    item = Item(name="Foo")
    with pytest.raises(ValidationError):
        ServerSentEvent[Item, Literal["item"]](data=item)


# App-level tests for discriminated union SSE schema generation

_discriminated_app = FastAPI()


@_discriminated_app.get("/stream", response_class=EventSourceResponse)
async def _stream_discriminated() -> AsyncIterable[
    ServerSentEvent[Item, Literal["item"]] | ServerSentEvent[Message, Literal["message"]]
]:
    yield ServerSentEvent[Item, Literal["item"]](
        data=Item(name="Plumbus", description=None), event="item"
    )
    yield ServerSentEvent[Message, Literal["message"]](
        data=Message(text="hello"), event="message"
    )


def test_discriminated_union_streams_correctly():
    with TestClient(_discriminated_app) as c:
        response = c.get("/stream")
    assert response.status_code == 200
    lines = response.text.split("\n")
    event_lines = [l for l in lines if l.startswith("event: ")]
    assert event_lines == ["event: item", "event: message"]


def test_discriminated_union_openapi_one_of():
    """Union of SSE types produces oneOf with discriminator in itemSchema."""
    with TestClient(_discriminated_app) as c:
        response = c.get("/openapi.json")
    schema = response.json()
    sse_schema = schema["paths"]["/stream"]["get"]["responses"]["200"]["content"][
        "text/event-stream"
    ]["itemSchema"]

    assert "oneOf" in sse_schema
    assert sse_schema.get("discriminator") == {"propertyName": "event"}

    variants = sse_schema["oneOf"]
    assert len(variants) == 2

    # Find item variant
    item_variant = next(v for v in variants if v.get("properties", {}).get("event", {}).get("const") == "item")
    assert "event" in item_variant.get("required", [])
    assert "data" in item_variant.get("required", [])
    assert item_variant["properties"]["data"]["contentMediaType"] == "application/json"
    assert "$ref" in item_variant["properties"]["data"]["contentSchema"]
    assert "Item" in item_variant["properties"]["data"]["contentSchema"]["$ref"]

    # Find message variant
    msg_variant = next(v for v in variants if v.get("properties", {}).get("event", {}).get("const") == "message")
    assert "$ref" in msg_variant["properties"]["data"]["contentSchema"]
    assert "Message" in msg_variant["properties"]["data"]["contentSchema"]["$ref"]


def test_single_sse_with_event_literal_schema():
    """Single ServerSentEvent[Data, Literal['x']] produces const event in itemSchema."""
    app = FastAPI()

    @app.get("/stream", response_class=EventSourceResponse)
    async def _stream() -> AsyncIterable[ServerSentEvent[Item, Literal["item"]]]:
        yield ServerSentEvent[Item, Literal["item"]](data=Item(name="x"), event="item")

    with TestClient(app) as c:
        response = c.get("/openapi.json")
    schema = response.json()
    sse_schema = schema["paths"]["/stream"]["get"]["responses"]["200"]["content"][
        "text/event-stream"
    ]["itemSchema"]

    # Single variant — no oneOf wrapper
    assert "oneOf" not in sse_schema
    assert sse_schema["properties"]["event"] == {"type": "string", "const": "item"}
    assert "event" in sse_schema.get("required", [])
    assert "data" in sse_schema.get("required", [])


def test_multi_literal_event_produces_enum():
    """Literal['a', 'b'] event type produces enum in OpenAPI schema."""
    app = FastAPI()

    @app.get("/stream", response_class=EventSourceResponse)
    async def _stream() -> AsyncIterable[ServerSentEvent[Item, Literal["a", "b"]]]:
        yield ServerSentEvent[Item, Literal["a", "b"]](data=Item(name="x"), event="a")

    with TestClient(app) as c:
        response = c.get("/openapi.json")
    schema = response.json()
    sse_schema = schema["paths"]["/stream"]["get"]["responses"]["200"]["content"][
        "text/event-stream"
    ]["itemSchema"]
    assert sse_schema["properties"]["event"] == {"type": "string", "enum": ["a", "b"]}


def test_mixed_union_no_discriminator():
    """Mixed union (SSE + plain model) produces oneOf but no discriminator."""
    app = FastAPI()

    @app.get("/stream", response_class=EventSourceResponse)
    async def _stream() -> AsyncIterable[
        ServerSentEvent[Item, Literal["item"]] | Message
    ]:
        yield ServerSentEvent[Item, Literal["item"]](data=Item(name="x"), event="item")
        yield Message(text="bye")  # type: ignore[misc]

    with TestClient(app) as c:
        response = c.get("/openapi.json")
    schema = response.json()
    sse_schema = schema["paths"]["/stream"]["get"]["responses"]["200"]["content"][
        "text/event-stream"
    ]["itemSchema"]

    assert "oneOf" in sse_schema
    # Not all variants have event literals (Message has NoneType key), so no discriminator
    assert "discriminator" not in sse_schema
    variants = sse_schema["oneOf"]
    assert len(variants) == 2

    # The item variant has event constraint
    item_v = next(v for v in variants if v.get("properties", {}).get("event", {}).get("const") == "item")
    assert item_v is not None

    # The Message variant has no event constraint
    msg_v = next(v for v in variants if "const" not in v.get("properties", {}).get("event", {}))
    assert "$ref" in msg_v["properties"]["data"]["contentSchema"]
    assert "Message" in msg_v["properties"]["data"]["contentSchema"]["$ref"]
