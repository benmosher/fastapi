from typing import Annotated, Any, Generic, Literal, Union, get_args, get_origin

from annotated_doc import Doc
from pydantic import AfterValidator, BaseModel, ConfigDict, Field, model_validator
from starlette.responses import StreamingResponse
from typing_extensions import Self, TypeVar

try:
    from types import UnionType as _UnionType
except ImportError:  # pragma: nocover
    _UnionType = None  # type: ignore[assignment,misc]

_NoneType: type = type(None)

Data = TypeVar("Data", default=Any)
"""Type variable for the `data` payload of a `ServerSentEvent`.

Use ``ServerSentEvent[MyModel]`` to indicate that every event in the
stream carries a ``MyModel`` instance as its ``data`` field.
"""

Event = TypeVar("Event", default=str | None)
"""Type variable for the ``event`` field of a `ServerSentEvent`.

Use ``ServerSentEvent[MyModel, Literal['event_name']]`` to constrain the
``event`` field to a specific string literal, enabling discriminated unions
in OpenAPI schema generation.
"""

# Canonical SSE event schema matching the OpenAPI 3.2 spec
# (Section 4.14.4 "Special Considerations for Server-Sent Events")
_SSE_EVENT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "data": {"type": "string"},
        "event": {"type": "string"},
        "id": {"type": "string"},
        "retry": {"type": "integer", "minimum": 0},
    },
}


class EventSourceResponse(StreamingResponse):
    """Streaming response with `text/event-stream` media type.

    Use as `response_class=EventSourceResponse` on a *path operation* that uses `yield`
    to enable Server Sent Events (SSE) responses.

    Works with **any HTTP method** (`GET`, `POST`, etc.), which makes it compatible
    with protocols like MCP that stream SSE over `POST`.

    The actual encoding logic lives in the FastAPI routing layer. This class
    serves mainly as a marker and sets the correct `Content-Type`.
    """

    media_type = "text/event-stream"


def _check_id_no_null(v: str | None) -> str | None:
    if v is not None and "\0" in v:
        raise ValueError("SSE 'id' must not contain null characters")
    return v


class ServerSentEvent(BaseModel, Generic[Data, Event]):
    """Represents a single Server-Sent Event.

    When `yield`ed from a *path operation function* that uses
    `response_class=EventSourceResponse`, each `ServerSentEvent` is encoded
    into the [SSE wire format](https://html.spec.whatwg.org/multipage/server-sent-events.html#parsing-an-event-stream)
    (`text/event-stream`).

    If you yield a plain object (dict, Pydantic model, etc.) instead, it is
    automatically JSON-encoded and sent as the `data:` field.

    All `data` values **including plain strings** are JSON-serialized.

    For example, `data="hello"` produces `data: "hello"` on the wire (with
    quotes).
    """

    # validate_default=True ensures that when Data is a concrete type (e.g.
    # ServerSentEvent[Item]), omitting `data` raises a ValidationError rather
    # than silently storing the None default.  Without this, Pydantic skips
    # default validation and None would be accepted even when Data=Item.
    model_config = ConfigDict(validate_default=True)

    data: Annotated[
        Data,
        Doc(
            """
            The event payload.

            Can be any JSON-serializable value: a Pydantic model, dict, list,
            string, number, etc. It is **always** serialized to JSON: strings
            are quoted (`"hello"` becomes `data: "hello"` on the wire).

            The type of `data` is controlled by the type variable `Data`:

            * `ServerSentEvent[Item]` — `data` must be an `Item` instance
              (non-nullable; omitting `data` will raise a validation error).
            * `ServerSentEvent[Item | None]` — `data` may be `None`, which is
              useful for comment-only or metadata events.
            * Bare `ServerSentEvent` (no type parameter) — `data` accepts any
              value including `None`, preserving backward compatibility.

            Mutually exclusive with `raw_data`.
            """
        ),
    ] = None  # type: ignore[assignment]
    raw_data: Annotated[
        str | None,
        Doc(
            """
            Raw string to send as the `data:` field **without** JSON encoding.

            Use this when you need to send pre-formatted text, HTML fragments,
            CSV lines, or any non-JSON payload. The string is placed directly
            into the `data:` field as-is.

            Mutually exclusive with `data`.
            """
        ),
    ] = None
    event: Annotated[
        Event,
        Doc(
            """
            Optional event type name.

            Maps to `addEventListener(event, ...)` on the browser. When omitted,
            the browser dispatches on the generic `message` event.

            The type of `event` is controlled by the type variable `Event`:

            * ``ServerSentEvent[Data, Literal['item']]`` — ``event`` must be the
              string ``'item'``; the OpenAPI schema adds ``const: "item"`` to the
              event property.
            * ``ServerSentEvent[Data, Literal['a', 'b']]`` — ``event`` must be one
              of the listed strings; the schema uses ``enum``.
            * Bare ``ServerSentEvent`` or ``ServerSentEvent[Data]`` — ``event``
              accepts any string or ``None``, preserving backward compatibility.
            """
        ),
    ] = None  # type: ignore[assignment]
    id: Annotated[
        str | None,
        AfterValidator(_check_id_no_null),
        Doc(
            """
            Optional event ID.

            The browser sends this value back as the `Last-Event-ID` header on
            automatic reconnection. **Must not contain null (`\\0`) characters.**
            """
        ),
    ] = None
    retry: Annotated[
        int | None,
        Field(ge=0),
        Doc(
            """
            Optional reconnection time in **milliseconds**.

            Tells the browser how long to wait before reconnecting after the
            connection is lost. Must be a non-negative integer.
            """
        ),
    ] = None
    comment: Annotated[
        str | None,
        Doc(
            """
            Optional comment line(s).

            Comment lines start with `:` in the SSE wire format and are ignored by
            `EventSource` clients. Useful for keep-alive pings to prevent
            proxy/load-balancer timeouts.
            """
        ),
    ] = None

    @model_validator(mode="after")
    def _check_data_exclusive(self) -> Self:
        if self.data is not None and self.raw_data is not None:
            raise ValueError(
                "Cannot set both 'data' and 'raw_data' on the same "
                "ServerSentEvent. Use 'data' for JSON-serialized payloads "
                "or 'raw_data' for pre-formatted strings."
            )
        return self


def format_sse_event(
    *,
    data_str: Annotated[
        str | None,
        Doc(
            """
            Pre-serialized data string to use as the `data:` field.
            """
        ),
    ] = None,
    event: Annotated[
        str | None,
        Doc(
            """
            Optional event type name (`event:` field).
            """
        ),
    ] = None,
    id: Annotated[
        str | None,
        Doc(
            """
            Optional event ID (`id:` field).
            """
        ),
    ] = None,
    retry: Annotated[
        int | None,
        Doc(
            """
            Optional reconnection time in milliseconds (`retry:` field).
            """
        ),
    ] = None,
    comment: Annotated[
        str | None,
        Doc(
            """
            Optional comment line(s) (`:` prefix).
            """
        ),
    ] = None,
) -> bytes:
    """Build SSE wire-format bytes from **pre-serialized** data.

    The result always ends with `\n\n` (the event terminator).
    """
    lines: list[str] = []

    if comment is not None:
        for line in comment.splitlines():
            lines.append(f": {line}")

    if event is not None:
        lines.append(f"event: {event}")

    if data_str is not None:
        for line in data_str.splitlines():
            lines.append(f"data: {line}")

    if id is not None:
        lines.append(f"id: {id}")

    if retry is not None:
        lines.append(f"retry: {retry}")

    lines.append("")
    lines.append("")
    return "\n".join(lines).encode("utf-8")


# Keep-alive comment, per the SSE spec recommendation
KEEPALIVE_COMMENT = b": ping\n\n"

# Seconds between keep-alive pings when a generator is idle.
# Private but importable so tests can monkeypatch it.
_PING_INTERVAL: float = 15.0


def get_event_literals(event_type: Any) -> tuple[str, ...] | None:
    """Return the string values from a ``Literal[...]`` event type, or ``None``.

    Used by the OpenAPI schema builder to decide whether to add ``const``/``enum``
    constraints to the ``event`` property of an SSE event schema.
    """
    if get_origin(event_type) is Literal:
        return get_args(event_type)
    return None


def _extract_single_variant(annotation: Any) -> tuple[Any, Any] | None:
    """Extract ``(data_type, event_type)`` from a single type annotation.

    Returns ``None`` for bare ``ServerSentEvent`` or ``NoneType`` (which has no
    meaningful data payload).  All other types produce a variant:

    * Parameterised ``ServerSentEvent[Data]`` → ``(Data, str | None)``
    * Parameterised ``ServerSentEvent[Data, Event]`` → ``(Data, Event)``
    * Plain type ``T`` → ``(T, type(None))`` — typed data, no event constraint

    Pydantic's generic BaseModel creates a real subclass (not a ``_GenericAlias``),
    so ``get_origin`` returns ``None``.  Instead we inspect
    ``__pydantic_generic_metadata__`` which Pydantic always attaches to
    parameterised models.
    """
    # Skip NoneType — it has no data payload
    if annotation is _NoneType:
        return None

    # Parameterised ServerSentEvent subclass
    if isinstance(annotation, type) and issubclass(annotation, ServerSentEvent):
        if annotation is ServerSentEvent:
            return None  # bare — no type info
        meta = getattr(annotation, "__pydantic_generic_metadata__", None)
        if not meta:
            return None
        args = meta.get("args", ())
        if not args or isinstance(args[0], TypeVar):
            return None
        data_type = args[0]
        # Use the default Event (str | None) when the second arg is absent or still a TypeVar
        if len(args) >= 2 and not isinstance(args[1], TypeVar):
            event_type = args[1]
        else:
            event_type = str | None
        return (data_type, event_type)

    # Plain type (BaseModel subclass, dataclass, scalar, …)
    if isinstance(annotation, type):
        return (annotation, _NoneType)

    return None


def get_sse_variants(annotation: Any) -> dict[Any, frozenset[type]] | None:
    """Build an event-type → data-types mapping from a stream item annotation.

    Handles single types, parameterised ``ServerSentEvent`` types, and unions.
    Returns ``None`` only when the annotation is bare ``ServerSentEvent``
    (no usable type information).

    Examples::

        # Single parameterised SSE
        get_sse_variants(ServerSentEvent[Item, Literal['item']])
        # → {Literal['item']: frozenset({Item})}

        # Default event (no literal constraint)
        get_sse_variants(ServerSentEvent[Item])
        # → {str | None: frozenset({Item})}

        # Union of SSE types
        get_sse_variants(
            ServerSentEvent[Item, Literal['item']]
            | ServerSentEvent[Message, Literal['message']]
        )
        # → {Literal['item']: frozenset({Item}),
        #    Literal['message']: frozenset({Message})}

        # Mixed union: plain model treated as SSEVariant(T, NoneType)
        get_sse_variants(ServerSentEvent[Item, Literal['item']] | Message)
        # → {Literal['item']: frozenset({Item}),
        #    type(None): frozenset({Message})}
    """
    origin = get_origin(annotation)
    is_union = origin is Union or (
        _UnionType is not None and isinstance(annotation, _UnionType)
    )

    if is_union:
        result: dict[Any, set[type]] = {}
        for member in get_args(annotation):
            info = _extract_single_variant(member)
            if info is not None:
                data_type, event_type = info
                result.setdefault(event_type, set()).add(data_type)
        if not result:
            return None
        return {k: frozenset(v) for k, v in result.items()}

    info = _extract_single_variant(annotation)
    if info is None:
        return None
    data_type, event_type = info
    return {event_type: frozenset({data_type})}


def get_sse_data_type(annotation: Any) -> Any | None:
    """Extract the ``Data`` type from a ``ServerSentEvent[Data]`` annotation.

    .. deprecated::
        Use :func:`get_sse_variants` instead.  This wrapper is retained for
        backward compatibility.

    Returns ``None`` for bare ``ServerSentEvent`` (no type parameter) or for
    any annotation that is not a *single* parameterised ``ServerSentEvent``.
    """
    if not (isinstance(annotation, type) and issubclass(annotation, ServerSentEvent)):
        return None
    if annotation is ServerSentEvent:
        return None
    variants = get_sse_variants(annotation)
    if variants is None or len(variants) != 1:
        return None
    data_types = next(iter(variants.values()))
    if len(data_types) != 1:
        return None
    return next(iter(data_types))
