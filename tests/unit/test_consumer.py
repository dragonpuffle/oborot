"""Consumer orchestration crosses only codec/application/transport interfaces."""

import asyncio
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from analytics_worker.application.ports import StreamMessage
from analytics_worker.presentation.consumer import InventoryConsumer
from common.contracts import InventoryEventV1
from common.errors import DependencyUnavailable
from common.event_codec import InventoryEventCodec
from common.values import MovementKind


async def test_invalid_then_duplicate_completes_dlq_and_ack_in_order():
    event = InventoryEventV1(uuid4(), MovementKind.RECEIPT, "A-100", 7, "WH-1")
    codec = InventoryEventCodec()
    transport = AsyncMock()
    transport.read_batch.return_value = [
        StreamMessage("1-0", {"data": "{"}),
        StreamMessage("2-0", {"data": codec.encode(event)}),
    ]
    handler = AsyncMock()
    handler.execute.return_value = False
    calls = []
    transport.dead_letter.side_effect = lambda *_: calls.append("dlq")
    handler.execute.side_effect = lambda _: calls.append("apply") or False
    transport.ack.side_effect = lambda _: calls.append("ack")
    assert await InventoryConsumer(transport, codec, handler).run_once() == 2
    assert calls == ["dlq", "apply", "ack"]
    transport.ack.assert_awaited_once_with("2-0")


@pytest.mark.parametrize("failure", [DependencyUnavailable("database"), asyncio.CancelledError()])
async def test_handler_failure_leaves_entire_batch_unacknowledged(failure):
    event = InventoryEventV1(uuid4(), MovementKind.RECEIPT, "A-100", 7, "WH-1")
    codec = InventoryEventCodec()
    transport = AsyncMock()
    transport.read_batch.return_value = [
        StreamMessage("1-0", {"data": codec.encode(event)}),
        StreamMessage("2-0", {"data": codec.encode(event)}),
    ]
    handler = AsyncMock()
    handler.execute.side_effect = failure
    with pytest.raises(type(failure)):
        await InventoryConsumer(transport, codec, handler).run_once()
    transport.ack.assert_not_awaited()
    transport.dead_letter.assert_not_awaited()
    assert handler.execute.await_count == 1
