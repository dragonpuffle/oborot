"""Application behavior exercised without database or stream dependencies."""

from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from analytics_worker.application.handlers import ApplyInventoryEvent
from common.contracts import InventoryEventV1
from common.values import MovementKind, StockKey


async def test_new_event_marks_and_applies_delta_before_commit():
    uow = AsyncMock()
    uow.__aenter__.return_value = uow
    uow.processed.mark_if_new.return_value = True
    calls = []
    uow.processed.mark_if_new.side_effect = lambda _: calls.append("mark") or True
    uow.projection.add_delta.side_effect = lambda *_: calls.append("delta")
    uow.commit.side_effect = lambda: calls.append("commit")
    event = InventoryEventV1(uuid4(), MovementKind.ISSUE, "A-100", 5, "WH-1")

    assert await ApplyInventoryEvent(lambda: uow).execute(event) is True
    assert calls == ["mark", "delta", "commit"]
    uow.projection.add_delta.assert_awaited_once_with(StockKey("WH-1", "A-100"), -5)


async def test_duplicate_commits_without_changing_projection():
    uow = AsyncMock()
    uow.__aenter__.return_value = uow
    uow.processed.mark_if_new.return_value = False
    event = InventoryEventV1(uuid4(), MovementKind.RECEIPT, "A-100", 7, "WH-1")
    assert await ApplyInventoryEvent(lambda: uow).execute(event) is False
    uow.projection.add_delta.assert_not_awaited()
    uow.commit.assert_awaited_once()


async def test_projection_error_exits_without_committing_marker():
    uow = AsyncMock()
    uow.__aenter__.return_value = uow
    uow.__aexit__.return_value = False
    failure = RuntimeError("projection failed")
    uow.projection.add_delta.side_effect = failure
    event = InventoryEventV1(uuid4(), MovementKind.RECEIPT, "A-100", 7, "WH-1")
    with pytest.raises(RuntimeError, match="projection failed"):
        await ApplyInventoryEvent(lambda: uow).execute(event)
    uow.commit.assert_not_awaited()
    assert uow.__aexit__.await_args.args[1] is failure
