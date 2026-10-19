"""Outbox use case and runner contract through dependency ports."""

import asyncio
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from common.contracts import InventoryEventV1
from common.errors import DependencyUnavailable
from common.values import MovementKind
from inventory_api.application.dto import OutboxMessage
from inventory_api.application.outbox import PublishOutboxBatch
from inventory_api.presentation.outbox_runner import OutboxRunner


class MemoryOutboxTransaction:
    def __init__(self, pending):
        self.pending = pending
        self.published = set()
        self.outbox = self
        self.committed = False
        self.closed = False

    async def __aenter__(self):
        self.staged = set()
        return self

    async def lock_pending(self, limit):
        return self.pending[:limit]

    async def mark_published(self, event_id):
        self.staged.add(event_id)

    async def commit(self):
        self.published.update(self.staged)
        self.committed = True

    async def __aexit__(self, *args):
        self.closed = True


def message():
    event = InventoryEventV1(uuid4(), MovementKind.RECEIPT, "A", 7, "WH")
    return OutboxMessage(event.id, event)


async def test_batch_commits_published_messages_and_obeys_limit():
    pending = [message(), message()]
    transaction = MemoryOutboxTransaction(pending)
    publisher = AsyncMock()
    scenario = PublishOutboxBatch(lambda: transaction, publisher, batch_size=1)
    assert await scenario.execute() == 1
    assert transaction.published == {pending[0].event_id}
    publisher.publish.assert_awaited_once_with(pending[0].event)
    assert transaction.committed and transaction.closed


async def test_partial_publish_failure_rolls_back_all_sql_marks():
    pending = [message(), message()]
    transaction = MemoryOutboxTransaction(pending)
    publisher = AsyncMock()
    publisher.publish.side_effect = [None, RuntimeError("offline")]
    scenario = PublishOutboxBatch(lambda: transaction, publisher, batch_size=2)
    with pytest.raises(RuntimeError, match="offline"):
        await scenario.execute()
    assert publisher.publish.await_count == 2
    assert transaction.published == set()
    assert not transaction.committed
    assert transaction.closed


async def test_publisher_retries_dependencies_and_propagates_cancellation(settings):
    publish = AsyncMock()
    publish.execute.side_effect = [DependencyUnavailable("offline"), 1, asyncio.CancelledError()]
    with pytest.raises(asyncio.CancelledError):
        await OutboxRunner(publish, settings.poll_interval).run()
    assert publish.execute.await_count == 3


async def test_batch_cancellation_rolls_back_marks_and_releases_transaction():
    pending = [message(), message()]
    transaction = MemoryOutboxTransaction(pending)
    publisher = AsyncMock()
    publisher.publish.side_effect = [None, asyncio.CancelledError()]
    scenario = PublishOutboxBatch(lambda: transaction, publisher, batch_size=2)
    with pytest.raises(asyncio.CancelledError):
        await scenario.execute()
    assert transaction.published == set()
    assert not transaction.committed
    assert transaction.closed
