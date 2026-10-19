"""Application behavior through transaction ports, without backend libraries."""

from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from common.values import MovementKind, Quantity, StockKey
from inventory_api.application.dto import MovementCommand, StockItem, StockSummary
from inventory_api.application.services import InventoryService
from inventory_api.domain.errors import InsufficientStock


class MemoryInventory:
    def __init__(self):
        self.state = {"balances": {}, "events": [], "outbox": []}
        self.transactions = []
        self.fail_outbox = False

    def transaction(self):
        transaction = MemoryInventoryTransaction(self)
        self.transactions.append(transaction)
        return transaction


class MemoryInventoryTransaction:
    def __init__(self, database):
        self.database = database
        self.balances = self
        self.events = AsyncMock(append=self.append_event)
        self.outbox = AsyncMock(append=self.append_outbox)
        self.committed = False
        self.closed = False

    async def __aenter__(self):
        self.state = deepcopy(self.database.state)
        return self

    async def increase(self, key, quantity):
        self.state["balances"][key] = self.state["balances"].get(key, 0) + quantity.value

    async def try_decrease(self, key, quantity):
        if self.state["balances"].get(key, 0) < quantity.value:
            return False
        self.state["balances"][key] -= quantity.value
        return True

    async def append_event(self, movement):
        self.state["events"].append(movement)

    async def append_outbox(self, event):
        if self.database.fail_outbox:
            raise RuntimeError("injected outbox failure")
        self.state["outbox"].append(event)

    async def commit(self):
        self.database.state = self.state
        self.committed = True

    async def __aexit__(self, *args):
        self.closed = True


async def test_receipt_commits_matching_balance_journal_and_outbox_through_ports():
    database = MemoryInventory()
    service = InventoryService(database.transaction, AsyncMock())
    command = MovementCommand(StockKey("WH", "A"), Quantity(7))
    event = await service.receive(command)
    assert database.state["balances"] == {StockKey("WH", "A"): 7}
    (movement,) = database.state["events"]
    assert movement.id == event.id
    assert movement.key == command.key
    assert movement.delta == 7
    assert movement.kind is MovementKind.RECEIPT
    assert database.state["outbox"] == [event]
    assert (event.warehouse, event.sku, event.qty, event.event_type) == (
        "WH",
        "A",
        7,
        MovementKind.RECEIPT,
    )
    assert database.transactions[0].committed
    assert database.transactions[0].closed


async def test_issue_uses_fresh_transaction_and_rejection_has_no_side_effects():
    database = MemoryInventory()
    service = InventoryService(database.transaction, AsyncMock())
    key = StockKey("WH", "A")
    await service.receive(MovementCommand(key, Quantity(7)))
    event = await service.issue(MovementCommand(key, Quantity(5)))
    assert database.state["balances"] == {key: 2}
    assert database.state["events"][-1].delta == -5
    assert event.event_type is MovementKind.ISSUE
    before = deepcopy(database.state)
    with pytest.raises(InsufficientStock):
        await service.issue(MovementCommand(key, Quantity(3)))
    assert database.state == before
    assert len(database.transactions) == 3
    assert all(transaction.closed for transaction in database.transactions)
    assert not database.transactions[-1].committed


@pytest.mark.parametrize("operation", ["receive", "issue"])
async def test_outbox_failure_rolls_back_balance_and_journal_through_ports(operation):
    database = MemoryInventory()
    service = InventoryService(database.transaction, AsyncMock())
    command = MovementCommand(StockKey("WH", "A"), Quantity(2))
    await service.receive(command)
    before = deepcopy(database.state)
    database.fail_outbox = True
    with pytest.raises(RuntimeError, match="injected outbox failure"):
        await getattr(service, operation)(command)
    assert database.state == before
    assert not database.transactions[-1].committed
    assert database.transactions[-1].closed


async def test_queries_return_dtos_without_opening_write_transaction():
    queries = AsyncMock()
    queries.stock.return_value = [StockItem("WH", "A", 0)]
    queries.summary.return_value = StockSummary((), ())
    database = MemoryInventory()
    service = InventoryService(database.transaction, queries)
    assert await service.stock("WH", "A") == [StockItem("WH", "A", 0)]
    assert await service.summary(5) == StockSummary((), ())
    queries.stock.assert_awaited_once_with("WH", "A")
    queries.summary.assert_awaited_once_with(5)
    assert database.transactions == []
