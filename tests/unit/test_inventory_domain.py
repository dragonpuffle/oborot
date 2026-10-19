from dataclasses import FrozenInstanceError
from uuid import uuid4

import pytest

from common.errors import ValidationError
from common.values import MovementKind, Quantity, StockKey
from inventory_api.domain.models import StockMovement


@pytest.mark.parametrize("kind,delta", [(MovementKind.RECEIPT, 7), (MovementKind.ISSUE, -7)])
def test_validated_movement_has_signed_delta_and_is_immutable(kind, delta):
    movement = StockMovement(uuid4(), StockKey("WH", "A"), Quantity(7), kind)
    assert movement.delta == delta
    with pytest.raises(FrozenInstanceError):
        movement.kind = MovementKind.ISSUE


@pytest.mark.parametrize(
    "changes",
    [
        {"id": "not-a-uuid"},
        {"key": ("WH", "A")},
        {"quantity": 7},
        {"kind": "inventory.receipt"},
    ],
)
def test_movement_rejects_unvalidated_constructor_arguments(changes):
    values = dict(
        id=uuid4(), key=StockKey("WH", "A"), quantity=Quantity(7), kind=MovementKind.RECEIPT
    )
    with pytest.raises(ValidationError):
        StockMovement(**(values | changes))
