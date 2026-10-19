from uuid import uuid4

import pytest

from common.contracts import InventoryEventV1
from common.errors import InvalidEvent
from common.event_codec import InventoryEventCodec
from common.values import MovementKind


@pytest.mark.parametrize(
    "changes",
    [
        {"qty": True},
        {"qty": 0},
        {"qty": 1000000001},
        {"sku": " A"},
        {"warehouse": "\ud800"},
        {"id": "invalid"},
        {"event_type": "inventory.receipt"},
    ],
)
def test_direct_event_constructor_rejects_invalid_values(changes):
    values = dict(id=uuid4(), event_type=MovementKind.RECEIPT, sku="A", qty=7, warehouse="W")
    with pytest.raises(InvalidEvent):
        InventoryEventV1(**{**values, **changes})


@pytest.mark.parametrize(
    "wire_type,kind",
    [("inventory.receipt", MovementKind.RECEIPT), ("inventory.issue", MovementKind.ISSUE)],
)
def test_codec_preserves_wire_and_ignores_extra_fields(wire_type, kind):
    codec = InventoryEventCodec()
    payload = dict(id=str(uuid4()), event_type=wire_type, sku="А", qty=7, warehouse="W")
    event = codec.from_mapping({**payload, "future": 1})
    assert event.event_type is kind
    assert codec.to_mapping(event) == payload
    assert codec.decode(codec.encode(event)) == event


@pytest.mark.parametrize("data", ["{", "[]", "null", "[" * 10000 + "0" + "]" * 10000])
def test_codec_rejects_bad_or_deep_json(data):
    with pytest.raises(InvalidEvent):
        InventoryEventCodec().decode(data)
