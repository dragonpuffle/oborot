from uuid import uuid4

import pytest

from common.event_codec import InventoryEventCodec
from common.values import Quantity, StockKey


@pytest.mark.parametrize("qty", [True, False, 0, -1, 1.1, "3", None, 1000000001])
def test_reject_invalid_quantity(qty):
    with pytest.raises(ValueError):
        Quantity(qty)


@pytest.mark.parametrize("payload", [None, [], "hi", {}, {"sku": "A", "qty": 1}])
def test_reject_invalid_shape(payload):
    with pytest.raises(ValueError):
        InventoryEventCodec().from_mapping(payload)


@pytest.mark.parametrize(
    "code", ["", " ", " A", "A ", "x" * 101, 1, None, "A\x00", "A\nB", "\ud800", "\udfff"]
)
def test_reject_invalid_identifiers(code):
    with pytest.raises(ValueError):
        StockKey(warehouse="W", sku=code)
    with pytest.raises(ValueError):
        StockKey(warehouse=code, sku="A")


def test_reject_unknown_event_type_and_bad_uuid():
    payload = dict(id=str(uuid4()), event_type="unknown", sku="A", qty=1, warehouse="W")
    with pytest.raises(ValueError):
        InventoryEventCodec().from_mapping(payload)
    with pytest.raises(ValueError):
        InventoryEventCodec().from_mapping(
            {**payload, "event_type": "inventory.issue", "id": "invalid"}
        )
