import logging
from dataclasses import asdict

from aiohttp import web

from common.errors import DependencyUnavailable
from common.values import Quantity, StockKey, validate_code
from inventory_api.application.dto import MovementCommand
from inventory_api.application.ports import HealthCheck
from inventory_api.application.services import InventoryService
from inventory_api.domain.errors import InsufficientStock

logger = logging.getLogger(__name__)


@web.middleware
async def error_responses(request, handler):
    try:
        return await handler(request)
    except web.HTTPException as error:
        headers = {
            key: value
            for key, value in error.headers.items()
            if key.lower() not in {"content-type", "content-length"}
        }
        return web.json_response({"error": error.reason}, status=error.status, headers=headers)
    except DependencyUnavailable:
        logger.exception("Inventory database request failed")
        return web.json_response({"error": "Database unavailable"}, status=503)


def query_fields(request, allowed):
    if set(request.query) - set(allowed):
        raise web.HTTPBadRequest(reason="Unexpected query parameter")
    if any(len(request.query.getall(key)) != 1 for key in request.query):
        raise web.HTTPBadRequest(reason="Repeated query parameter")
    return request.query


def query_name(value, field):
    try:
        return validate_code(value, field)
    except ValueError as error:
        raise web.HTTPBadRequest(reason=str(error)) from error


class InventoryController:
    def __init__(self, service: InventoryService, health_check: HealthCheck):
        self._service = service
        self._health_check = health_check

    async def operation(self, request):
        try:
            if request.content_type != "application/json":
                raise ValueError("Expected application/json")
            payload = await request.json()
            if not isinstance(payload, dict):
                raise ValueError("body must be a JSON object")
            sku = validate_code(payload.get("sku"), "sku")
            key = StockKey(payload.get("warehouse"), sku)
            quantity = Quantity(payload.get("qty"))
            if set(payload) - {"sku", "qty", "warehouse"}:
                raise ValueError("Unexpected JSON field")
        except (ValueError, LookupError, RecursionError) as error:
            raise web.HTTPBadRequest(reason=str(error)) from error
        command = MovementCommand(key, quantity)
        try:
            if request.path == "/receipts":
                event = await self._service.receive(command)
            else:
                event = await self._service.issue(command)
        except InsufficientStock as error:
            raise web.HTTPConflict(reason="Insufficient stock") from error
        return web.json_response(
            {
                "id": str(event.id),
                "event_type": event.event_type.value,
                "sku": event.sku,
                "qty": event.qty,
                "warehouse": event.warehouse,
            },
            status=201,
        )

    async def stock(self, request):
        query = query_fields(request, {"warehouse", "sku"})
        warehouse = query_name(query.get("warehouse"), "warehouse")
        sku = query_name(query["sku"], "sku") if "sku" in query else None
        items = await self._service.stock(warehouse, sku)
        return web.json_response({"items": [asdict(item) for item in items]})

    async def summary(self, request):
        query = query_fields(request, {"top_n"})
        raw_top_n = query.get("top_n", "5")
        if (
            not raw_top_n.isascii()
            or not raw_top_n.isdecimal()
            or len(raw_top_n) > 3
            or not 1 <= int(raw_top_n) <= 100
        ):
            raise web.HTTPBadRequest(reason="top_n must be an integer between 1 and 100")
        return web.json_response(asdict(await self._service.summary(int(raw_top_n))))

    async def health(self, request):
        await self._health_check.check()
        return web.json_response({"status": "ok"})

    def register(self, app):
        app.router.add_post("/receipts", self.operation)
        app.router.add_post("/issues", self.operation)
        app.router.add_get("/stock", self.stock)
        app.router.add_get("/stock/summary", self.summary)
        app.router.add_get("/health", self.health)
