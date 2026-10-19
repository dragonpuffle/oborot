"""Wire decoding and delivery acknowledgement around the application use case."""

import asyncio
import logging

from analytics_worker.application.handlers import ApplyInventoryEvent
from analytics_worker.application.ports import StreamTransport
from common.errors import DependencyUnavailable, InvalidEvent
from common.event_codec import InventoryEventCodec

logger = logging.getLogger(__name__)


class InventoryConsumer:
    def __init__(
        self,
        transport: StreamTransport,
        codec: InventoryEventCodec,
        handler: ApplyInventoryEvent,
        *,
        poll_interval: float = 0.1,
    ):
        self._transport = transport
        self._codec = codec
        self._handler = handler
        self._poll_interval = poll_interval

    async def run_once(self) -> int:
        await self._transport.ensure_group()
        messages = await self._transport.read_batch()
        for message in messages:
            try:
                event = self._codec.decode(message.fields.get("data", ""))
            except InvalidEvent as error:
                await self._transport.dead_letter(message, str(error))
                logger.warning(
                    "Invalid event %s moved to dead-letter: %s", message.message_id, error
                )
                continue
            logger.info(
                "Event received event_id=%s message_id=%s type=%s sku=%s warehouse=%s qty=%s",
                event.id,
                message.message_id,
                event.event_type.value,
                event.sku,
                event.warehouse,
                event.qty,
            )
            stage = "processing"
            try:
                applied = await self._handler.execute(event)
                stage = "acknowledgement"
                await self._transport.ack(message.message_id)
            except Exception:
                logger.exception(
                    "Event failed stage=%s event_id=%s message_id=%s",
                    stage,
                    event.id,
                    message.message_id,
                )
                raise
            logger.info(
                "Event processed outcome=%s event_id=%s message_id=%s",
                "applied" if applied else "duplicate_skipped",
                event.id,
                message.message_id,
            )
        return len(messages)

    async def run(self) -> None:
        logger.info("Analytics consumer started; waiting for inventory events")
        while True:
            try:
                if await self.run_once():
                    continue
            except DependencyUnavailable:
                logger.exception("Inventory consumer dependency failure; retrying")
            await asyncio.sleep(self._poll_interval)
