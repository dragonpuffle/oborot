import asyncio
import logging

from common.errors import DependencyUnavailable
from inventory_api.application.outbox import PublishOutboxBatch

logger = logging.getLogger(__name__)


class OutboxRunner:
    def __init__(self, publish_batch: PublishOutboxBatch, poll_interval: float):
        self._publish_batch = publish_batch
        self._poll_interval = poll_interval

    async def run(self) -> None:
        while True:
            try:
                if await self._publish_batch.execute():
                    continue
            except DependencyUnavailable:
                logger.exception("Outbox dependency failure; retrying unpublished events")
            await asyncio.sleep(self._poll_interval)
