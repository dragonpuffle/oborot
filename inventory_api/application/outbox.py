from collections.abc import Callable

from inventory_api.application.ports import EventPublisher, OutboxUnitOfWork


class PublishOutboxBatch:
    def __init__(
        self,
        outbox_uow_factory: Callable[[], OutboxUnitOfWork],
        publisher: EventPublisher,
        batch_size: int,
    ):
        self._uow_factory = outbox_uow_factory
        self._publisher = publisher
        self._batch_size = batch_size

    async def execute(self) -> int:
        async with self._uow_factory() as uow:
            messages = await uow.outbox.lock_pending(self._batch_size)
            for message in messages:
                await self._publisher.publish(message.event)
                await uow.outbox.mark_published(message.event_id)
            await uow.commit()
        return len(messages)
