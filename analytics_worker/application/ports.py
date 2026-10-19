"""Small transactional and stream interfaces owned by analytics."""

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from types import TracebackType
from typing import Protocol
from uuid import UUID

from common.values import StockKey


class ProcessedEventsRepository(Protocol):
    async def mark_if_new(self, event_id: UUID) -> bool: ...


class ProjectionRepository(Protocol):
    async def add_delta(self, key: StockKey, delta: int) -> None: ...


class ProjectionUnitOfWork(Protocol):
    processed: ProcessedEventsRepository
    projection: ProjectionRepository

    async def __aenter__(self) -> "ProjectionUnitOfWork": ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...
    async def commit(self) -> None: ...


ProjectionUowFactory = Callable[[], ProjectionUnitOfWork]


@dataclass(frozen=True)
class StreamMessage:
    message_id: str
    fields: Mapping[str, str]


class StreamTransport(Protocol):
    async def ensure_group(self) -> None: ...
    async def read_batch(self) -> Sequence[StreamMessage]: ...
    async def ack(self, message_id: str) -> None: ...
    async def dead_letter(self, message: StreamMessage, reason: str) -> None: ...
