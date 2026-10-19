import asyncio

from redis.asyncio import Redis
from redis.exceptions import RedisError

from common.contracts import InventoryEventV1
from common.errors import DependencyUnavailable
from common.event_codec import InventoryEventCodec


class RedisEventPublisher:
    def __init__(
        self, redis: Redis, stream: str, codec: InventoryEventCodec, *, timeout: float = 5
    ):
        self._redis = redis
        self._stream = stream
        self._codec = codec
        self._timeout = timeout

    async def publish(self, event: InventoryEventV1) -> None:
        try:
            await asyncio.wait_for(
                self._redis.xadd(self._stream, {"data": self._codec.encode(event)}),
                timeout=self._timeout,
            )
        except (RedisError, OSError, asyncio.TimeoutError) as error:
            raise DependencyUnavailable("Event stream unavailable") from error
