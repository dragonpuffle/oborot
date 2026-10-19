"""Bounded pending recovery, fresh reads and atomic dead-letter delivery."""

import json

from redis.asyncio import Redis
from redis.exceptions import RedisError, ResponseError

from analytics_worker.application.ports import StreamMessage
from common.config import Settings
from common.errors import DependencyUnavailable

_DEAD_LETTER = """
local kind = redis.call('TYPE', KEYS[2]).ok
if kind ~= 'none' and kind ~= 'stream' then
    return redis.error_reply('WRONGTYPE dead-letter destination is not a stream')
end
redis.call('XADD', KEYS[2], '*', 'source_stream', KEYS[1], 'source_id', ARGV[2],
           'data', ARGV[3], 'error', ARGV[4])
return redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
"""


class RedisStreamTransport:
    def __init__(self, redis: Redis, settings: Settings):
        self._redis = redis
        self._settings = settings
        self.claim_cursor = "0-0"

    async def ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                self._settings.stream, self._settings.group, id="0-0", mkstream=True
            )
        except ResponseError as error:
            if not str(error).startswith("BUSYGROUP"):
                raise DependencyUnavailable(str(error)) from error
        except (RedisError, OSError) as error:
            raise DependencyUnavailable(str(error)) from error

    async def read_batch(self) -> list[StreamMessage]:
        settings = self._settings
        try:
            claimed = await self._redis.xautoclaim(
                settings.stream,
                settings.group,
                settings.consumer,
                min_idle_time=settings.claim_idle_ms,
                start_id=self.claim_cursor,
                count=settings.batch_size,
            )
            self.claim_cursor = claimed[0]
            entries = list(claimed[1])
            streams = await self._redis.xreadgroup(
                settings.group,
                settings.consumer,
                {settings.stream: ">"},
                count=settings.batch_size,
                block=settings.block_ms if settings.block_ms > 0 and not entries else None,
            )
            for _, fresh in streams:
                entries.extend(fresh)
            return [StreamMessage(message_id, fields) for message_id, fields in entries]
        except (RedisError, OSError) as error:
            raise DependencyUnavailable(str(error)) from error

    async def ack(self, message_id: str) -> None:
        try:
            await self._redis.xack(self._settings.stream, self._settings.group, message_id)
        except (RedisError, OSError) as error:
            raise DependencyUnavailable(str(error)) from error

    async def dead_letter(self, message: StreamMessage, reason: str) -> None:
        settings = self._settings
        try:
            await self._redis.eval(
                _DEAD_LETTER,
                2,
                settings.stream,
                settings.dead_letter_stream,
                settings.group,
                message.message_id,
                json.dumps(dict(message.fields)),
                reason,
            )
        except (RedisError, OSError) as error:
            raise DependencyUnavailable(str(error)) from error
