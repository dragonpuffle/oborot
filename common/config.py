"""Explicit environment configuration shared by both services."""

import math
import os
import socket
from dataclasses import dataclass
from uuid import uuid4


@dataclass(frozen=True)
class Settings:
    database_url: str = "postgresql+asyncpg://oborot:oborot@localhost:55432/oborot"
    redis_url: str = "redis://localhost:56379/0"
    stream: str = "inventory.events"
    group: str = "inventory.analytics"
    consumer: str = "worker"
    dead_letter_stream: str = "inventory.dead_letters"
    claim_idle_ms: int = 30000
    block_ms: int = 1000
    batch_size: int = 100
    poll_interval: float = 0.25

    def __post_init__(self):
        if self.stream == self.dead_letter_stream:
            raise ValueError("stream and dead_letter_stream must be distinct")
        for field in (
            "database_url",
            "redis_url",
            "stream",
            "group",
            "consumer",
            "dead_letter_stream",
        ):
            if not getattr(self, field):
                raise ValueError(f"{field} must not be empty")
        if self.claim_idle_ms < 0 or self.block_ms <= 0 or not 1 <= self.batch_size <= 1000:
            raise ValueError("claim_idle_ms >= 0, block_ms > 0 and batch_size 1..1000 are required")
        if not math.isfinite(self.poll_interval) or self.poll_interval <= 0:
            raise ValueError("poll_interval must be finite and positive")

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=os.getenv("DATABASE_URL", cls.database_url),
            redis_url=os.getenv("REDIS_URL", cls.redis_url),
            stream=os.getenv("REDIS_STREAM", cls.stream),
            group=os.getenv("REDIS_GROUP", cls.group),
            consumer=os.getenv("REDIS_CONSUMER", f"{socket.gethostname()}-{uuid4().hex[:8]}"),
            dead_letter_stream=os.getenv("REDIS_DLQ", cls.dead_letter_stream),
            claim_idle_ms=int(os.getenv("CLAIM_IDLE_MS", "30000")),
            block_ms=int(os.getenv("BLOCK_MS", "1000")),
            batch_size=int(os.getenv("BATCH_SIZE", "100")),
            poll_interval=float(os.getenv("POLL_INTERVAL", "0.25")),
        )
