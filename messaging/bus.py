"""Message-bus abstractions and a Redis Streams implementation."""

from __future__ import annotations

import asyncio
import json
import os
from abc import ABC, abstractmethod
from typing import AsyncIterator, Optional

from .schemas import AgentMessage


class MessageBus(ABC):
    """Transport-only interface; coordination decisions stay in Agents."""

    @abstractmethod
    async def publish(self, topic: str, message: AgentMessage) -> str:
        raise NotImplementedError

    @abstractmethod
    async def consume(
        self, topic: str, group: str, consumer: str, *, block_ms: int = 1000
    ) -> AsyncIterator[tuple[str, AgentMessage]]:
        raise NotImplementedError

    @abstractmethod
    async def ack(self, topic: str, group: str, message_id: str) -> None:
        raise NotImplementedError


class RedisStreamBus(MessageBus):
    """Redis Streams transport with consumer groups and explicit ACKs."""

    def __init__(self, url: Optional[str] = None):
        try:
            import redis.asyncio as redis
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError("Install redis>=4.6 to use the message bus") from exc
        self._redis = redis.from_url(
            url or os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True,
        )

    async def publish(self, topic: str, message: AgentMessage) -> str:
        return await self._redis.xadd(
            topic,
            message.to_bus_fields(),
            maxlen=int(os.getenv("MESSAGE_MAXLEN", "10000")),
            approximate=True,
        )

    async def ensure_group(self, topic: str, group: str) -> None:
        try:
            await self._redis.xgroup_create(topic, group, id="0", mkstream=True)
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def consume(
        self, topic: str, group: str, consumer: str, *, block_ms: int = 1000
    ) -> AsyncIterator[tuple[str, AgentMessage]]:
        await self.ensure_group(topic, group)
        while True:
            rows = await self._redis.xreadgroup(
                group, consumer, {topic: ">"}, count=10, block=block_ms
            )
            for _, entries in rows:
                for stream_id, fields in entries:
                    payload = fields.get("payload")
                    if payload:
                        yield stream_id, AgentMessage.from_payload(payload)

    async def ack(self, topic: str, group: str, message_id: str) -> None:
        await self._redis.xack(topic, group, message_id)

    async def latest(self, topic: str) -> Optional[AgentMessage]:
        rows = await self._redis.xrevrange(topic, count=1)
        if not rows:
            return None
        _, fields = rows[0]
        payload = fields.get("payload")
        return AgentMessage.from_payload(payload) if payload else None


async def publish_message(message: AgentMessage, topic: Optional[str] = None) -> str:
    """Convenience API used by adapters and migration code."""
    bus = RedisStreamBus()
    stream = topic or f"bettafish:{message.task_id}:events"
    return await bus.publish(stream, message)


def publish_message_sync(message: AgentMessage, topic: Optional[str] = None) -> str:
    return asyncio.run(publish_message(message, topic))
