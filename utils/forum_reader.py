"""消息总线读取工具，不再读取 forum.log。"""

from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, List, Optional

from messaging import RedisStreamBus


def _read_messages(task_id: Optional[str] = None) -> List[Any]:
    task = task_id or os.getenv("BETTAFISH_TASK_ID", "default")
    topic = f"bettafish:{task}:events"

    async def read() -> List[Any]:
        bus = RedisStreamBus()
        rows = await bus._redis.xrevrange(topic, count=20)
        messages = []
        for _, fields in reversed(rows):
            payload = fields.get("payload")
            if payload:
                messages.append(__import__("messaging").AgentMessage.from_payload(payload))
        return messages

    try:
        return asyncio.run(read())
    except Exception:
        return []


def get_recent_agent_messages(task_id: Optional[str] = None, limit: int = 5) -> List[Dict[str, Any]]:
    messages = _read_messages(task_id)
    return [
        {
            "message_id": message.message_id,
            "sender": message.sender,
            "message_type": message.message_type,
            "content": message.content,
            "confidence": message.confidence,
            "round_id": message.round_id,
        }
        for message in messages[-limit:]
    ]


def get_latest_consensus(task_id: Optional[str] = None) -> Optional[str]:
    for message in reversed(_read_messages(task_id)):
        if message.message_type in {"consensus", "host_summary"}:
            return message.content.get("text") or message.content.get("summary")
    return None


def get_latest_host_speech(*args: Any, **kwargs: Any) -> Optional[str]:
    """Compatibility name; returns consensus messages, never forum.log content."""
    return get_latest_consensus()


def format_host_speech_for_prompt(summary: str) -> str:
    if not summary:
        return ""
    return f"\n### 最新协作共识（来自消息总线）\n{summary}\n---\n"
