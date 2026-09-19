"""Agent-side decentralized coordination helpers."""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from messaging import AgentMessage, publish_message_sync


def publish_agent_event(
    sender: str,
    message_type: str,
    content: Dict[str, Any],
    *,
    task_id: Optional[str] = None,
    round_id: int = 0,
    confidence: float = 0.5,
    reply_to: Optional[str] = None,
) -> str:
    task = task_id or os.getenv("BETTAFISH_TASK_ID", "default")
    message = AgentMessage(
        task_id=task,
        round_id=round_id,
        sender=sender,
        message_type=message_type,
        content=content,
        confidence=confidence,
        reply_to=reply_to,
    )
    return publish_message_sync(message, f"bettafish:{task}:events")


def publish_observation(sender: str, text: str, **kwargs: Any) -> str:
    return publish_agent_event(
        sender, "observation", {"text": text}, **kwargs
    )


def publish_challenge(sender: str, target_message_id: str, reason: str, **kwargs: Any) -> str:
    return publish_agent_event(
        sender,
        "challenge",
        {"reason": reason, "target_message_id": target_message_id},
        reply_to=target_message_id,
        **kwargs,
    )


def publish_task_offer(sender: str, title: str, **kwargs: Any) -> str:
    return publish_agent_event(sender, "task_offer", {"title": title}, **kwargs)
