"""Message-driven runtime shared by Query, Media, and Insight agents."""

from __future__ import annotations

import asyncio
import os
import threading
import uuid
from typing import Any, Dict, Optional

from loguru import logger

from messaging import AgentMessage, RedisStreamBus


_ROLE_CAPABILITIES = {
    "query": {"web", "news", "search", "verification"},
    "media": {"multimodal", "image", "video", "media"},
    "insight": {"database", "history", "sentiment", "trend"},
}


class MessageDrivenRuntime:
    """Runs an agent's inbox consumer independently of the research workflow."""

    def __init__(self, agent: Any, role: str, task_id: Optional[str] = None):
        self.agent = agent
        self.role = role.lower()
        self.task_id = task_id or os.getenv("BETTAFISH_TASK_ID", "default")
        self.topic = f"bettafish:{self.task_id}:events"
        self.group = f"agent-{self.role}"
        self.consumer = f"{self.role}-{uuid.uuid4().hex[:8]}"
        self.inbox: list[AgentMessage] = []
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name=f"{self.role}-message-agent")
        self._thread.start()
        logger.info(f"{self.role} Agent 消息驱动运行时已启动")

    def stop(self) -> None:
        self._stop.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                asyncio.run(self._consume_once())
            except Exception as exc:
                logger.warning(f"{self.role} Agent 消息订阅暂不可用: {exc}")
                self._stop.wait(2)

    async def _consume_once(self) -> None:
        bus = RedisStreamBus()
        async for stream_id, message in bus.consume(self.topic, self.group, self.consumer, block_ms=1000):
            if self._stop.is_set():
                break
            if message.sender == self.role:
                await bus.ack(self.topic, self.group, stream_id)
                continue
            try:
                self.handle_message(message)
                await bus.ack(self.topic, self.group, stream_id)
            except Exception:
                logger.exception(f"{self.role} Agent 处理消息失败: {message.message_id}")

    def handle_message(self, message: AgentMessage) -> None:
        self.inbox.append(message)
        if len(self.inbox) > 100:
            del self.inbox[:-100]

        if message.message_type == "task_offer" and self._can_claim(message):
            self._publish_claim(message)
        elif message.message_type == "challenge":
            logger.info(f"{self.role} Agent 收到质疑: {message.content.get('reason', '')}")
        elif message.message_type in {"observation", "task_result", "consensus"}:
            logger.info(f"{self.role} Agent 收到 {message.message_type} 消息，来自 {message.sender}")

        callback = getattr(self.agent, "on_agent_message", None)
        if callable(callback):
            callback(message)

    def _can_claim(self, message: AgentMessage) -> bool:
        required = message.content.get("required_capabilities", [])
        if not required:
            return False
        return bool(set(map(str.lower, required)) & _ROLE_CAPABILITIES.get(self.role, set()))

    def _publish_claim(self, message: AgentMessage) -> None:
        try:
            from coordination.agent_events import publish_agent_event
            publish_agent_event(
                self.role,
                "task_claim",
                {"task_offer_id": message.message_id, "role": self.role},
                task_id=self.task_id,
                round_id=message.round_id,
                reply_to=message.message_id,
            )
        except Exception as exc:
            logger.warning(f"{self.role} Agent 发布任务认领失败: {exc}")


def enable_message_driven(agent: Any, role: str) -> MessageDrivenRuntime:
    runtime = MessageDrivenRuntime(agent, role)
    agent.message_runtime = runtime
    runtime.start()
    try:
        from coordination.agent_events import publish_agent_event
        publish_agent_event(role.lower(), "agent_ready", {"capabilities": sorted(_ROLE_CAPABILITIES.get(role.lower(), set()))})
    except Exception as exc:
        logger.debug(f"发布 {role} ready 事件失败: {exc}")
    return runtime
