"""Compatibility reader backed by Redis Streams when enabled.

Existing SummaryNodes can continue calling get_latest_host_speech(). During the
migration, consensus/host_summary messages are preferred and forum.log remains
a safe fallback when Redis is unavailable.
"""

import asyncio
import os
import re
from pathlib import Path
from typing import Optional

from loguru import logger


def _latest_bus_summary(task_id: Optional[str] = None) -> Optional[str]:
    if os.getenv("MESSAGE_BUS_ENABLED", "0").lower() not in {"1", "true", "yes"}:
        return None
    try:
        from messaging import RedisStreamBus

        async def read():
            message = await RedisStreamBus().latest(
                f"bettafish:{task_id or os.getenv('BETTAFISH_TASK_ID', 'default')}:events"
            )
            if message and message.message_type in {"consensus", "host_summary"}:
                return message.content.get("text") or message.content.get("summary")
            return None

        return asyncio.run(read())
    except Exception as exc:
        logger.debug(f"消息总线不可用，回退到forum.log: {exc}")
        return None


def get_latest_host_speech(log_dir: str = "logs") -> Optional[str]:
    bus_summary = _latest_bus_summary()
    if bus_summary:
        return bus_summary
    path = Path(log_dir) / "forum.log"
    try:
        if not path.exists():
            return None
        with open(path, "r", encoding="utf-8", errors="ignore") as file:
            for line in reversed(file.readlines()):
                match = re.match(r"\[(\d{2}:\d{2}:\d{2})\]\s*\[HOST\]\s*(.+)", line)
                if match:
                    return match.group(2).replace("\\n", "\n").strip()
    except Exception as exc:
        logger.error(f"读取协作上下文失败: {exc}")
    return None


def format_host_speech_for_prompt(host_speech: str) -> str:
    if not host_speech:
        return ""
    return f"""
### 最新协作上下文
以下内容来自消息总线中的共识/主持人摘要，请作为参考，不要盲目接受：

{host_speech}

---
"""
