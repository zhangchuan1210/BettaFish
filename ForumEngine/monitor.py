"""消息总线驱动的 ForumEngine 兼容入口。

本模块不再创建 ForumHost，也不读写 forum.log。它只负责从三个 Agent
的普通运行日志中提取 SummaryNode 结果，并发布结构化 observation 事件。
"""

from __future__ import annotations

import os
import re
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional

from loguru import logger

from messaging import AgentMessage, publish_message_sync


class LogMonitor:
    """Legacy log bridge; collaboration is performed through Redis Streams."""

    def __init__(self, log_dir: str = "logs"):
        self.log_dir = Path(log_dir)
        self.monitored_logs = {
            "insight": self.log_dir / "insight.log",
            "media": self.log_dir / "media.log",
            "query": self.log_dir / "query.log",
        }
        self.file_positions: Dict[str, int] = {}
        self.is_monitoring = False
        self.monitor_thread: Optional[threading.Thread] = None
        self.task_id = os.getenv("BETTAFISH_TASK_ID", "default")
        self.round_id = int(os.getenv("BETTAFISH_ROUND_ID", "0"))
        self.target_node_patterns = (
            "FirstSummaryNode",
            "ReflectionSummaryNode",
            "nodes.summary_node",
            "正在生成首次段落总结",
            "正在生成反思总结",
        )
        self.log_dir.mkdir(exist_ok=True)

    def is_target_log_line(self, line: str) -> bool:
        if "ERROR" in line or "Traceback" in line:
            return False
        return any(pattern in line for pattern in self.target_node_patterns)

    def _extract_content(self, line: str) -> Optional[str]:
        if not self.is_target_log_line(line):
            return None
        # Keep the complete structured output after the logger separator.
        content = re.sub(
            r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d+\s*\|\s*\w+\s*\|\s*[^|]+?\s*-\s*",
            "",
            line,
        )
        content = re.sub(r"^\[\d{2}:\d{2}:\d{2}\]\s*", "", content).strip()
        if len(content) < 30 or any(x in content for x in ("正在生成", "开始处理", "处理完成")):
            return None
        return content

    def _publish(self, sender: str, content: str) -> None:
        message = AgentMessage(
            task_id=self.task_id,
            round_id=self.round_id,
            sender=sender,
            message_type="observation",
            content={"text": content},
            confidence=0.5,
        )
        try:
            publish_message_sync(message, f"bettafish:{self.task_id}:events")
        except Exception as exc:
            # The bridge must not stop an Agent when Redis is temporarily down.
            logger.warning(f"消息总线发布失败，跳过本条事件: {exc}")

    def _read_new_lines(self, app_name: str) -> List[str]:
        path = self.monitored_logs[app_name]
        if not path.exists():
            return []
        position = self.file_positions.get(app_name, 0)
        size = path.stat().st_size
        if size < position:
            position = 0
        with path.open("r", encoding="utf-8", errors="ignore") as file:
            file.seek(position)
            lines = file.readlines()
            self.file_positions[app_name] = file.tell()
        return [line.strip() for line in lines if line.strip()]

    def monitor_logs(self) -> None:
        while self.is_monitoring:
            for app_name in self.monitored_logs:
                for line in self._read_new_lines(app_name):
                    content = self._extract_content(line)
                    if content:
                        self._publish(app_name, content)
            time.sleep(float(os.getenv("MESSAGE_POLL_INTERVAL", "1")))

    def start_monitoring(self) -> bool:
        if self.is_monitoring:
            return False
        self.is_monitoring = True
        self.monitor_thread = threading.Thread(target=self.monitor_logs, daemon=True)
        self.monitor_thread.start()
        logger.info("消息总线 Agent 事件桥接已启动（无 ForumHost/forum.log）")
        return True

    def stop_monitoring(self) -> None:
        self.is_monitoring = False
        if self.monitor_thread and self.monitor_thread.is_alive():
            self.monitor_thread.join(timeout=2)
        logger.info("消息总线 Agent 事件桥接已停止")

    def get_forum_log_content(self) -> List[str]:
        """Deprecated API: forum.log communication has been removed."""
        return []


_monitor_instance: Optional[LogMonitor] = None


def get_monitor() -> LogMonitor:
    global _monitor_instance
    if _monitor_instance is None:
        _monitor_instance = LogMonitor()
    return _monitor_instance


def start_forum_monitoring() -> bool:
    return get_monitor().start_monitoring()


def stop_forum_monitoring() -> None:
    get_monitor().stop_monitoring()


def get_forum_log() -> List[str]:
    return []
