"""Opt-in bridge from legacy Agent logs to the structured message bus.

Run with: python -m ForumEngine.decentralized_monitor
The legacy forum.log is retained for UI compatibility, while structured events are
published to Redis Streams for independent Agents to consume.
"""

import asyncio
import os
import time
from typing import Optional

from loguru import logger

from .monitor import LogMonitor
from messaging import AgentMessage, publish_message_sync


class DecentralizedLogMonitor(LogMonitor):
    """Legacy log parser that emits events without invoking a central host."""

    def __init__(self, log_dir: str = "logs", task_id: Optional[str] = None):
        super().__init__(log_dir)
        self.task_id = task_id or os.getenv("BETTAFISH_TASK_ID", "default")
        self.round_id = int(os.getenv("BETTAFISH_ROUND_ID", "0"))

    def _trigger_host_speech(self):
        """Disable the old fixed five-message central-host trigger."""
        return None

    def publish_observation(self, sender: str, content: str) -> None:
        message = AgentMessage(
            task_id=self.task_id,
            round_id=self.round_id,
            sender=sender.lower(),
            message_type="observation",
            content={"text": content},
            confidence=0.5,
        )
        try:
            publish_message_sync(
                message, f"bettafish:{self.task_id}:events"
            )
        except Exception:
            logger.exception("发布去中心化协作消息失败")

    def run_once(self) -> None:
        """Read new legacy output and publish it without central coordination."""
        for app_name, log_file in self.monitored_logs.items():
            if app_name not in self.file_positions:
                self.file_positions[app_name] = self.get_file_size(log_file)
                self.file_line_counts[app_name] = self.get_file_line_count(log_file)
                self.capturing_json[app_name] = False
                self.json_buffer[app_name] = []
                self.in_error_block[app_name] = False
            new_lines = self.read_new_lines(log_file, app_name)
            for content in self.process_lines_for_json(new_lines, app_name):
                source = app_name.upper()
                self.write_to_forum_log(content, source)
                self.publish_observation(source, content)

    def run_forever(self) -> None:
        self.is_monitoring = True
        while self.is_monitoring:
            self.run_once()
            time.sleep(float(os.getenv("MESSAGE_POLL_INTERVAL", "1")))


def main() -> None:
    DecentralizedLogMonitor().run_forever()


if __name__ == "__main__":
    main()
