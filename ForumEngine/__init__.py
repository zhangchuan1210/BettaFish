"""Agent event bridge package.

ForumHost and forum.log are intentionally not part of the collaboration API.
Use ``messaging.RedisStreamBus`` and ``coordination.agent_events`` for all
inter-agent communication.
"""

from .monitor import LogMonitor, get_monitor, start_forum_monitoring, stop_forum_monitoring

__all__ = [
    "LogMonitor",
    "get_monitor",
    "start_forum_monitoring",
    "stop_forum_monitoring",
]
