"""Removed legacy ForumHost implementation.

This module is retained only as a migration marker so stale imports fail with a
clear message. It must not be used for runtime collaboration.
"""

raise RuntimeError(
    "ForumHost was removed. Use messaging.RedisStreamBus and "
    "coordination.agent_events instead."
)
