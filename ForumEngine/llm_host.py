"""ForumHost has been removed.

Agent collaboration is now performed through structured Redis Streams messages.
Use ``messaging.AgentMessage`` and ``RedisStreamBus`` instead.
"""

raise ImportError(
    "ForumHost/forum.log communication was removed; use the messaging package"
)
