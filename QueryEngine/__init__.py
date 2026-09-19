"""Query Agent public API."""

from .agent import DeepSearchAgent as _DeepSearchAgent, create_agent as _create_agent
from .utils.config import Settings
from coordination.message_driven import enable_message_driven


class DeepSearchAgent(_DeepSearchAgent):
    """Query Agent with a Redis Streams inbox."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        enable_message_driven(self, "query")


def create_agent(*args, **kwargs):
    return DeepSearchAgent(*args, **kwargs)

__all__ = ["DeepSearchAgent", "create_agent", "Settings"]
