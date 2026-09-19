"""Insight Agent public API."""

from .agent import DeepSearchAgent as _DeepSearchAgent
from .agent import create_agent as _create_agent
from .utils.config import Settings, settings
from coordination.message_driven import enable_message_driven


class DeepSearchAgent(_DeepSearchAgent):
    """Insight Agent with a Redis Streams inbox."""
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        enable_message_driven(self, "insight")


def create_agent(*args, **kwargs):
    return DeepSearchAgent(*args, **kwargs)

__all__ = ["DeepSearchAgent", "create_agent", "settings", "Settings"]
