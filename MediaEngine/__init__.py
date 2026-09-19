"""Media Agent public API."""

from .agent import DeepSearchAgent as _DeepSearchAgent, AnspireSearchAgent as _AnspireSearchAgent
from .agent import create_agent as _create_agent
from .utils.config import Settings
from coordination.message_driven import enable_message_driven


class DeepSearchAgent(_DeepSearchAgent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        enable_message_driven(self, "media")


class AnspireSearchAgent(_AnspireSearchAgent):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        enable_message_driven(self, "media")


def create_agent(*args, **kwargs):
    return DeepSearchAgent(*args, **kwargs)

__all__ = ["DeepSearchAgent", "AnspireSearchAgent", "create_agent", "Settings"]
