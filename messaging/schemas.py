"""Structured messages for decentralized BettaFish collaboration."""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from pydantic import BaseModel, Field


class AgentMessage(BaseModel):
    """A durable, replayable collaboration event."""

    message_id: str = Field(default_factory=lambda: str(uuid4()))
    task_id: str
    round_id: int = 0
    sender: str
    message_type: str
    content: Dict[str, Any] = Field(default_factory=dict)
    reply_to: Optional[str] = None
    correlation_id: Optional[str] = None
    evidence: List[Dict[str, Any]] = Field(default_factory=list)
    confidence: float = 0.5
    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )

    def to_bus_fields(self) -> Dict[str, str]:
        return {"payload": self.model_dump_json()}

    @classmethod
    def from_payload(cls, payload: str) -> "AgentMessage":
        return cls.model_validate_json(payload)
