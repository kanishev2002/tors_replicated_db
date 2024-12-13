from pydantic import BaseModel
from typing import Optional
from .enums import MessageType, ActionType

class AppMessage(BaseModel):
    type: MessageType = MessageType.app_message
    term: int
    action_type: ActionType
    key: str
    value: Optional[str] = None
