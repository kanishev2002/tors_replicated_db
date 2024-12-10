from pydantic import BaseModel
from typing import Optional
from .enums import MessageType, ActionType

class AppMessage(BaseModel):
    message_type: MessageType = MessageType.app_message
    action_type: ActionType
    key: str
    value: Optional[str] = None
