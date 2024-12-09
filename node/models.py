from pydantic import BaseModel
from enum import Enum
from typing import Optional
from .raft.models import MessageType

class ActionType(Enum):
    create = 'create'
    read = 'read'
    update = 'update'
    delete = 'delete'

class AppMessage(BaseModel):
    message_type: MessageType = MessageType.app_message
    action_type: ActionType
    key: str
    value: Optional[str] = None
