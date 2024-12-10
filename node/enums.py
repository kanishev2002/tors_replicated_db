
from enum import Enum

class ActionType(str, Enum):
    create = 'create'
    read = 'read'
    update = 'update'
    delete = 'delete'

class NodeRole(str, Enum):
    leader = 'leader'
    candidate = 'candidate'
    follower = 'follower'

class MessageType(str, Enum):
    vote_request = 'vote_request'
    vote_response = 'vote_response'
    replicate_log_request = 'replicate_log_request'
    replicate_log_response = 'replicate_log_response'
    app_message = 'app_message'