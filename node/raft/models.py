from enum import Enum
from typing import List
from pydantic import BaseModel

class NodeRole(Enum):
    leader = 'leader'
    candidate = 'candidate'
    follower = 'follower'

class MessageType(Enum):
    vote_request = 'VoteRequest'
    vote_response = 'VoteResponse'
    replicate_log_request = 'LogRequest'
    replicate_log_response = 'LogResponse'
    app_message = 'AppMessage'

class VoteRequest(BaseModel):
    type: MessageType = MessageType.vote_request
    candidate_id: str
    candidate_current_term: int
    candidate_log_length: int
    last_term: int

class VoteResponse(BaseModel):
    type: MessageType = MessageType.vote_response
    voter_id: str
    term: int
    granted: bool

class LogRequest(BaseModel):
    type: MessageType = MessageType.replicate_log_request
    leader_id: str
    term: int
    log_length: int
    log_term: int
    leader_commit: int
    entries: List

class LogResponse(BaseModel):
    follower_id: str 
    term: int
    ack: int
    success: bool
