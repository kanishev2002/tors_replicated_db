from contextlib import asynccontextmanager
import logging
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel
from .raft.raft import Raft
from .raft.models import *
from .database import db
from .models import AppMessage, ActionType
import random



class ValueBody(BaseModel):
    value: str

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start election timer in an async context on app start
    raft.start_election_timer()
    yield

app = FastAPI(lifespan=lifespan)
raft = Raft()

@app.get('/{key}')
async def read_item(key: str):
    if key not in db:
        raise HTTPException(404, detail='Item not found')
    elif raft.current_role == NodeRole.leader:
        other_node_ips = raft.other_nodes.values()
        redirect_location = random.choice(other_node_ips)
        return Response(status_code=302, headers={'Location': redirect_location})
    else:
        return db[key]

@app.post('/{key}')
async def create_item(key: str, value: ValueBody):
    await raft.send_message(AppMessage(action_type=ActionType.create, key=key, value=value))

@app.patch('/{key}')
async def update_item(key: str, value: ValueBody):
    await raft.send_message(AppMessage(action_type=ActionType.update, key=key, value=value))

@app.delete('/{key}')
async def delete_item(key: str):
    await raft.send_message(AppMessage(action_type=ActionType.delete, key=key))

@app.post('/raft', status_code=200)
async def raft_protocol_message(message: VoteRequest | VoteResponse | LogRequest | LogResponse | AppMessage):
    try:
        match message.type:
            case MessageType.vote_request:
                await raft.handle_vote_request(message)
            case MessageType.vote_response:
                await raft.handle_vote_response(message)
            case MessageType.replicate_log_request:
                await raft.handle_replicate_log_request(message)
            case MessageType.replicate_log_response:
                await raft.handle_log_response(message)
            case MessageType.app_message:
                await raft.send_message(message)
    except Exception as e:
        logging.error(f'Raft protocol exception: {e}')
        raise HTTPException(500, 'Internal server error')
