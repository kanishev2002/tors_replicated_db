from contextlib import asynccontextmanager
import logging
from fastapi import FastAPI, HTTPException, Response, Request
from pydantic import BaseModel

from .enums import NodeRole
from .raft.raft import Raft
from .raft.models import *
from .database import db
from .models import AppMessage, ActionType
import random

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [Main] - %(levelname)s - %(message)s",
)

logger = logging.getLogger("main")


class ValueBody(BaseModel):
    value: str

app = FastAPI()
raft = Raft()

@app.get('/{key}')
async def read_item(key: str):
    if key not in db:
        raise HTTPException(404, detail='Item not found')
    elif raft.current_role == NodeRole.leader:
        other_node_ips = list(raft.other_nodes.values())
        redirect_ip = random.choice(other_node_ips)
        redirect_location = f'{redirect_ip}/{key}'
        return Response(status_code=302, headers={'Location': redirect_location})
    else:
        return db[key]

@app.post('/{key}')
async def create_item(key: str, body: ValueBody):
    await raft.send_message(AppMessage(action_type=ActionType.create, key=key, value=body.value, term=raft.current_term))

@app.patch('/{key}')
async def update_item(key: str, body: ValueBody):
    await raft.send_message(AppMessage(action_type=ActionType.update, key=key, value=body.value, term=raft.current_term))

@app.delete('/{key}')
async def delete_item(key: str):
    await raft.send_message(AppMessage(action_type=ActionType.delete, key=key, term=raft.current_term))

@app.post('/internal/raft', status_code=200)
async def raft_protocol_message(request: Request):
    payload = await request.json()
    try:
        match payload['type']:
            case MessageType.vote_request:
                return await raft.handle_vote_request(VoteRequest(**payload))
            case MessageType.replicate_log_request:
                return await raft.handle_replicate_log_request(LogRequest(**payload))
            case MessageType.app_message:
                await raft.send_message(AppMessage(**payload))
    except Exception as e:
        logger.error(f'Raft protocol exception: {e}')
        raise HTTPException(500, 'Internal server error')
