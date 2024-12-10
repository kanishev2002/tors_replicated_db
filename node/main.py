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

from os import getenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [Main] - %(levelname)s - %(message)s",
)

logger = logging.getLogger("main")


class ValueBody(BaseModel):
    value: str

@asynccontextmanager
async def lifespan(app: FastAPI):
    print('Lifespan is starting')
    # Start election timer in an async context on app start
    logger.info('Starting raft...')
    if getenv('NODE_ID') == '1':
        raft.start_election_timer()
    print('Lifespan finished')
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
async def create_item(key: str, body: ValueBody):
    await raft.send_message(AppMessage(action_type=ActionType.create, key=key, value=body.value))

@app.patch('/{key}')
async def update_item(key: str, body: ValueBody):
    await raft.send_message(AppMessage(action_type=ActionType.update, key=key, value=body.value))

@app.delete('/{key}')
async def delete_item(key: str):
    await raft.send_message(AppMessage(action_type=ActionType.delete, key=key))

@app.post('/internal/raft', status_code=200)
async def raft_protocol_message(request: Request):
    payload = await request.json()
    try:
        match payload['type']:
            case MessageType.vote_request:
                await raft.handle_vote_request(VoteRequest(**payload))
            case MessageType.vote_response:
                await raft.handle_vote_response(VoteResponse(**payload))
            case MessageType.replicate_log_request:
                await raft.handle_replicate_log_request(LogRequest(**payload))
            case MessageType.replicate_log_response:
                await raft.handle_log_response(LogResponse(**payload))
            case MessageType.app_message:
                await raft.send_message(AppMessage(**payload))
    except Exception as e:
        logger.error(f'Raft protocol exception: {e}')
        raise HTTPException(500, 'Internal server error')
