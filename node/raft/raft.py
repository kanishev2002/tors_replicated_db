from os import getenv, path
import json
import httpx
from typing import Dict, List
import logging
from .models import *
import asyncio
import random
from ..models import AppMessage, ActionType
from ..database import db
from fastapi.encoders import jsonable_encoder
from ..enums import NodeRole
from collections import defaultdict
import time

logger = logging.getLogger("Raft")
logger.setLevel(logging.DEBUG)

# Prevent duplicate handlers
if not logger.hasHandlers():
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("%(asctime)s - [Raft] - [%(funcName)s] - %(levelname)s: %(message)s")
    )
    logger.addHandler(handler)

# Prevent propagation to parent loggers
logger.propagate = False

class Raft:
    def __init__(self):
        self.node_id: str = getenv('NODE_ID')
        node_ips: Dict[str, str] = json.loads(getenv('NODE_IPS'))
        node_ips.pop(self.node_id)
        self.other_nodes = node_ips

        self._state_storage_file = f'state_{self.node_id}.json'
        if path.exists(self._state_storage_file):
            with open(self._state_storage_file) as file:
                json_data = json.load(file)
                self.current_term = json_data['current_term']
                self.voted_for = json_data['voted_for']
                self.commit_length = json_data['commit_length']
                self.log = list(map(AppMessage.model_validate, json_data['log']))
        else:
            self.current_term = 0
            self.voted_for = None
            self.log = []
            self.commit_length = 0
        
        # These don't need to be persisted
        self.current_role = NodeRole.follower
        self.current_leader = None
        self.votes_received = set()
        self.sent_length = defaultdict(int)
        self.acked_length = defaultdict(int)

        # Timers
        self.heartbeat_task = None
        # TODO: return timeouts
        # self.election_timeout_min = 0.15
        # self.election_timeout_max = 0.3
        # self.heartbeat_interval = 0.05
        # self.election_timeout_min = 1.5
        # self.election_timeout_max = 3
        self.election_timeout = random.uniform(2, 5)
        self.heartbeat_interval = 0.5
        self.reset_election_timer = asyncio.Event()

        self.last_heartbeat = time.monotonic()
        asyncio.create_task(self.election_timer())

        logger.info('Raft initialization complete.')

    async def election_timer(self):
        while True:
            if self.current_role == NodeRole.follower \
                  and time.monotonic() - self.last_heartbeat > self.election_timeout:
                await self._initialize_election()
            await asyncio.sleep(2)

    async def _initialize_election(self):
        logging.info('Election started')
        self.current_term += 1
        self.current_role = NodeRole.candidate
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}

        last_term = 0
        if len(self.log) > 0:
            last_term = self.log[-1].term
        
        logger.debug('Sending election requests')
        try:
            responses: List[httpx.Response] = await asyncio.gather(*(self._send(node, VoteRequest(type=MessageType.vote_request, 
                                            candidate_id=self.node_id, 
                                            candidate_current_term=self.current_term, 
                                            candidate_log_length=len(self.log), 
                                            last_term=last_term)) for node in self.other_nodes))
            vote_responses = [VoteResponse(**res.json()) for res in responses if res is not None]
            for response in vote_responses:
                await self.handle_vote_response(response)
        except Exception as e:
            logger.error('Error during polling: %s', e)
        logger.debug('Polling finished')
        
    def start_heartbeat_timer(self):
        # Cancel any previous heartbeat task
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        
        logger.info('Starting heartbeat task')
        self.heartbeat_task = asyncio.create_task(self._run_heartbeat_timer())

    async def _run_heartbeat_timer(self):
        while self.current_role == NodeRole.leader:
            # Send heartbeat (empty ReplicateLog) to all followers
            logger.info('Sending heartbeat')
            await self._send_replicate()
            await asyncio.sleep(self.heartbeat_interval)
        logger.info('No longer leader. Stopping heartbeat.')

    async def _send_replicate(self):
        responses: List[httpx.Response] = await asyncio.gather(*(self._replicate_log(node) for node in self.other_nodes))
        log_responses: List[LogResponse] = [LogResponse(**res.json()) for res in responses if res is not None]
        for response in log_responses:
            await self.handle_log_response(response)
    
    async def handle_vote_request(self, request: VoteRequest) -> VoteResponse:
        logger.info('Got vote request.')
        logger.debug('Request: %s', request.model_dump_json())
        myLogTerm = self.log[-1].term if len(self.log) > 0 else 0

        logOk = (request.last_term > myLogTerm) or (request.last_term == myLogTerm and request.candidate_log_length >= len(self.log))
        termOk = (request.candidate_current_term > self.current_term) or \
            (request.candidate_current_term == self.current_term and (self.voted_for in [None, request.candidate_id]))
        
        logger.debug('Log OK: %s', logOk)
        logger.debug('Term OK: %s', termOk)

        if logOk and termOk:
            self.current_term = request.candidate_current_term
            self.current_role = NodeRole.follower
            self.voted_for = request.candidate_id
            self.last_heartbeat = time.monotonic()
            self._persist_state()
            logger.info('Voted for %s on term %s', self.voted_for, self.current_term)
            return VoteResponse(type=MessageType.vote_response, 
                                           voter_id=self.node_id, 
                                           term=self.current_term, 
                                           granted=True)
                            
        else:
            logger.info('Vote not granted to %s', request.candidate_id)
            return VoteResponse(type=MessageType.vote_response, 
                                           voter_id=self.node_id, 
                                           term=self.current_term, 
                                           granted=False)
                        

    async def handle_vote_response(self, response: VoteResponse):
        logger.info('Got vote response from %s', response.voter_id)
        logger.debug('Response: %s', response.model_dump_json())

        if self.current_role == NodeRole.candidate and response.term == self.current_term and response.granted:
            self.votes_received.add(response.voter_id)
            quorum = (len(self.other_nodes) + 1) // 2 + 1
            if len(self.votes_received) >= quorum:
                logger.info('Got a quorum of votes. Becoming a leader.')
                self.current_role = NodeRole.leader
                self.current_leader = self.node_id
                for follower in self.other_nodes:
                    self.sent_length[follower] = len(self.log)
                    self.acked_length[follower] = 0
                self.start_heartbeat_timer()
                
        elif response.term > self.current_term:
            logger.info('Got response from a higher term. Becoming a follower.')
            self.current_term = response.term
            self.current_role = NodeRole.follower
            self.voted_for = None
        
        self._persist_state()
    
    async def send_message(self, message: AppMessage):
        logger.info('Got an app message.')
        logger.debug(f'Message: {message.model_dump_json()}')
        logger.debug(f'Current leader: {self.current_leader}')
        logger.debug(f'Other nodes: {self.other_nodes}')
        if self.current_role == NodeRole.leader:
            logger.info('Added app message to log. Replicating...')
            self.log.append(message)
            self.acked_length[self.node_id] = len(self.log)
            await self._send_replicate()
        else:
            logger.info('Not the leader. Forwarding the message to %s', self.current_leader)
            await self._send(self.current_leader, message)

    async def _replicate_log(self, follower_id: str):
        logger.debug('Replicating log')
        i = self.sent_length[follower_id]
        entries = self.log[i:]
        prev_log_term = 0
        if i > 0:
            prev_log_term = self.log[i-1].term
        return await self._send(follower_id, LogRequest(
                                          leader_id=self.current_leader, 
                                          term=self.current_term, log_length=i, log_term=prev_log_term, 
                                          leader_commit=self.commit_length, 
                                          entries=entries))

    async def handle_replicate_log_request(self, request: LogRequest) -> LogResponse:
        logger.info('Got replicate log request.')
        logger.debug('Request: %s', request.model_dump_json())
        if request.term >= self.current_term:
            self.last_heartbeat = time.monotonic()

        if request.term > self.current_term:
            logger.info('Got a request with a higher term. Becoming a follower.')
            self.current_term = request.term
            self.voted_for = None
            self.current_role = NodeRole.follower
            self.current_leader = request.leader_id

        if request.term == self.current_term and self.current_role in [NodeRole.candidate, NodeRole.follower]:
            logger.info('Got a request with a current term. Becoming a follower.')
            self.current_role = NodeRole.follower
            self.current_leader = request.leader_id


        log_ok = len(self.log) >= request.log_length and (request.log_length == 0 or request.log_term == self.log[request.log_length-1].term)
        logger.debug('Log OK: %s', log_ok)

        if request.term == self.current_term and log_ok:
            logger.info('Request successful. Appending logs.')
            self._append_entries(request.log_length, request.leader_commit, request.entries)
            logger.debug('Successfully appended entries.')
            ack = request.log_length + len(request.entries)
            self._persist_state()
            return LogResponse(
                            follower_id=self.node_id, 
                            term=self.current_term, 
                            ack=ack, 
                            success=True)
        else:
            logger.info('Request unsuccessful.')
            return LogResponse(
                            follower_id=self.node_id, 
                            term=self.current_term, 
                            ack=0, 
                            success=False)
        
    
    def _append_entries(self, log_length: int, leader_commit: int, entries: List[AppMessage]):
        if len(entries) > 0 and len(self.log) > log_length:
            logger.debug('Trimming log.')
            if self.log[log_length].term != entries[0].term:
                self.log = self.log[:log_length]
        
        if log_length + len(entries) > len(self.log):
            logger.debug('Appending new entries.')
            for i in range(len(self.log)-log_length, len(entries)):
                self.log.append(entries[i])

        if leader_commit > self.commit_length:
            logger.debug('Applying app messages.')
            for i in range(self.commit_length, leader_commit):
                self._apply_message(self.log[i])
            self.commit_length = leader_commit
    
    async def handle_log_response(self, response: LogResponse):
        logger.info('Got replicate log response.')
        logger.debug('Response: %s', response.model_dump_json())

        if response.term == self.current_term and self.current_role == NodeRole.leader:
            if response.success and response.ack >= self.acked_length[response.follower_id]:
                logger.info('Successfully replicated log on %s', response.follower_id)
                self.sent_length[response.follower_id] = response.ack
                self.acked_length[response.follower_id] = response.ack
                self._commit_log_entries()
            elif self.sent_length[response.follower_id] > 0:
                logger.info('Shortening log request for %s.', response.follower_id)
                self.sent_length[response.follower_id] -= 1
                response: httpx.Response = await self._replicate_log(response.follower_id)
                await self.handle_log_response(LogResponse(**response.json()))
        elif response.term > self.current_term:
            logger.info('Got a response from higher term. Becoming a follower.')
            self.current_term = response.term
            self.current_role = NodeRole.follower
            self.voted_for = None
        self._persist_state()

    def _commit_log_entries(self):
        logger.debug('Commiting log entries.')
        min_acks = (len(self.other_nodes) + 1) // 2

        ready = self._find_ready(min_acks)
        logger.debug('Ready: %s', ready)

        if ready != 0 and ready > self.commit_length and self.log[ready-1].term == self.current_term:
            for i in range(self.commit_length, ready):
                self._apply_message(self.log[i])
            self.commit_length = ready

    def _find_ready(self, min_acks: int) -> int:
        l = 0
        r = len(self.log) + 1
        while r - l > 1:
            m = (l+r) // 2
            acks = 0
            for node in self.other_nodes:
                if self.acked_length[node] >= m:
                    acks += 1
            if acks >= min_acks:
                l = m
            else:
                r = m
        return l


        
    
    async def _send(self, node_id, message: BaseModel):
        try:
            url = f'{self.other_nodes[node_id]}/internal/raft'
            logger.debug('Sending a request to %s', url)
            message_json = jsonable_encoder(message)
            logger.debug('Sending json: %s', json.dumps(message_json))
            async with httpx.AsyncClient() as client:
                return await client.post(url, json=message_json, timeout=httpx.Timeout(5, connect=1))
            logger.debug('Request completed')
        except httpx.RequestError as err:
            logger.error(f'Send HTTP error: {err}')
        except Exception as err:
            logger.error(f'Send error: {err}')

    def _persist_state(self):
        data = dict(
            current_term = self.current_term,
            voted_for = self.voted_for,
            commit_length = self.commit_length,
            log = list(
                map(
                    lambda entry: entry.model_dump(),
                    self.log
                )
            )
        )
        data_str = json.dumps(data)
        try:
            with open(self._state_storage_file, 'w') as file:
                file.write(data_str)
        except Exception as e:
            logger.error(f'Failed to save state: {e}')
    
    def _apply_message(self, message: AppMessage):
        match message.action_type:
            case ActionType.create:
                if message.key not in db:
                    db[message.key] = message.value
            case ActionType.update:
                if message.key in db:
                    db[message.key] = message.value
            case ActionType.delete:
                if message.key in db:
                    db.pop(message.key)

    