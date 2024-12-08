from os import getenv, path
import json
import httpx
from typing import Dict, List
import logging
from node.raft.models import *
import asyncio
import random
from node.models import AppMessage, ActionType
from node.database import db

class Raft:
    def __init__(self):
        self.node_id = int(getenv('NODE_ID'))
        self.other_nodes: Dict[str, str] = json.loads(getenv('NODE_IPS'))

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
        self.sent_length = dict()
        self.acked_length = dict()

        # Timers
        self.election_task = None
        self.heartbeat_task = None
        self.election_timeout_min = 0.15
        self.election_timeout_max = 0.3
        self.heartbeat_interval = 0.05
        self.reset_election_timer = asyncio.Event()

        self.start_election_timer()

    def start_election_timer(self):
        # Cancel existing timer if any
        if self.election_task:
            self.election_task.cancel()
        
        self.election_task = asyncio.create_task(self._run_election_timer())

    async def _run_election_timer(self):
        while True:
            # Wait a random timeout
            timeout = random.uniform(self.election_timeout_min, self.election_timeout_max)
            
            try:
                # Use wait_for to race between a heartbeat reset event and the timeout
                await asyncio.wait_for(self.reset_election_timer.wait(), timeout=timeout)
                # If we get here, it means reset_election_timer was set, so reset it and loop again
                self.reset_election_timer.clear()
            except asyncio.TimeoutError:
                # Timeout happened; no heartbeat received in the given timeframe
                # This means we should start an election if we're follower/candidate
                if self.current_role in [NodeRole.follower, NodeRole.candidate]:
                    await self._initialize_election()
                # Break or loop again depending on your logic
                # If you remain a candidate and fail election, you might continue looping
                # If you become leader, election timer might not be needed.

    async def _initialize_election(self):
        self.current_term += 1
        self.current_role = NodeRole.candidate
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}

        last_term = 0
        if len(self.log) > 0:
            last_term = self.log[-1].term
        
        await asyncio.gather(*(self._send(node, VoteRequest(type=MessageType.vote_request, 
                                         candidate_id=self.node_id, 
                                         candidate_current_term=self.current_term, 
                                         candidate_log_length=len(self.log), 
                                         last_term=last_term)) for node in self.other_nodes.values()))
        

        self.start_election_timer()

    def start_heartbeat_timer(self):
        # Cancel any previous heartbeat task
        if self.heartbeat_task:
            self.heartbeat_task.cancel()
        
        self.heartbeat_task = asyncio.create_task(self._run_heartbeat_timer())

    async def _run_heartbeat_timer(self):
        while self.current_role == NodeRole.leader:
            # Send heartbeat (empty ReplicateLog) to all followers
            await self._send_heartbeat()
            await asyncio.sleep(self.heartbeat_interval)

    async def _send_heartbeat(self):
        await asyncio.gather(*(self._replicate_log(node) for node in self.other_nodes.values()))
    
    async def handle_vote_request(self, request: VoteRequest):
        myLogTerm = self.log[-1].term if len(self.log) > 0 else 0

        logOk = (request.last_term > myLogTerm) or (request.last_term == myLogTerm and request.candidate_log_length >= len(self.log))
        termOk = (request.candidate_current_term > self.current_term) or \
            (request.candidate_current_term == self.current_term and (self.voted_for in [None, request.candidate_id]))

        if logOk and termOk:
            self.current_term = request.candidate_current_term
            self.current_role = NodeRole.follower
            self.voted_for = request.candidate_id
            await self._send(self.other_nodes[request.candidate_id], 
                              VoteResponse(type=MessageType.vote_response, 
                                           voter_id=self.node_id, 
                                           term=self.current_term, 
                                           granted=True)
                            )
        else:
            await self._send(self.other_nodes[request.candidate_id], 
                              VoteResponse(type=MessageType.vote_response, 
                                           voter_id=self.node_id, 
                                           term=self.current_term, 
                                           granted=False)
                            )            
        self._persist_state()

    async def handle_vote_response(self, response: VoteResponse):
        if self.current_role == NodeRole.candidate and response.term == self.current_term and response.granted:
            self.votes_received.add(response.voter_id)
            quorum = (len(self.other_nodes) + 1) // 2 + 1
            if len(self.votes_received) >= quorum:
                self.current_role = NodeRole.leader
                self.current_leader = self.node_id
                self.election_task.cancel()
                self.start_heartbeat_timer()
                for follower in self.other_nodes.keys():
                    self.sent_length[follower] = len(self.log)
                    self.acked_length[follower] = 0
                    await self._replicate_log(follower)
        elif response.term > self.current_term:
            self.current_term = response.term
            self.current_role = NodeRole.follower
            self.voted_for = None
            self.election_task.cancel()
            self.start_election_timer()
        
        self._persist_state()
    
    async def send_message(self, message: AppMessage):
        if self.current_role == NodeRole.leader:
            self.log.append(message)
            self.acked_length[self.node_id] = len(self.log)
            for follower in self.other_nodes:
                await self._replicate_log(follower)
        else:
            await self._send(self.current_leader, message)

    async def _replicate_log(self, follower_id: str):
        i = self.sent_length[follower_id]
        entries = self.log[i:]
        prev_log_term = 0
        if i > 0:
            prev_log_term = self.log[i-1].term
        await self._send(follower_id, LogRequest(type=MessageType.replicate_log_request,
                                          leader_id=self.current_leader, 
                                          term=self.current_term, log_length=i, log_term=prev_log_term, 
                                          leader_commit=self.commit_length, 
                                          entries=entries))

    async def handle_replicate_log_request(self, request: LogRequest):
        if request.term > self.current_term:
            self.current_term = request.term
            self.current_role = NodeRole.follower
            self.current_leader = request.leader_id
        if request.term == self.current_term and self.current_role == NodeRole.candidate:
            self.current_role = NodeRole.follower
            self.current_leader = request.leader_id
        
        self.reset_election_timer.set()

        log_ok = len(self.log) >= request.log_length and (request.log_length == 0 or request.log_term == self.log[request.log_length-1].term)

        if request.term == self.current_term and log_ok:
            self._append_entries(request.log_length, request.leader_commit, request.entries)
            ack = request.log_length + len(request.entries)
            await self._send(request.leader_id, LogResponse(type=MessageType.replicate_log_response, 
                                                      follower_id=self.node_id, 
                                                      term=self.current_term, 
                                                      ack=ack, 
                                                      success=True))
        else:
            await self._send(request.leader_id, LogResponse(type=MessageType.replicate_log_response, 
                                                      follower_id=self.node_id, 
                                                      term=self.current_term, 
                                                      ack=0, 
                                                      success=False))
        
        self._persist_state()
        
    
    def _append_entries(self, log_length: int, leader_commit: int, entries: List):
        if len(entries) > 0 and len(self.log) > log_length:
            if self.log[log_length-1].term != entries[0].term:
                self.log = self.log[:log_length]
        
        if log_length + len(entries) > len(self.log):
            for i in range(len(self.log)-log_length, len(entries)):
                self.log.append(entries[i])

        if leader_commit > self.commit_length:
            for i in range(self.commit_length, leader_commit):
                self._apply_message(self.log[i])
            self.commit_length = leader_commit
    
    async def handle_log_response(self, response: LogResponse):
        if response.term == self.current_term and self.current_role == NodeRole.leader:
            if response.success and response.ack >= self.acked_length[response.follower_id]:
                self.sent_length[response.follower_id] = response.ack
                self.acked_length[response.follower_id] = response.ack
                self._commit_log_entries()
            elif self.sent_length[response.follower_id] > 0:
                self.sent_length[response.follower_id] -= 1
                await self._replicate_log(response.follower_id)
        elif response.term > self.current_term:
            self.current_term = response.term
            self.current_role = NodeRole.follower
            self.voted_for = None
        self._persist_state()

    def _commit_log_entries(self):
        min_acks = (len(self.other_nodes) + 1) // 2

        ready = self._find_ready(min_acks)

        if ready != 0 and ready > self.commit_length and self.log[ready-1].term == self.current_term:
            for i in range(self.commit_length, ready):
                self._apply_message(self.log[i])
            self.commit_length = ready

    def _find_ready(self, min_acks: int) -> int:
        l = 0
        r = len(self.log)
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
        url = self.other_nodes[node_id]
        try:
            async with httpx.AsyncClient() as client:
                await client.post(url, json=message.model_dump())
        except httpx.RequestError as err:
            logging.error(f'Send error: {err}')

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
            logging.error(f'Failed to save state: {e}')
    
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

    