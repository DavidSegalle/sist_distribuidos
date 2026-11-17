import os
import random
import sys
import threading
import time
from concurrent import futures
import traceback
from enum import Enum

import grpc

# Fix protobuf relative import inside generated_files
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "generated_files"))
from generated_files import route_guide_pb2
from generated_files import route_guide_pb2_grpc

from db_manager import DatabaseManager


class Role(Enum):
    FOLLOWER = "FOLLOWER"
    CANDIDATE = "CANDIDATE"
    LEADER = "LEADER"


CLUSTER_NODES = {
    1: "localhost:50051",
    2: "localhost:50052",
    3: "localhost:50053",
    4: "localhost:50054",
}
QUORUM_SIZE = 3

FETCH_INTERVAL_SEC = 0.100       # 100ms Pull heartbeat
HEARTBEAT_TIMEOUT_SEC = 0.500    # Leader marks follower inactive after 500ms
ELECTION_TIMEOUT_MIN = 1.500     # Randomized election timeout min
ELECTION_TIMEOUT_MAX = 3.000     # Randomized election timeout max
RPC_TIMEOUT_SEC = 0.400


class RaftClusterNode(route_guide_pb2_grpc.ClusterServiceServicer):
    def __init__(self, node_id: int, peers: dict[int, str]):
        self.node_id = node_id
        self.peers = {pid: addr for pid, addr in peers.items() if pid != node_id}
        self.address = peers[node_id]

        # Uses your existing DatabaseManager with a dedicated DB per node
        self.db = DatabaseManager(db_path=f"db_{self.node_id}.db")

        self.lock = threading.RLock()
        self.running = True

        self.role = Role.FOLLOWER
        self.leader_id: int | None = None
        self.commit_index: int = self.db.get_metadata()["last_applied"]

        self.election_timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        self.last_leader_contact = time.monotonic()
        self.follower_last_seen: dict[int, float] = {}
        self.follower_last_index: dict[int, int] = {pid: 0 for pid in self.peers}

        self._log_status("Node initialized from SQLite persistence")

        self.election_thread = threading.Thread(target=self._election_loop, daemon=True)
        self.fetch_thread = threading.Thread(target=self._pull_fetch_loop, daemon=True)

    def _log_status(self, msg: str) -> None:
        term = self.db.get_metadata()["current_term"]
        print(
            f"[Node {self.node_id} | Role: {self.role.value:<9} | "
            f"Term: {term} | CommitIdx: {self.commit_index} | "
            f"Leader: {self.leader_id}] {msg}",
            flush=True,
        )

    def _reset_election_timer(self) -> None:
        self.election_timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        self.last_leader_contact = time.monotonic()

    def _become_follower(self, new_term: int, leader_id: int | None = None, reason: str = "") -> None:
        current_term = self.db.get_metadata()["current_term"]
        role_changed = self.role != Role.FOLLOWER
        term_changed = new_term > current_term

        if term_changed:
            # Clears voted_for atomically in SQLite when advancing term
            self.db.update_term_and_vote(new_term, None)

        self.role = Role.FOLLOWER
        if leader_id is not None:
            self.leader_id = leader_id
        self._reset_election_timer()

        if role_changed or term_changed:
            self._log_status(f"Became FOLLOWER ({reason})" if reason else "Became FOLLOWER")

    def _start_election(self) -> None:
        with self.lock:
            if not self.running or self.role == Role.LEADER:
                return

            new_term = self.db.get_metadata()["current_term"] + 1
            # Persist incremented term and self-vote as string
            self.db.update_term_and_vote(new_term, str(self.node_id))

            self.role = Role.CANDIDATE
            self.leader_id = None
            self._reset_election_timer()

            last_log_index, last_log_term = self.db.get_last_log_info()
            self._log_status(f"Election timeout -> CANDIDATE for Term {new_term}")

        votes = 1
        vote_lock = threading.Lock()

        def request_vote_from_peer(peer_id: int, peer_addr: str) -> None:
            nonlocal votes
            req = route_guide_pb2.VoteRequest(
                term=new_term,
                candidate_id=self.node_id,
                last_log_index=last_log_index,
                last_log_term=last_log_term,
            )
            try:
                with grpc.insecure_channel(peer_addr) as channel:
                    stub = route_guide_pb2_grpc.ClusterServiceStub(channel)
                    resp = stub.RequestVote(req, timeout=RPC_TIMEOUT_SEC)

                with self.lock:
                    current_term = self.db.get_metadata()["current_term"]
                    if resp.term > current_term:
                        self._become_follower(resp.term, reason=f"higher term in VoteResponse from {peer_id}")
                        return

                    if self.role != Role.CANDIDATE or current_term != new_term:
                        return

                    if resp.vote_granted:
                        with vote_lock:
                            votes += 1
                            total = votes
                        self._log_status(f"Vote granted by Node {peer_id} ({total}/{QUORUM_SIZE})")
                        if total >= QUORUM_SIZE:
                            self.role = Role.LEADER
                            self.leader_id = self.node_id
                            now = time.monotonic()
                            for pid in self.peers:
                                self.follower_last_seen[pid] = now
                                self.follower_last_index[pid] = 0
                            self._log_status(f"Elected LEADER for Term {new_term}!")
            except grpc.RpcError:
                pass

        for pid, addr in self.peers.items():
            threading.Thread(target=request_vote_from_peer, args=(pid, addr), daemon=True).start()

    def _election_loop(self) -> None:
        while self.running:
            time.sleep(0.05)
            trigger = False
            with self.lock:
                if self.role in (Role.FOLLOWER, Role.CANDIDATE):
                    if (time.monotonic() - self.last_leader_contact) >= self.election_timeout:
                        trigger = True
            if trigger:
                self._start_election()

    def _pull_fetch_loop(self) -> None:
        peer_ids = list(self.peers.keys())
        rr_idx = 0

        while self.running:
            time.sleep(FETCH_INTERVAL_SEC)
            with self.lock:
                if self.role != Role.FOLLOWER:
                    continue
                target_id = self.leader_id
                current_term = self.db.get_metadata()["current_term"]
                last_idx, last_term = self.db.get_last_log_info()

            if target_id is None or target_id not in self.peers:
                target_id = peer_ids[rr_idx % len(peer_ids)]
                rr_idx += 1

            req = route_guide_pb2.FetchRequest(
                term=current_term,
                follower_id=self.node_id,
                last_log_index=last_idx,
                last_log_term=last_term,
            )
            try:
                with grpc.insecure_channel(self.peers[target_id]) as channel:
                    stub = route_guide_pb2_grpc.ClusterServiceStub(channel)
                    resp = stub.Fetch(req, timeout=RPC_TIMEOUT_SEC)

                with self.lock:
                    local_term = self.db.get_metadata()["current_term"]
                    if resp.term > local_term:
                        self._become_follower(
                            resp.term,
                            leader_id=resp.leader_id or None,
                            reason="higher term in FetchResponse",
                        )
                        continue

                    if resp.term < local_term or self.role != Role.FOLLOWER:
                        continue

                    if resp.leader_id != 0:
                        self.leader_id = resp.leader_id
                        self._reset_election_timer()

                    if resp.success:
                        if resp.entries:
                            entries_to_save = []
                            for entry in resp.entries:
                                if "=" in entry.command:
                                    key, value = entry.command.split("=", 1)
                                else:
                                    key, value = entry.command, entry.command
                                entries_to_save.append(
                                    {
                                        "log_index": entry.index,
                                        "term": entry.term,
                                        "operation": "SET",
                                        "key": key.strip(),
                                        "value": value.strip(),
                                    }
                                )
                            self.db.append_or_overwrite_entries(entries_to_save)
                            self._log_status(
                                f"Replicated {len(entries_to_save)} entry(ies) from Leader {self.leader_id}"
                            )

                        if resp.commit_index > self.commit_index:
                            self.commit_index = self.db.apply_commits_up_to(
                                resp.commit_index
                            )
                            self._log_status(
                                f"Applied commits up to index {self.commit_index}"
                            )
                    else:
                        # Log divergence detected by leader at last_idx: truncate and step back
                        if last_idx > self.commit_index:
                            deleted = self.db.truncate_log_from(last_idx)
                            self._log_status(
                                f"Log divergence at index {last_idx} (term {last_term}) -> "
                                f"truncated {deleted} entry(ies)"
                            )
            except grpc.RpcError:
                pass

    # --- gRPC RPCs ---

    def RequestVote(self, request, context):
        with self.lock:
            local_term = self.db.get_metadata()["current_term"]
            if request.term > local_term:
                self._become_follower(request.term, reason=f"higher term in RequestVote from {request.candidate_id}")
                local_term = request.term

            if request.term < local_term:
                return route_guide_pb2.VoteResponse(term=local_term, vote_granted=False)

            voted_for = self.db.get_metadata()["voted_for"]  # Stored as str or None
            cand_id_str = str(request.candidate_id)

            can_vote = (voted_for is None) or (voted_for == cand_id_str)
            log_ok = self.db.is_candidate_log_up_to_date(
                request.last_log_index, request.last_log_term
            )

            if can_vote and log_ok:
                self.db.set_voted_for(cand_id_str)
                self._reset_election_timer()
                self._log_status(f"Voted for Candidate Node {request.candidate_id} in Term {local_term}")
                return route_guide_pb2.VoteResponse(term=local_term, vote_granted=True)

            return route_guide_pb2.VoteResponse(term=local_term, vote_granted=False)

    def Fetch(self, request, context):
        with self.lock:
            local_term = self.db.get_metadata()["current_term"]
            if request.term > local_term:
                self._become_follower(
                    request.term,
                    reason=f"higher term in Fetch from {request.follower_id}",
                )
                local_term = request.term

            if request.term < local_term or self.role != Role.LEADER:
                return route_guide_pb2.FetchResponse(
                    term=local_term,
                    success=False,
                    commit_index=self.commit_index,
                    entries=[],
                    leader_id=self.leader_id or 0,
                )

            # 1. Record heartbeat and follower's confirmed replicated index
            # Record heartbeat
            self.follower_last_seen[request.follower_id] = time.monotonic()

            # Check for log divergence at request.last_log_index
            if request.last_log_index > 0:
                leader_entry = self.db.get_log_entry(request.last_log_index)
                if (
                    leader_entry is None
                    or leader_entry["term"] != request.last_log_term
                ):
                    # Do NOT update follower_last_index[request.follower_id] with a divergent index
                    return route_guide_pb2.FetchResponse(
                        term=local_term,
                        success=False,
                        commit_index=self.commit_index,
                        entries=[],
                        leader_id=self.node_id,
                    )

            # Only record follower_last_index once prefix consistency is confirmed
            self.follower_last_index[request.follower_id] = request.last_log_index

            # 3. Advance commit_index to the highest N > self.commit_index
            #    replicated on >= QUORUM_SIZE nodes with entry["term"] == local_term
            leader_last_index, _ = self.db.get_last_log_info()
            new_commit_index = self.commit_index

            for n in range(self.commit_index + 1, leader_last_index + 1):
                entry = self.db.get_log_entry(n)
                if entry is None or entry["term"] != local_term:
                    continue

                # Count leader (1) + followers whose last_log_index >= n
                replicated_count = 1 + sum(
                    1
                    for pid in self.peers
                    if self.follower_last_index.get(pid, 0) >= n
                )
                if replicated_count >= QUORUM_SIZE:
                    new_commit_index = n

            if new_commit_index > self.commit_index:
                self.commit_index = self.db.apply_commits_up_to(new_commit_index)
                self._log_status(f"Quorum reached -> CommitIdx advanced to {self.commit_index}")

            # 4. Look up all entries starting from request.last_log_index + 1 and pack them
            raw_entries = self.db.get_log_entries_from(request.last_log_index + 1)
            packed_entries = [
                route_guide_pb2.LogEntry(
                    index=e["log_index"],
                    term=e["term"],
                    command=f"{e['key']}={e['value']}",
                )
                for e in raw_entries
            ]

            return route_guide_pb2.FetchResponse(
                term=local_term,
                success=True,
                commit_index=self.commit_index,
                entries=packed_entries,
                leader_id=self.node_id,
            )

    def Write(self, request, context):
        try:
            with self.lock:
                if self.role != Role.LEADER:
                    return route_guide_pb2.WriteResponse(
                        success=False,
                        message="Not leader",
                        leader_id=self.leader_id or 0,
                        leader_address=CLUSTER_NODES.get(self.leader_id, ""),
                    )

                now = time.monotonic()
                active = 1 + sum(
                    1 for pid in self.peers
                    if (now - self.follower_last_seen.get(pid, 0.0)) <= HEARTBEAT_TIMEOUT_SEC
                )
                if active < QUORUM_SIZE:
                    return route_guide_pb2.WriteResponse(
                        success=False,
                        message=f"Error: No quorum ({active}/{QUORUM_SIZE})",
                        leader_id=self.node_id,
                        leader_address=self.address,
                    )

                term = self.db.get_metadata()["current_term"]
                if "=" in request.data:
                    key, value = request.data.split("=", 1)
                else:
                    key, value = request.data, request.data

                key = key.strip()
                value = value.strip()
                new_idx = self.db.append_log_entry(term=term, key=key, value=value)
                self._log_status(f"Appended [{key}={value}] at index {new_idx}, waiting for quorum...")

            # Poll outside self.lock so incoming Fetch RPCs can replicate & advance commit_index
            deadline = time.monotonic() + RPC_TIMEOUT_SEC
            while time.monotonic() < deadline:
                with self.lock:
                    curr_term = self.db.get_metadata()["current_term"]
                    if self.role != Role.LEADER or curr_term != term:
                        return route_guide_pb2.WriteResponse(
                            success=False,
                            message="Lost leadership before commit",
                            leader_id=self.leader_id or 0,
                            leader_address=CLUSTER_NODES.get(self.leader_id, ""),
                        )

                    if self.commit_index >= new_idx:
                        self._log_status(f"Committed write [{key}={value}] at index {new_idx}")
                        return route_guide_pb2.WriteResponse(
                            success=True,
                            message=f"Committed at index {new_idx}",
                            leader_id=self.node_id,
                            leader_address=self.address,
                        )

                time.sleep(0.01)  # 10ms polling interval

            return route_guide_pb2.WriteResponse(
                success=False,
                message=f"Error: Quorum timeout waiting to commit index {new_idx}",
                leader_id=self.node_id,
                leader_address=self.address,
            )
        except Exception as e:
            traceback.print_exc()
            context.abort(grpc.StatusCode.INTERNAL, str(e))

    def Read(self, request, context):
        try:
            if request.key:
                val = self.db.read_committed(request.key)
                return route_guide_pb2.ReadResponse(
                    committed_data=[f"{request.key}={val}"] if val is not None else []
                )
            all_data = self.db.read_all_committed()
            return route_guide_pb2.ReadResponse(
                committed_data=[f"{k}={v}" for k, v in all_data.items()]
            )
        except Exception as e:
            traceback.print_exc()
            context.abort(grpc.StatusCode.INTERNAL, str(e))

    def start(self) -> grpc.Server:
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        route_guide_pb2_grpc.add_ClusterServiceServicer_to_server(self, server)
        server.add_insecure_port(self.address)
        server.start()
        self.election_thread.start()
        self.fetch_thread.start()
        return server

    def stop(self) -> None:
        self.running = False
        self.db.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python server.py <node_id (1..4)>")
        sys.exit(1)

    nid = int(sys.argv[1])
    node = RaftClusterNode(nid, CLUSTER_NODES)
    srv = node.start()
    try:
        srv.wait_for_termination()
    except KeyboardInterrupt:
        node.stop()
        srv.stop(0.5)