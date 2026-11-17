
    
  
import time
import threading
from enum import Enum
from typing import Dict, Optional

from db_manager import DbManager


class Role(Enum):
    SEGUIDOR = "SEGUIDOR"
    CANDIDATO = "CANDIDATO"
    LIDER = "LIDER"


class ServerState:
    def __init__(self, node_id: int, peer_ids: list[int]):
        self.node_id = node_id
        self.peer_ids = peer_ids
        self.lock = threading.RLock()

        # Estado persistente
        self.storage = DbManager(node_id)
        self.current_term, self.voted_for = self.storage.get_metadata()

        # Estado volátil
        self.role = Role.SEGUIDOR
        self.leader_id: Optional[int] = None
        self.commit_index = self._recover_commit_index()

        # Informações mantidas pelo líder
        self.follower_last_index: Dict[int, int] = {
            pid: 0 for pid in peer_ids
        }
        self.follower_last_heartbeat: Dict[int, float] = {
            pid: 0.0 for pid in peer_ids
        }

    def _recover_commit_index(self) -> int:
        """Recupera o índice dos dados já efetivados no banco final."""
        committed = self.storage.get_all_committed_data()
        return len(committed)

    def step_down_to_follower(
        self,
        new_term: int,
        leader_id: Optional[int] = None,
    ) -> None:
        """
        Regride para seguidor quando recebe informação válida de termo
        igual ou superior. Um termo maior também zera o voto persistido.
        """
        with self.lock:
            if new_term < self.current_term:
                return

            term_changed = new_term > self.current_term
            role_changed = self.role != Role.SEGUIDOR

            if term_changed:
                self.current_term = new_term
                self.voted_for = None
                self.storage.save_metadata(
                    self.current_term,
                    self.voted_for,
                )

            if term_changed or role_changed:
                self.role = Role.SEGUIDOR

            if leader_id is not None:
                self.leader_id = leader_id
            elif term_changed:
                self.leader_id = None

            if term_changed or role_changed:
                self.print_status("Voltou para Seguidor")

    def become_candidate(self) -> None:
        with self.lock:
            self.role = Role.CANDIDATO
            self.current_term += 1
            self.voted_for = self.node_id
            self.leader_id = None

            self.storage.save_metadata(
                self.current_term,
                self.voted_for,
            )
            self.print_status("Iniciou eleição (Candidato)")

    def become_leader(self) -> None:
        with self.lock:
            if self.role != Role.CANDIDATO:
                return

            self.role = Role.LIDER
            self.leader_id = self.node_id

            now = time.time()
            last_idx, _ = self.storage.get_last_log_info()

            # Não considerar seguidores ativos antes de receber Fetch real.
            for pid in self.peer_ids:
                self.follower_last_index[pid] = 0
                self.follower_last_heartbeat[pid] = 0.0

            self.print_status(
                f"Eleito Líder! Último índice local: {last_idx}"
            )

    def record_vote(self, candidate_id: int) -> None:
        with self.lock:
            self.voted_for = candidate_id
            self.storage.save_metadata(
                self.current_term,
                self.voted_for,
            )

    def has_active_quorum(self, heartbeat_timeout_sec: float) -> bool:
        """Quórum = líder + pelo menos dois seguidores com Fetch recente."""
        with self.lock:
            if self.role != Role.LIDER:
                return False

            now = time.time()
            active_followers = sum(
                1
                for pid in self.peer_ids
                if (
                    self.follower_last_heartbeat.get(pid, 0.0) > 0
                    and now - self.follower_last_heartbeat[pid]
                    <= heartbeat_timeout_sec
                )
            )
            return active_followers + 1 >= 3

    def advance_commit_index(self, new_commit_index: int) -> None:
        with self.lock:
            if new_commit_index > self.commit_index:
                self.commit_index = new_commit_index
                self.storage.commit_up_to(self.commit_index)
                self.print_status(
                    f"Commit avançou para {self.commit_index}"
                )

    def print_status(self, event: str = "") -> None:
        with self.lock:
            last_idx, last_term = self.storage.get_last_log_info()
            msg = (
                f"[Nó {self.node_id} | {self.role.value:<9} | "
                f"Termo: {self.current_term} | "
                f"CommitIndex: {self.commit_index} | "
                f"LastLog: ({last_idx}, T{last_term})]"
            )
            if event:
                msg += f" -> {event}"
            print(msg, flush=True)

