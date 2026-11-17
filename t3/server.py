import os
import sys
import time
import random
import threading
from concurrent import futures
import grpc

# Adiciona o diretório dos arquivos gerados pelo protoc ao path
sys.path.append("./generated_files")

import generated_files.route_guide_pb2 as pb2
import generated_files.route_guide_pb2_grpc as pb2_grpc
from server_state import ServerState, Role

# Configuração estática dos 4 nós do cluster
CLUSTER_NODES = {
    1: "localhost:50051",
    2: "localhost:50052",
    3: "localhost:50053",
    4: "localhost:50054",
}

# Temporizadores (em segundos)
FETCH_INTERVAL = 0.15          # Réplicas buscam dados a cada 150ms
HEARTBEAT_TIMEOUT = 0.60       # Líder marca réplica como inativa após 600ms sem Fetch
ELECTION_TIMEOUT_MIN = 1.5     # Tempo mínimo para iniciar eleição (1.5s)
ELECTION_TIMEOUT_MAX = 3.0     # Tempo máximo para iniciar eleição (3.0s)


class ClusterNodeServicer(pb2_grpc.ClusterServiceServicer):
    def __init__(self, node_id: int):
        self.node_id = node_id
        self.peer_ids = [nid for nid in CLUSTER_NODES if nid != node_id]
        self.state = ServerState(node_id, self.peer_ids)

        # Controle de temporizador de eleição
        self.last_leader_contact = time.time()
        self.election_timeout = self._random_timeout()

        # Evento para notificar a thread de Write quando o commit_index avançar
        self.commit_condition = threading.Condition(self.state.lock)
        self.running = True

        self._election_in_progress = False
        self._election_lock = threading.Lock()
        self._election_round = 0

        self._discover_leader()

        # Inicia threads de background: monitoramento de eleição e loop de Fetch (Pull)
        threading.Thread(target=self._election_timer_loop, daemon=True).start()
        threading.Thread(target=self._fetch_loop, daemon=True).start()

        self.state.print_status("Nó iniciado e pronto")

    def _random_timeout(self) -> float:
        return random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)

    def _reset_election_timer(self):
        self.last_leader_contact = time.time()
        self.election_timeout = self._random_timeout()

    # =========================================================================
    # 1. ELEIÇÃO DE LÍDER (RequestVote & Timer)
    # =========================================================================
    def _election_timer_loop(self):
        while self.running:
            time.sleep(0.05)

            with self.state.lock:
                expired = (
                    self.state.role != Role.LIDER
                    and time.time() - self.last_leader_contact
                    >= self.election_timeout
                )

            # Inicia fora do state.lock. O método controla sua própria trava.
            if expired:
                self._start_election()

    def _start_election(self):
        if not self._election_lock.acquire(blocking=False):
            return

        try:
            with self.state.lock:
                if self.state.role == Role.LIDER:
                    return

                if self._election_in_progress:
                    return

                self._election_in_progress = True
                self._election_round += 1
                election_round = self._election_round

                self.state.become_candidate()
                self._reset_election_timer()

                term = self.state.current_term
                last_index, last_term = self.state.storage.get_last_log_info()

            votes = 1  # voto próprio
            responses = 0
            vote_lock = threading.Lock()
            quorum_won = threading.Event()

            def ask_vote(peer_id):
                nonlocal votes, responses

                granted = False
                higher_term = None

                try:
                    with grpc.insecure_channel(CLUSTER_NODES[peer_id]) as channel:
                        stub = pb2_grpc.ClusterServiceStub(channel)
                        request = pb2.VoteRequest(
                            term=term,
                            candidate_id=self.node_id,
                            last_log_index=last_index,
                            last_log_term=last_term,
                        )
                        response = stub.RequestVote(request, timeout=0.6)

                    if response.term > term:
                        higher_term = response.term
                    else:
                        granted = response.vote_granted

                except grpc.RpcError:
                    pass

                with self.state.lock:
                    # Ignora respostas de uma eleição que já ficou obsoleta.
                    if election_round != self._election_round:
                        return

                    if higher_term is not None:
                        if higher_term > self.state.current_term:
                            self.state.step_down_to_follower(higher_term)
                            self._reset_election_timer()
                            self._election_in_progress = False
                        return

                    if (
                        granted
                        and self.state.role == Role.CANDIDATO
                        and self.state.current_term == term
                        and not quorum_won.is_set()
                    ):
                        with vote_lock:
                            votes += 1
                            if votes >= 3:
                                quorum_won.set()
                                self.state.become_leader()
                                self._election_in_progress = False
                                self._reset_election_timer()

                    with vote_lock:
                        responses += 1
                        if responses == len(self.peer_ids):
                            # Sem quórum: encerra esta rodada. O temporizador
                            # fará uma nova tentativa após o timeout aleatório.
                            self._election_in_progress = False

            for peer_id in self.peer_ids:
                threading.Thread(
                    target=ask_vote,
                    args=(peer_id,),
                    daemon=True,
                ).start()

        finally:
            self._election_lock.release()

    def _discover_leader(self):
        """Sonda os pares ao iniciar para descobrir se já existe um líder ativo no cluster."""
        last_idx, last_term = self.state.storage.get_last_log_info()
        for pid in self.peer_ids:
            addr = CLUSTER_NODES[pid]
            try:
                with grpc.insecure_channel(addr) as channel:
                    stub = pb2_grpc.ClusterServiceStub(channel)
                    req = pb2.FetchRequest(
                        term=self.state.current_term,
                        follower_id=self.node_id,
                        last_log_index=last_idx,
                        last_log_term=last_term,
                    )
                    resp = stub.Fetch(req, timeout=0.2)
                    with self.state.lock:
                        # Se o par respondeu com um líder válido, adota o termo e o líder
                        if resp.leader_id != 0:
                            if resp.term > self.state.current_term:
                                self.state.step_down_to_follower(resp.term, resp.leader_id)
                            else:
                                self.state.leader_id = resp.leader_id
                            self._reset_election_timer()
                            self.state.print_status(f"Descobriu Líder ativo: Nó {resp.leader_id}")
                            return
            except grpc.RpcError:
                continue

    def RequestVote(self, request, context):
        with self.state.lock:
            if request.term < self.state.current_term:
                return pb2.VoteResponse(
                    term=self.state.current_term,
                    vote_granted=False,
                )

            if request.term > self.state.current_term:
                self.state.step_down_to_follower(request.term)
                self._election_in_progress = False
                self._reset_election_timer()

            my_last_index, my_last_term = (
                self.state.storage.get_last_log_info()
            )

            candidate_log_is_up_to_date = (
                request.last_log_term > my_last_term
                or (
                    request.last_log_term == my_last_term
                    and request.last_log_index >= my_last_index
                )
            )

            can_vote = self.state.voted_for in (
                None,
                request.candidate_id,
            )

            if can_vote and candidate_log_is_up_to_date:
                self.state.record_vote(request.candidate_id)
                self.state.leader_id = None
                self._election_in_progress = False
                self._reset_election_timer()

                self.state.print_status(
                    f"Votou no Nó {request.candidate_id} "
                    f"(Termo {request.term})"
                )
                return pb2.VoteResponse(
                    term=self.state.current_term,
                    vote_granted=True,
                )

            return pb2.VoteResponse(
                term=self.state.current_term,
                vote_granted=False,
            )


    # =========================================================================
    # 2. REPLICAÇÃO PULL & HEARTBEAT (Fetch Loop & Handler)
    # =========================================================================
    
    def _fetch_loop(self):
        """Loop de replicação Pull: o seguidor busca entradas no líder."""

        while self.running:
            try:
                with self.state.lock:
                    if self.state.role == Role.LIDER:
                        pass
                    else:
                        leader_id = self.state.leader_id

                # Se este nó for líder, não executa Pull.
                if self.state.role == Role.LIDER:
                    time.sleep(FETCH_INTERVAL)
                    continue

                # Se não conhece o líder, tenta descobri-lo.
                if leader_id is None:
                    self._discover_leader()
                    time.sleep(FETCH_INTERVAL)
                    continue

                # Localiza o endereço do líder.
                if leader_id not in CLUSTER_NODES:
                    with self.state.lock:
                        self.state.leader_id = None
                    time.sleep(FETCH_INTERVAL)
                    continue

                channel = grpc.insecure_channel(CLUSTER_NODES[leader_id])
                stub = pb2_grpc.ClusterServiceStub(channel)

                with self.state.lock:
                    current_term = self.state.current_term
                    last_idx = self.state.storage.get_last_log_index()

                    if last_idx > 0:
                        last_entry = self.state.storage.get_log_entry(last_idx)
                        last_term = last_entry[1] if last_entry else 0
                    else:
                        last_term = 0

                req = pb2.FetchRequest(
                    term=current_term,
                    follower_id=self.node_id,
                    last_log_index=last_idx,
                    last_log_term=last_term,
                )

                resp = stub.Fetch(req, timeout=0.25)
                channel.close()

                with self.state.lock:
                    # O termo recebido é maior: este nó precisa recuar.
                    if resp.term > self.state.current_term:
                        self.state.step_down_to_follower(resp.term)
                        self.state.leader_id = None
                        self._election_in_progress = False
                        self._reset_election_timer()
                        continue

                    # Confirma que o próprio nó consultado é o líder.
                    if resp.leader_id != leader_id:
                        self.state.leader_id = None
                        continue

                    # Só renova o timeout após confirmação do líder.
                    self._reset_election_timer()
                    self.state.leader_id = leader_id

                    # Trata inconsistência de log.
                    if not resp.success:
                        if last_idx > 0:
                            self.state.print_status(
                                f"Conflito no índice {last_idx}! "
                                "Truncando log local..."
                            )

                            # Mantenha aqui o código original de truncamento
                            # do seu projeto, se ele existir.
                        continue

                    # Recebe e grava as entradas enviadas pelo líder.
                    for entry in resp.entries:
                        self.state.storage.append_log_entry(
                            entry.index,
                            entry.term,
                            entry.command
                        )

                    # Atualiza o commit_index local, sem ultrapassar
                    # o último índice disponível no log.
                    local_last_idx = self.state.storage.get_last_log_index()
                    new_commit = min(resp.commit_index, local_last_idx)

                    if new_commit > self.state.commit_index:
                        self.state.advance_commit_index(new_commit)

            except grpc.RpcError:
                with self.state.lock:
                    self.state.leader_id = None

            except Exception as e:
                print(f"[Nó {self.node_id}] Erro no Fetch Loop: {e}")

            time.sleep(FETCH_INTERVAL)


    def Fetch(self, request, context):
        """Executado pelo Líder ao receber FetchRequest (Heartbeat + Pull) de uma réplica."""
        with self.state.lock:
            if request.term < self.state.current_term:
                return pb2.FetchResponse(
                    term=self.state.current_term,
                    success=False,
                    commit_index=self.state.commit_index,
                    leader_id=self.state.leader_id or 0,
                )

            if request.term > self.state.current_term:
                self.state.step_down_to_follower(request.term)
                self._election_in_progress = False
                self._reset_election_timer()

            # O Fetch é recebido pelo líder. Se o papel já não for líder,
            # responda informando o termo/leader_id atual.
            if self.state.role != Role.LIDER:
                return pb2.FetchResponse(
                    term=self.state.current_term,
                    success=False,
                    commit_index=self.state.commit_index,
                    leader_id=0,
                )

            # Registra atividade (heartbeat) e o índice atual confirmado pela réplica
            fid = request.follower_id
            self.state.follower_last_heartbeat[fid] = time.time()
            self.state.follower_last_index[fid] = request.last_log_index

            # Verifica se o Líder consegue avançar o commit_index (quórum >= 3)
            self._try_advance_leader_commit()

            # Verifica consistência do último log informado pela réplica
            if request.last_log_index > 0:
                local_entry = self.state.storage.get_log_entry(request.last_log_index)
                if local_entry is None or local_entry[1] != request.last_log_term:
                    # Divergência detectada: sinaliza success=False para a réplica truncar
                    return pb2.FetchResponse(
                        term=self.state.current_term,
                        success=False,
                        commit_index=self.state.commit_index,
                        leader_id=self.node_id,
                    )

            # Busca entradas faltantes a partir de last_log_index + 1
            missing = self.state.storage.get_entries_from(request.last_log_index + 1)
            pb_entries = [
                pb2.LogEntry(index=idx, term=term, command=cmd)
                for (idx, term, cmd) in missing
            ]

            return pb2.FetchResponse(
                term=self.state.current_term,
                success=True,
                commit_index=self.state.commit_index,
                entries=pb_entries,
                leader_id=self.node_id,
            )

    def _try_advance_leader_commit(self):
        """Avança o commit_index do Líder se a maioria (3 de 4 nós) já possui o índice."""
        last_idx, _ = self.state.storage.get_last_log_info()
        for idx in range(self.state.commit_index + 1, last_idx + 1):
            entry = self.state.storage.get_log_entry(idx)
            # Regra do Raft: líder só commita entradas do seu próprio termo por contagem
            if entry and entry[1] == self.state.current_term:
                replicated_count = 1 + sum(
                    1 for pid in self.peer_ids
                    if self.state.follower_last_index.get(pid, 0) >= idx
                )
                if replicated_count >= 3:
                    self.state.advance_commit_index(idx)
                    self.commit_condition.notify_all()

    # =========================================================================
    # 3. OPERAÇÕES DO CLIENTE (Write & Read)
    # =========================================================================
    def Write(self, request, context):
        with self.commit_condition:
            # 1. Redirecionamento caso o cliente chame um Seguidor ou Candidato
            if self.state.role != Role.LIDER:
                lid = self.state.leader_id or 0
                laddr = CLUSTER_NODES.get(lid, "")
                return pb2.WriteResponse(
                    success=False,
                    message=f"Nó {self.node_id} não é o líder.",
                    leader_id=lid,
                    leader_address=laddr,
                )

            # 2. Bloqueio imediato por perda de quórum
            if not self.state.has_active_quorum(HEARTBEAT_TIMEOUT):
                self.state.print_status("Escrita rejeitada: Sem quórum ativo (< 3 nós)")
                return pb2.WriteResponse(
                    success=False,
                    message="Erro: Cluster sem quórum ativo (menos de 3 nós).",
                    leader_id=self.node_id,
                    leader_address=CLUSTER_NODES[self.node_id],
                )

            # 3. Grava no log local do Líder (ainda uncommitted)
            last_idx, _ = self.state.storage.get_last_log_info()
            new_idx = last_idx + 1
            term = self.state.current_term
            self.state.storage.append_log(new_idx, term, request.data)
            self.state.print_status(f"Gravou no log local idx={new_idx} (aguardando quórum): '{request.data}'")

            # Gatilho auxiliar para demonstrar o Cenário 4B (Crash antes de replicar)
            if request.data.startswith("CRASH_TEST:"):
                self.state.print_status("SIMULANDO CRASH IMEDIATO ANTES DA REPLICAÇÃO!")
                os._exit(1)

            # 4. Aguarda até que os seguidores puxem o dado e o quórum confirme o commit
            deadline = time.time() + 2.0
            while self.state.commit_index < new_idx and time.time() < deadline:
                remaining = deadline - time.time()
                if remaining > 0:
                    self.commit_condition.wait(timeout=remaining)

            if self.state.commit_index >= new_idx:
                return pb2.WriteResponse(
                    success=True,
                    message=f"Dado '{request.data}' comitado no índice {new_idx}.",
                    leader_id=self.node_id,
                    leader_address=CLUSTER_NODES[self.node_id],
                )
            else:
                return pb2.WriteResponse(
                    success=False,
                    message="Timeout aguardando confirmação do quórum.",
                    leader_id=self.node_id,
                    leader_address=CLUSTER_NODES[self.node_id],
                )

    def Read(self, request, context):
        """Leitura eventual consistente: retorna exclusivamente dados já comitados."""
        with self.state.lock:
            committed = self.state.storage.get_all_committed_data()
            return pb2.ReadResponse(committed_data=committed)


def serve(node_id: int):
    address = CLUSTER_NODES[node_id]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb2_grpc.add_ClusterServiceServicer_to_server(ClusterNodeServicer(node_id), server)
    server.add_insecure_port(address)
    server.start()
    print(f"Servidor do Nó {node_id} escutando em {address}...")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\nNó {node_id} desligado manualmente.")
        server.stop(0)


if __name__ == "__main__":
    if len(sys.argv) != 2 or int(sys.argv[1]) not in CLUSTER_NODES:
        print("Uso: python server.py <node_id>  (onde node_id é 1, 2, 3 ou 4)")
        sys.exit(1)

    serve(int(sys.argv[1]))