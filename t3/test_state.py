import os
import time
from server_state import ServerState, Role

def limpar_bancos():
    for i in range(1, 5):
        if os.path.exists(f"node_{i}.db"):
            os.remove(f"node_{i}.db")

def rodar_testes():
    limpar_bancos()
    print("=== 1. Testando Eleição e Persistência em Disco ===")
    no1 = ServerState(node_id=1, peer_ids=[2, 3, 4])
    no1.become_candidate()
    no1.become_leader()
    assert no1.current_term == 1
    assert no1.role == Role.LIDER

    # Simula gravação no log (ainda SEM commit)
    no1.storage.append_log(index=1, term=1, command="x=10")
    assert no1.storage.get_all_committed_data() == [], "Erro: Dado uncommitted vazou na leitura!"
    print("[OK] Isolamento de dados uncommitted funcionando.")

    # Simula avanço de commit após quórum
    no1.advance_commit_index(1)
    assert no1.storage.get_all_committed_data() == ["x=10"]
    print("[OK] Efetivação no banco final após commit funcionando.")

    print("\n=== 2. Testando Crash e Recuperação do SQLite ===")
    # Grava uma segunda entrada que NÃO será comitada antes do crash
    no1.storage.append_log(index=2, term=1, command="x=999_invalido")
    del no1  # Destrói o objeto da memória (simula queda do processo)

    # Religa o Nó 1 lendo do arquivo node_1.db
    no1_recuperado = NodeState(node_id=1, peer_ids=[2, 3, 4])
    assert no1_recuperado.current_term == 1, "Erro: Não recuperou o termo do disco!"
    assert no1_recuperado.commit_index == 1, "Erro: CommitIndex errado após recuperação!"
    assert no1_recuperado.storage.get_all_committed_data() == ["x=10"]
    assert no1_recuperado.storage.get_last_log_info() == (2, 1)
    print("[OK] Recuperação após desligamento funcionando.")

    print("\n=== 3. Testando Truncamento por Divergência ===")
    # O novo líder está no Termo 2 e ordena truncar a partir do índice 2
    no1_recuperado.step_down_to_follower(new_term=2, leader_id=2)
    no1_recuperado.storage.truncate_log_from(conflict_index=2)
    assert no1_recuperado.storage.get_last_log_info() == (1, 1)
    print("[OK] Truncamento de log divergente funcionando.")

    print("\n=== 4. Testando Controle de Quórum ===")
    no2 = NodeState(node_id=2, peer_ids=[1, 3, 4])
    no2.become_candidate()
    no2.become_leader()
    # Apenas o nó 1 envia heartbeat; nós 3 e 4 estão "mortos" (timestamp 0.0)
    no2.follower_last_heartbeat[1] = time.time()
    no2.follower_last_heartbeat[3] = 0.0
    no2.follower_last_heartbeat[4] = 0.0
    assert no2.has_active_quorum(heartbeat_timeout_sec=1.0) is False, "Erro: Aceitou quórum com apenas 2 nós!"
    
    # Nó 3 volta a responder -> agora tem 3 nós ativos (Líder 2 + Seguidores 1 e 3)
    no2.follower_last_heartbeat[3] = time.time()
    assert no2.has_active_quorum(heartbeat_timeout_sec=1.0) is True
    print("[OK] Regra de quórum fixo (mínimo 3 de 4 nós) funcionando.")

    limpar_bancos()
    print("\nTODOS OS TESTES PASSARAM COM SUCESSO!")

if __name__ == "__main__":
    rodar_testes()