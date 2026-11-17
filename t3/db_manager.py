import sqlite3
import threading
from typing import List, Tuple, Optional

class DbManager:
    def __init__(self, node_id: int):
        self.db_path = f"db_{node_id}.db"
        self.lock = threading.RLock()
        self._init_db()

    def _get_conn(self):
        # timeout=10 evita erro de 'database is locked' sob alta concorrência
        return sqlite3.connect(self.db_path, timeout=10.0)

    def _init_db(self):
        with self.lock, self._get_conn() as conn:
            cursor = conn.cursor()
            # 1. Metadados do consenso (apenas 1 linha com id = 1)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS metadata (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    current_term INTEGER NOT NULL,
                    voted_for INTEGER
                )
            """)
            cursor.execute("""
                INSERT OR IGNORE INTO metadata (id, current_term, voted_for)
                VALUES (1, 0, NULL)
            """)

            # 2. Log de operações (entradas com índice, termo e comando)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS log_entries (
                    log_index INTEGER PRIMARY KEY,
                    term INTEGER NOT NULL,
                    command TEXT NOT NULL
                )
            """)

            # 3. Banco final / Máquina de estados (apenas dados comitados)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS state_machine (
                    log_index INTEGER PRIMARY KEY,
                    data TEXT NOT NULL
                )
            """)
            conn.commit()

    # --- Métodos de Termo e Voto ---
    def get_metadata(self) -> Tuple[int, Optional[int]]:
        with self.lock, self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT current_term, voted_for FROM metadata WHERE id = 1")
            return cursor.fetchone()

    def save_metadata(self, term: int, voted_for: Optional[int]):
        with self.lock, self._get_conn() as conn:
            conn.execute(
                "UPDATE metadata SET current_term = ?, voted_for = ? WHERE id = 1",
                (term, voted_for)
            )
            conn.commit()

    # --- Métodos do Log ---
    def append_log(self, index: int, term: int, command: str):
        with self.lock, self._get_conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO log_entries (log_index, term, command) VALUES (?, ?, ?)",
                (index, term, command)
            )
            conn.commit()

    def get_last_log_info(self) -> Tuple[int, int]:
        """Retorna (last_log_index, last_log_term). Retorna (0, 0) se o log estiver vazio."""
        with self.lock, self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT log_index, term FROM log_entries ORDER BY log_index DESC LIMIT 1")
            row = cursor.fetchone()
            return (row[0], row[1]) if row else (0, 0)

    def get_log_entry(self, index: int) -> Optional[Tuple[int, int, str]]:
        """Retorna (log_index, term, command) ou None se não existir."""
        with self.lock, self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT log_index, term, command FROM log_entries WHERE log_index = ?", (index,))
            return cursor.fetchone()

    def get_entries_from(self, start_index: int) -> List[Tuple[int, int, str]]:
        """Retorna todas as entradas a partir de start_index (inclusivo)."""
        with self.lock, self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT log_index, term, command FROM log_entries WHERE log_index >= ? ORDER BY log_index ASC",
                (start_index,)
            )
            return cursor.fetchall()

    def truncate_log_from(self, conflict_index: int):
        """Apaga entradas a partir do índice conflitante (usado na resolução de inconsistência)."""
        with self.lock, self._get_conn() as conn:
            conn.execute("DELETE FROM log_entries WHERE log_index >= ?", (conflict_index,))
            conn.commit()

    # --- Métodos do Banco Final (Dados Comitados) ---
    def commit_up_to(self, target_commit_index: int):
        """Aplica no banco final todas as entradas do log até target_commit_index."""
        with self.lock, self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR IGNORE INTO state_machine (log_index, data)
                SELECT log_index, command FROM log_entries
                WHERE log_index <= ?
            """, (target_commit_index,))
            conn.commit()

    def get_all_committed_data(self) -> List[str]:
        """Retorna apenas os dados efetivados para responder leituras do cliente."""
        with self.lock, self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT data FROM state_machine ORDER BY log_index ASC")
            return [row[0] for row in cursor.fetchall()]