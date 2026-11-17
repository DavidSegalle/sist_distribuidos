import sqlite3
import threading
from typing import Any, Dict, List, Optional, Tuple


class DatabaseManager:

    def __init__(self, db_path: str = "db.db") -> None:
        self.db_path = db_path
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            cursor = self.conn.cursor()

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS node_metadata (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    current_term INTEGER NOT NULL DEFAULT 0,
                    voted_for TEXT DEFAULT NULL,
                    last_applied INTEGER NOT NULL DEFAULT 0
                );
                """
            )

            cursor.execute(
                """
                INSERT OR IGNORE INTO node_metadata (id, current_term, voted_for, last_applied)
                VALUES (1, 0, NULL, 0);
                """
            )

            # Operation log
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS operation_log (
                    log_index INTEGER PRIMARY KEY,
                    term INTEGER NOT NULL,
                    operation TEXT NOT NULL DEFAULT 'SET',
                    key TEXT NOT NULL,
                    value TEXT
                );
                """
            )

            ## Commited values
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS state_machine (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
                """
            )
            self.conn.commit()

    def get_metadata(self) -> Dict[str, Any]:
        """Returns current_term, voted_for, and last_applied."""
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute(
                "SELECT current_term, voted_for, last_applied FROM node_metadata WHERE id = 1;"
            )
            row = cursor.fetchone()
            return {
                "current_term": row["current_term"],
                "voted_for": row["voted_for"],
                "last_applied": row["last_applied"],
            }

    def update_term_and_vote(self, new_term: int, voted_for: Optional[str]) -> None:
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                UPDATE node_metadata
                SET current_term = ?, voted_for = ?
                WHERE id = 1;
                """,
                (new_term, voted_for),
            )
            self.conn.commit()

    def get_last_log_info(self) -> Tuple[int, int]:
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT log_index, term
                FROM operation_log
                ORDER BY log_index DESC
                LIMIT 1;
                """
            )
            row = cursor.fetchone()
            if row is None:
                return (0, 0)
            return (row["log_index"], row["term"])

    def get_log_entry(self, log_index: int) -> Optional[Dict[str, Any]]:
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT log_index, term, operation, key, value
                FROM operation_log
                WHERE log_index = ?;
                """,
                (log_index,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_log_entries_from(self, start_index: int) -> List[Dict[str, Any]]:
        """
        Returns all log entries with log_index >= start_index, ordered ascending.
        Used by the Leader to serve FetchRequests from followers.
        """
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT log_index, term, operation, key, value
                FROM operation_log
                WHERE log_index >= ?
                ORDER BY log_index ASC;
                """,
                (start_index,),
            )
            return [dict(row) for row in cursor.fetchall()]

    def append_log_entry(self, term: int, key: str, value: Optional[str]) -> int:

        with self._lock:
            last_index, _ = self.get_last_log_info()
            log_index = last_index + 1

            cursor = self.conn.cursor()
            cursor.execute(
                """
                INSERT INTO operation_log (log_index, term, operation, key, value)
                VALUES (?, ?, 'SET', ?, ?);
                """,
                (log_index, term, key, value),
            )
            self.conn.commit()
            return log_index

    def append_or_overwrite_entries(self, entries: List[Dict[str, Any]]) -> None:
        """
        Replicates leader entries. If values don't match
        """

        with self._lock:
            cursor = self.conn.cursor()
            
            # For every entry received
            for entry in entries:
                idx = entry["log_index"]
                term = entry["term"]
                op = entry.get("operation", "SET")
                key = entry["key"]
                val = entry.get("value")

                existing = self.get_log_entry(idx)
                if existing is not None:

                    # Erases local log at conflict point
                    if existing["term"] != term:
                        cursor.execute(
                            "DELETE FROM operation_log WHERE log_index >= ?;",
                            (idx,),
                        )
                    else:
                        # Entry already matches at this index and term
                        continue

                cursor.execute(
                    """
                    INSERT INTO operation_log (log_index, term, operation, key, value)
                    VALUES (?, ?, ?, ?, ?);
                    """,
                    (idx, term, op, key, val),
                )
            self.conn.commit()

    def truncate_log_from(self, from_index: int) -> int:
        """
        If local logs diverge from leader delete them
        """
        with self._lock:
            last_applied = self.get_metadata()["last_applied"]
            if from_index <= last_applied:
                raise ValueError(
                    f"Cannot truncate already committed/applied index {from_index} "
                    f"(last_applied={last_applied})."
                )
            cursor = self.conn.cursor()
            cursor.execute(
                "DELETE FROM operation_log WHERE log_index >= ?;",
                (from_index,),
            )
            deleted = cursor.rowcount
            self.conn.commit()
            return deleted

    def is_candidate_log_up_to_date(
        self, candidate_last_index: int, candidate_last_term: int
    ) -> bool:

        with self._lock:
            local_last_index, local_last_term = self.get_last_log_info()
            if candidate_last_term != local_last_term:
                return candidate_last_term > local_last_term
            return candidate_last_index >= local_last_index

    def apply_commits_up_to(self, commit_index: int) -> int:

        with self._lock:
            last_applied = self.get_metadata()["last_applied"]
            last_log_index, _ = self.get_last_log_info()
            target_index = min(commit_index, last_log_index)

            if target_index <= last_applied:
                return last_applied

            cursor = self.conn.cursor()
            cursor.execute(
                """
                SELECT log_index, operation, key, value
                FROM operation_log
                WHERE log_index > ? AND log_index <= ?
                ORDER BY log_index ASC;
                """,
                (last_applied, target_index),
            )
            rows = cursor.fetchall()

            for row in rows:
                op = row["operation"].upper()
                key = row["key"]
                val = row["value"]

                if op == "DELETE":
                    cursor.execute("DELETE FROM state_machine WHERE key = ?;", (key,))
                else:
                    # Default SET/PUT upsert behavior
                    cursor.execute(
                        """
                        INSERT INTO state_machine (key, value)
                        VALUES (?, ?)
                        ON CONFLICT(key) DO UPDATE SET value = excluded.value;
                        """,
                        (key, val),
                    )

            cursor.execute(
                "UPDATE node_metadata SET last_applied = ? WHERE id = 1;",
                (target_index,),
            )
            self.conn.commit()
            return target_index

    def read_committed(self, key: str) -> Optional[str]:
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT value FROM state_machine WHERE key = ?;", (key,))
            row = cursor.fetchone()
            return row["value"] if row else None

    def read_all_committed(self) -> Dict[str, Optional[str]]:
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT key, value FROM state_machine ORDER BY key ASC;")
            return {row["key"]: row["value"] for row in cursor.fetchall()}

    def close(self) -> None:
        with self._lock:
            self.conn.close()