import sqlite3
import threading
from typing import Any, Dict, List, Optional, Tuple


class DatabaseManager:
    """
    Manages persistent storage for a Raft/KRaft-style Pull-based replicated node.
    Persists:
      1. Node state: current_term and voted_for.
      2. Operation log: sequential log_index, term, key, value (or operation payload).
      3. Committed state machine (final key-value store) & last_applied index.
    """

    def __init__(self, db_path: str = "db.db") -> None:
        self.db_path = db_path
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._configure_pragmas()
        self._init_schema()

    def _configure_pragmas(self) -> None:
        with self._lock:
            cursor = self.conn.cursor()
            # WAL mode improves concurrent read/write performance and durability
            cursor.execute("PRAGMA journal_mode=WAL;")
            cursor.execute("PRAGMA synchronous=FULL;")
            self.conn.commit()

    def _init_schema(self) -> None:
        with self._lock:
            cursor = self.conn.cursor()
            # 1. Persistent Raft metadata (single-row table)
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

            # 2. Persistent operation log (1-based indexing matching Raft conventions)
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

            # 3. Committed state machine (final database visible to client reads)
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS state_machine (
                    key TEXT PRIMARY KEY,
                    value TEXT
                );
                """
            )
            self.conn.commit()

    # -------------------------------------------------------------------------
    # Node Metadata (Term & Vote) Operations
    # -------------------------------------------------------------------------

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

    def get_current_term(self) -> int:
        with self._lock:
            return self.get_metadata()["current_term"]

    def get_voted_for(self) -> Optional[str]:
        with self._lock:
            return self.get_metadata()["voted_for"]

    def get_last_applied(self) -> int:
        with self._lock:
            return self.get_metadata()["last_applied"]

    def update_term_and_vote(self, new_term: int, voted_for: Optional[str]) -> None:
        """
        Atomically persists a new term and vote.
        Used when stepping down to Follower on a higher term (voted_for=None),
        starting an election (new_term = term + 1, voted_for=self_id),
        or granting a vote in the current term.
        """
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

    def set_voted_for(self, candidate_id: Optional[str]) -> None:
        """Persists the vote granted for the current term."""
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute(
                "UPDATE node_metadata SET voted_for = ? WHERE id = 1;",
                (candidate_id,),
            )
            self.conn.commit()

    # -------------------------------------------------------------------------
    # Operation Log Operations
    # -------------------------------------------------------------------------

    def get_last_log_info(self) -> Tuple[int, int]:
        """
        Returns (last_log_index, last_log_term).
        Returns (0, 0) if the log is empty.
        """
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
        """Fetches a single log entry by its sequential log_index."""
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

    def append_log_entry(
        self,
        term: int,
        key: str,
        value: Optional[str],
        operation: str = "SET",
        log_index: Optional[int] = None,
    ) -> int:
        """
        Appends a new entry to the persistent operation log.
        If log_index is None, automatically assigns last_log_index + 1 (Leader behavior).
        Returns the assigned log_index.
        """
        with self._lock:
            if log_index is None:
                last_index, _ = self.get_last_log_info()
                log_index = last_index + 1

            cursor = self.conn.cursor()
            cursor.execute(
                """
                INSERT INTO operation_log (log_index, term, operation, key, value)
                VALUES (?, ?, ?, ?, ?);
                """,
                (log_index, term, operation, key, value),
            )
            self.conn.commit()
            return log_index

    def append_or_overwrite_entries(self, entries: List[Dict[str, Any]]) -> None:
        """
        Replicates entries received from the Leader via FetchResponse.
        If an existing entry has the same index but a different term,
        truncates from that conflicting index onward before inserting.
        """
        if not entries:
            return

        with self._lock:
            cursor = self.conn.cursor()
            for entry in entries:
                idx = entry["log_index"]
                term = entry["term"]
                op = entry.get("operation", "SET")
                key = entry["key"]
                val = entry.get("value")

                existing = self.get_log_entry(idx)
                if existing is not None:
                    if existing["term"] != term:
                        # Conflict detected: truncate local log from idx onward
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
        Deletes all uncommitted log entries with log_index >= from_index.
        Used when a follower detects log divergence with the current leader.
        Returns the number of deleted rows.
        """
        with self._lock:
            last_applied = self.get_last_applied()
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
        """
        Raft/Pull election safety check:
        Returns True if the candidate's log is at least as up-to-date as this node's log.
        Comparison rule:
          1. Higher last_log_term wins.
          2. If last_log_term is equal, higher or equal last_log_index wins.
        """
        with self._lock:
            local_last_index, local_last_term = self.get_last_log_info()
            if candidate_last_term != local_last_term:
                return candidate_last_term > local_last_term
            return candidate_last_index >= local_last_index

    # -------------------------------------------------------------------------
    # State Machine (Committed Data) & Eventual Consistency Read Operations
    # -------------------------------------------------------------------------

    def apply_commits_up_to(self, commit_index: int) -> int:
        """
        Applies all log entries from (last_applied + 1) up to min(commit_index, last_log_index)
        into the final committed state_machine table.
        Uncommitted entries (> commit_index) remain invisible to client reads.
        Returns the updated last_applied index.
        """
        with self._lock:
            last_applied = self.get_last_applied()
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
        """
        Reads a value from the committed state machine.
        Guarantees uncommitted log entries are never returned.
        """
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT value FROM state_machine WHERE key = ?;", (key,))
            row = cursor.fetchone()
            return row["value"] if row else None

    def read_all_committed(self) -> Dict[str, Optional[str]]:
        """Returns a dictionary of all committed key-value pairs."""
        with self._lock:
            cursor = self.conn.cursor()
            cursor.execute("SELECT key, value FROM state_machine ORDER BY key ASC;")
            return {row["key"]: row["value"] for row in cursor.fetchall()}

    def close(self) -> None:
        with self._lock:
            self.conn.close()