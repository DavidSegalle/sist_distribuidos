import os
from db_manager import DatabaseManager


def print_section(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f" {title}")
    print(f"{'=' * 60}")


def demo_initial_and_election_state(db: DatabaseManager) -> None:
    print_section("1. INITIAL STATE & ELECTION METADATA PERSISTENCE")
    print("Initial metadata:", db.get_metadata())
    print("Initial last log info (index, term):", db.get_last_log_info())

    # Node becomes candidate in term 1 and votes for itself
    db.update_term_and_vote(new_term=1, voted_for="node_1")
    print("After starting election (term=1, voted_for='node_1'):", db.get_metadata())

    # Node receives a message with a higher term (term=2) and steps down to follower
    db.update_term_and_vote(new_term=2, voted_for=None)
    print("After stepping down on higher term (term=2, voted_for=None):", db.get_metadata())


def demo_log_append_and_commit_isolation(db: DatabaseManager) -> None:
    print_section("2. LOG APPEND & UNCOMMITTED DATA ISOLATION")
    idx1 = db.append_log_entry(term=2, key="user:1", value="Alice")
    idx2 = db.append_log_entry(term=2, key="user:2", value="Bob")
    print(f"Appended log entries at indices: {idx1}, {idx2}")
    print("Current log entries:", db.get_log_entries_from(1))

    # Before commit_index advances, reads must return None / empty
    print("Read 'user:1' before commit:", db.read_committed("user:1"))
    print("All committed state before commit:", db.read_all_committed())

    # Quorum confirms up to index 1
    applied = db.apply_commits_up_to(commit_index=1)
    print(f"Applied commits up to index {applied}")
    print("Read 'user:1' after commit_index=1:", db.read_committed("user:1"))
    print("Read 'user:2' after commit_index=1 (still uncommitted):", db.read_committed("user:2"))

    # Quorum confirms up to index 2
    applied = db.apply_commits_up_to(commit_index=2)
    print(f"Applied commits up to index {applied}")
    print("All committed state after commit_index=2:", db.read_all_committed())


def demo_log_truncation_and_sync(db: DatabaseManager) -> None:
    print_section("3. LOG TRUNCATION & RESYNCHRONIZATION (SCENARIO 4B)")
    # Simulate an uncommitted entry from an old leader at index 3 (term 2)
    idx3 = db.append_log_entry(term=2, key="config:mode", value="stale_uncommitted")
    print(f"Appended uncommitted entry at index {idx3}:", db.get_log_entry(idx3))

    # New leader in term 3 sends authoritative entries starting at index 3
    incoming_entries = [
        {"log_index": 3, "term": 3, "operation": "SET", "key": "config:mode", "value": "active_leader_n2"},
        {"log_index": 4, "term": 3, "operation": "SET", "key": "user:3", "value": "Charlie"},
    ]
    print("Incoming entries from new leader (term 3):", incoming_entries)
    db.append_or_overwrite_entries(incoming_entries)

    print("Log after conflict truncation & overwrite:", db.get_log_entries_from(1))

    # Commit up to index 4
    db.apply_commits_up_to(commit_index=4)
    print("Final committed state machine:", db.read_all_committed())


def demo_vote_log_comparison(db: DatabaseManager) -> None:
    print_section("4. CANDIDATE LOG VALIDATION FOR VOTING")
    local_idx, local_term = db.get_last_log_info()
    print(f"Local node last log -> index={local_idx}, term={local_term}")

    candidates = [
        ("Candidate A (higher term, shorter log)", 1, 4),
        ("Candidate B (lower term, longer log)", 10, 2),
        ("Candidate C (same term, equal index)", local_idx, local_term),
        ("Candidate D (same term, lower index)", local_idx - 1, local_term),
    ]
    for label, c_idx, c_term in candidates:
        eligible = db.is_candidate_log_up_to_date(candidate_last_index=c_idx, candidate_last_term=c_term)
        print(f"  {label} (idx={c_idx}, term={c_term}) -> Grant vote? {eligible}")


def demo_restart_persistence(db_path: str) -> None:
    print_section("5. DISK PERSISTENCE ACROSS NODE RESTART")
    reopened_db = DatabaseManager(db_path)
    print("Reopened metadata:", reopened_db.get_metadata())
    print("Reopened last log info:", reopened_db.get_last_log_info())
    print("Reopened committed data:", reopened_db.read_all_committed())
    reopened_db.close()


def main() -> None:
    db_path = "db_test.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    db = DatabaseManager(db_path)
    try:
        demo_initial_and_election_state(db)
        demo_log_append_and_commit_isolation(db)
        demo_log_truncation_and_sync(db)
        demo_vote_log_comparison(db)
    finally:
        db.close()

    demo_restart_persistence(db_path)




if __name__ == "__main__":
    main()