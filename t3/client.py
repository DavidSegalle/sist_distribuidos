import os
import sys
import grpc

# Fix protobuf relative import inside generated_files (same as server.py)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "generated_files"))
from generated_files import route_guide_pb2
from generated_files import route_guide_pb2_grpc

CLUSTER_NODES = {
    1: "localhost:50051",
    2: "localhost:50052",
    3: "localhost:50053",
    4: "localhost:50054",
}
RPC_TIMEOUT_SEC = 1.0


class ClusterClient:
    def __init__(self, nodes: dict[int, str]):
        self.nodes = nodes
        self.known_leader_addr: str | None = None

    def write(self, data: str, target_node_id: int | None = None) -> bool:
        """
        Sends a Write request. If the contacted node is not the leader,
        follows leader_address redirection or probes remaining nodes.
        """
        candidates = []
        if target_node_id in self.nodes:
            candidates.append(self.nodes[target_node_id])
        if self.known_leader_addr and self.known_leader_addr not in candidates:
            candidates.append(self.known_leader_addr)
        for addr in self.nodes.values():
            if addr not in candidates:
                candidates.append(addr)

        visited = set()
        idx = 0
        while idx < len(candidates):
            addr = candidates[idx]
            idx += 1
            if addr in visited:
                continue
            visited.add(addr)

            try:
                with grpc.insecure_channel(addr) as channel:
                    stub = route_guide_pb2_grpc.ClusterServiceStub(channel)
                    resp = stub.Write(
                        route_guide_pb2.WriteRequest(data=data),
                        timeout=RPC_TIMEOUT_SEC,
                    )

                if resp.success:
                    self.known_leader_addr = resp.leader_address or addr
                    print(f"[WRITE OK] Leader Node {resp.leader_id} ({self.known_leader_addr}): {resp.message}")
                    return True

                # Follower redirected us to the current leader
                if resp.leader_address and resp.leader_address not in visited:
                    print(f"[REDIRECT] {addr} -> Leader Node {resp.leader_id} ({resp.leader_address})")
                    self.known_leader_addr = resp.leader_address
                    candidates.insert(idx, resp.leader_address)
                else:
                    print(f"[WRITE REJECTED] {addr}: {resp.message}")

            except grpc.RpcError as e:
                print(f"[UNREACHABLE] {addr}: {e.code().name}")

        print("[WRITE FAILED] Could not complete write on any cluster node.")
        return False

    def read(self, node_id: int, key: str = "") -> list[str]:
        """Reads committed data from any specific node in the cluster."""
        if node_id not in self.nodes:
            print(f"[ERROR] Invalid node_id {node_id}. Choose from {list(self.nodes.keys())}.")
            return []

        addr = self.nodes[node_id]
        try:
            with grpc.insecure_channel(addr) as channel:
                stub = route_guide_pb2_grpc.ClusterServiceStub(channel)
                resp = stub.Read(
                    route_guide_pb2.ReadRequest(key=key),
                    timeout=RPC_TIMEOUT_SEC,
                )
            entries = list(resp.committed_data)
            print(f"[READ Node {node_id} ({addr})] -> {entries}")
            return entries
        except grpc.RpcError as e:
            print(f"[READ ERROR] Node {node_id} ({addr}) unreachable: {e.code().name}")
            return []


def main() -> None:
    client = ClusterClient(CLUSTER_NODES)
    print("=== Distributed Cluster Client ===")
    print("Commands:")
    print("  write <key=value> [node_id]   -> Write data (auto-redirects to leader)")
    print("  read <node_id> [key]          -> Read committed data from a node")
    print("  readall [key]                 -> Read from all 4 nodes")
    print("  exit                          -> Quit\n")

    while True:
        try:
            raw = input("client> ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not raw:
            continue

        parts = raw.split()
        cmd = parts[0].lower()

        if cmd in ("exit", "quit"):
            break
        elif cmd == "write" and len(parts) >= 2:
            payload = parts[1]
            target = int(parts[2]) if len(parts) >= 3 else None
            client.write(payload, target_node_id=target)
        elif cmd == "read" and len(parts) >= 2:
            nid = int(parts[1])
            key = parts[2] if len(parts) >= 3 else ""
            client.read(nid, key=key)
        elif cmd == "readall":
            key = parts[1] if len(parts) >= 2 else ""
            for nid in CLUSTER_NODES:
                client.read(nid, key=key)
        else:
            print("Invalid command. Use: write <k=v> [node_id] | read <node_id> [key] | readall [key]")


if __name__ == "__main__":
    main()