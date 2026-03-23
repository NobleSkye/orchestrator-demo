#!/usr/bin/env python3
"""
Minecraft Server Orchestrator Service

Central controller for managing Minecraft server instances across multiple nodes.
Runs on the Velocity proxy server (Port 8080) and coordinates with Node Agents
to provision, monitor, and decommission game server containers.

Features:
- REST API for server lifecycle management
- Node registry with health monitoring
- Least-loaded, resource-aware load balancing
- AutoServer config.toml integration for Velocity proxy
- SQLite persistence for server instance tracking
- Plugin update coordination across distributed nodes
"""

import json
import logging
import os
import sqlite3
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

import requests
from flask import Flask, Response, jsonify, request

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
)
logger = logging.getLogger("orchestrator")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CONFIG_PATH = os.environ.get(
    "ORCHESTRATOR_CONFIG", "/opt/minecraft/orchestrator.config.json"
)
AUTOSERVER_CONFIG_PATH = os.environ.get(
    "AUTOSERVER_CONFIG", "/opt/minecraft/velocity/autoserver/config.toml"
)
DB_PATH = os.environ.get("ORCHESTRATOR_DB", "/opt/minecraft/orchestrator.db")
AUTH_TOKEN = os.environ.get("AGENT_SECRET", "change-me-in-production")
ORCHESTRATOR_HOST = os.environ.get("ORCHESTRATOR_HOST", "0.0.0.0")
ORCHESTRATOR_PORT = int(os.environ.get("ORCHESTRATOR_PORT", "8080"))

# ---------------------------------------------------------------------------
# Data Models
# ---------------------------------------------------------------------------


@dataclass
class Node:
    """Represents a physical or virtual minigame node running a Node Agent."""

    id: str
    host: str
    port: int
    max_servers: int
    type: str  # e.g. "minigame", "lobby"
    labels: Dict[str, str] = field(default_factory=dict)
    healthy: bool = True
    failed_pings: int = 0
    current_servers: int = 0


@dataclass
class ServerInstance:
    """Represents a running Minecraft server container."""

    id: str
    gamemode: str
    node_id: str
    container_name: str
    port: int
    address: str  # host:port reachable from Velocity
    status: str  # "starting", "running", "stopping", "stopped", "error"
    created_at: float
    player_count: int = 0
    memory: str = "1G"


# ---------------------------------------------------------------------------
# Database Layer
# ---------------------------------------------------------------------------


class Database:
    """Thread-safe SQLite persistence for server instances."""

    def __init__(self, db_path: str) -> None:
        self._db_path = db_path
        self._lock = threading.Lock()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS servers (
                    id TEXT PRIMARY KEY,
                    gamemode TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    container_name TEXT NOT NULL,
                    port INTEGER NOT NULL,
                    address TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'starting',
                    created_at REAL NOT NULL,
                    player_count INTEGER NOT NULL DEFAULT 0,
                    memory TEXT NOT NULL DEFAULT '1G'
                )
                """
            )
            conn.commit()
            conn.close()

    def save_server(self, server: ServerInstance) -> None:
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                """
                INSERT OR REPLACE INTO servers
                (id, gamemode, node_id, container_name, port, address,
                 status, created_at, player_count, memory)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    server.id,
                    server.gamemode,
                    server.node_id,
                    server.container_name,
                    server.port,
                    server.address,
                    server.status,
                    server.created_at,
                    server.player_count,
                    server.memory,
                ),
            )
            conn.commit()
            conn.close()

    def remove_server(self, server_id: str) -> None:
        with self._lock:
            conn = self._get_conn()
            conn.execute("DELETE FROM servers WHERE id = ?", (server_id,))
            conn.commit()
            conn.close()

    def get_server(self, server_id: str) -> Optional[ServerInstance]:
        with self._lock:
            conn = self._get_conn()
            row = conn.execute(
                "SELECT * FROM servers WHERE id = ?", (server_id,)
            ).fetchone()
            conn.close()
            if row is None:
                return None
            return self._row_to_instance(row)

    def get_all_servers(self) -> List[ServerInstance]:
        with self._lock:
            conn = self._get_conn()
            rows = conn.execute("SELECT * FROM servers").fetchall()
            conn.close()
            return [self._row_to_instance(r) for r in rows]

    def get_servers_on_node(self, node_id: str) -> List[ServerInstance]:
        with self._lock:
            conn = self._get_conn()
            rows = conn.execute(
                "SELECT * FROM servers WHERE node_id = ?", (node_id,)
            ).fetchall()
            conn.close()
            return [self._row_to_instance(r) for r in rows]

    def update_status(self, server_id: str, status: str) -> None:
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                "UPDATE servers SET status = ? WHERE id = ?", (status, server_id)
            )
            conn.commit()
            conn.close()

    @staticmethod
    def _row_to_instance(row: sqlite3.Row) -> ServerInstance:
        return ServerInstance(
            id=row["id"],
            gamemode=row["gamemode"],
            node_id=row["node_id"],
            container_name=row["container_name"],
            port=row["port"],
            address=row["address"],
            status=row["status"],
            created_at=row["created_at"],
            player_count=row["player_count"],
            memory=row["memory"],
        )


# ---------------------------------------------------------------------------
# Node Manager
# ---------------------------------------------------------------------------


class NodeManager:
    """Manages the registry of nodes and their health status."""

    def __init__(self, nodes_config: List[Dict[str, Any]]) -> None:
        self._lock = threading.Lock()
        self.nodes: Dict[str, Node] = {}
        for cfg in nodes_config:
            node = Node(
                id=cfg["id"],
                host=cfg["host"],
                port=cfg.get("port", 5000),
                max_servers=cfg.get("max_servers", 10),
                type=cfg.get("type", "minigame"),
                labels=cfg.get("labels", {}),
            )
            self.nodes[node.id] = node
        logger.info("Loaded %d node(s) from config", len(self.nodes))

    def get_node(self, node_id: str) -> Optional[Node]:
        return self.nodes.get(node_id)

    def get_all_nodes(self) -> List[Node]:
        return list(self.nodes.values())

    def select_node(
        self,
        db: Database,
        gamemode: Optional[str] = None,
        preferred_node: Optional[str] = None,
    ) -> Optional[Node]:
        """
        Select the best node for a new server using least-loaded strategy.
        Optionally filter by preferred node or gamemode-compatible type.
        """
        with self._lock:
            candidates: List[Node] = []

            # If a specific node is requested, try that first
            if preferred_node and preferred_node in self.nodes:
                node = self.nodes[preferred_node]
                if node.healthy:
                    current = len(db.get_servers_on_node(node.id))
                    if current < node.max_servers:
                        node.current_servers = current
                        return node

            # Otherwise, find the least-loaded healthy node
            for node in self.nodes.values():
                if not node.healthy:
                    continue
                current = len(db.get_servers_on_node(node.id))
                node.current_servers = current
                if current < node.max_servers:
                    candidates.append(node)

            if not candidates:
                return None

            # Sort by current load (ascending), then by max_servers (descending)
            candidates.sort(key=lambda n: (n.current_servers, -n.max_servers))
            return candidates[0]

    def mark_unhealthy(self, node_id: str) -> None:
        with self._lock:
            if node_id in self.nodes:
                self.nodes[node_id].healthy = False
                logger.warning("Node %s marked UNHEALTHY", node_id)

    def mark_healthy(self, node_id: str) -> None:
        with self._lock:
            if node_id in self.nodes:
                self.nodes[node_id].healthy = True
                self.nodes[node_id].failed_pings = 0

    def increment_failed_pings(self, node_id: str) -> int:
        """Increment failed ping count. Returns new count."""
        with self._lock:
            if node_id in self.nodes:
                self.nodes[node_id].failed_pings += 1
                return self.nodes[node_id].failed_pings
        return 0

    def _node_url(self, node: Node) -> str:
        return f"http://{node.host}:{node.port}"

    def ping_node(self, node: Node) -> bool:
        """Check if a node agent is reachable."""
        try:
            resp = requests.get(
                f"{self._node_url(node)}/status",
                headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
                timeout=5,
            )
            return resp.status_code == 200
        except requests.RequestException:
            return False

    def create_container(
        self, node: Node, spec: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Ask a node agent to create a container. Returns response JSON or None."""
        try:
            resp = requests.post(
                f"{self._node_url(node)}/create",
                json=spec,
                headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
                timeout=60,
            )
            if resp.status_code == 200:
                return resp.json()
            logger.error(
                "Node %s create failed: %s %s",
                node.id,
                resp.status_code,
                resp.text,
            )
        except requests.RequestException as exc:
            logger.error("Node %s create error: %s", node.id, exc)
        return None

    def destroy_container(
        self, node: Node, container_name: str, server_id: str
    ) -> bool:
        """Ask a node agent to destroy a container."""
        try:
            resp = requests.post(
                f"{self._node_url(node)}/destroy",
                json={"container_name": container_name, "server_id": server_id},
                headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
                timeout=30,
            )
            return resp.status_code == 200
        except requests.RequestException as exc:
            logger.error("Node %s destroy error: %s", node.id, exc)
            return False


# ---------------------------------------------------------------------------
# AutoServer Config Manager
# ---------------------------------------------------------------------------


class AutoServerConfig:
    """
    Manages the Velocity AutoServer config.toml file.
    Appends/removes server entries so Velocity can route players dynamically.
    """

    MANAGED_MARKER = "# managed-by-orchestrator"

    def __init__(self, config_path: str) -> None:
        self._path = config_path
        self._lock = threading.Lock()

    def add_server(self, server: ServerInstance) -> None:
        """Append a managed server block to the AutoServer config."""
        block = (
            f"\n{self.MANAGED_MARKER}:{server.id}\n"
            f'[servers."{server.id}"]\n'
            f"start = \"echo 'Managed by orchestrator'\"\n"
            f'stop = "curl -s -X POST http://localhost:{ORCHESTRATOR_PORT}'
            f'/destroy/{server.id}"\n'
            f'address = "{server.address}"\n'
            f"startupDelay = 5\n"
            f"timeout = 300\n"
        )
        with self._lock:
            try:
                with open(self._path, "a") as f:
                    f.write(block)
                logger.info("AutoServer config: added %s", server.id)
            except OSError as exc:
                logger.error("Failed to write AutoServer config: %s", exc)

    def remove_server(self, server_id: str) -> None:
        """Remove a managed server block from the AutoServer config."""
        with self._lock:
            try:
                with open(self._path, "r") as f:
                    lines = f.readlines()
            except FileNotFoundError:
                return

            new_lines: List[str] = []
            skip = False
            for line in lines:
                if line.strip() == f"{self.MANAGED_MARKER}:{server_id}":
                    skip = True
                    continue
                if skip:
                    # Stop skipping when we hit the next section or managed marker
                    if line.startswith("[servers.") or line.startswith(
                        self.MANAGED_MARKER
                    ):
                        skip = False
                        if line.startswith(self.MANAGED_MARKER):
                            # This is the start of the next managed block
                            new_lines.append(line)
                            continue
                    else:
                        continue
                new_lines.append(line)

            try:
                with open(self._path, "w") as f:
                    f.writelines(new_lines)
                logger.info("AutoServer config: removed %s", server_id)
            except OSError as exc:
                logger.error("Failed to update AutoServer config: %s", exc)


# ---------------------------------------------------------------------------
# Health Monitor Background Thread
# ---------------------------------------------------------------------------


def health_monitor(node_manager: NodeManager, interval: int = 30) -> None:
    """
    Periodically ping all nodes and mark them healthy/unhealthy.
    A node is marked unhealthy after 3 consecutive failed pings.
    """
    while True:
        for node in node_manager.get_all_nodes():
            if node_manager.ping_node(node):
                if not node.healthy:
                    logger.info("Node %s recovered", node.id)
                node_manager.mark_healthy(node.id)
            else:
                count = node_manager.increment_failed_pings(node.id)
                if count >= 3:
                    node_manager.mark_unhealthy(node.id)
        time.sleep(interval)


# ---------------------------------------------------------------------------
# Idle Server Reaper Background Thread
# ---------------------------------------------------------------------------


def idle_reaper(
    db: Database,
    node_manager: NodeManager,
    autoserver: AutoServerConfig,
    idle_timeout_minutes: int = 5,
    max_lifetime_hours: int = 2,
) -> None:
    """
    Periodically check for idle or expired servers and destroy them.
    - Servers with 0 players for longer than idle_timeout_minutes are destroyed.
    - Servers older than max_lifetime_hours are destroyed regardless.
    """
    while True:
        now = time.time()
        for server in db.get_all_servers():
            if server.status not in ("running", "starting"):
                continue

            lifetime_exceeded = (now - server.created_at) > (
                max_lifetime_hours * 3600
            )
            idle_exceeded = (
                server.player_count == 0
                and (now - server.created_at) > (idle_timeout_minutes * 60)
            )

            if lifetime_exceeded or idle_exceeded:
                reason = "lifetime" if lifetime_exceeded else "idle"
                logger.info(
                    "Reaping server %s (reason: %s)", server.id, reason
                )
                node = node_manager.get_node(server.node_id)
                if node:
                    node_manager.destroy_container(
                        node, server.container_name, server.id
                    )
                autoserver.remove_server(server.id)
                db.remove_server(server.id)

        time.sleep(60)


# ---------------------------------------------------------------------------
# Flask Application
# ---------------------------------------------------------------------------


def load_config() -> Dict[str, Any]:
    """Load orchestrator configuration from JSON file."""
    try:
        with open(CONFIG_PATH, "r") as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as exc:
        logger.warning("Could not load config from %s: %s", CONFIG_PATH, exc)
        return {"nodes": [], "auto_shutdown": {}, "templates": {}}


def create_app() -> Flask:
    """Create and configure the Flask application."""
    app = Flask(__name__)

    config = load_config()
    db = Database(DB_PATH)
    node_manager = NodeManager(config.get("nodes", []))
    autoserver = AutoServerConfig(AUTOSERVER_CONFIG_PATH)

    auto_shutdown = config.get("auto_shutdown", {})
    idle_timeout = auto_shutdown.get("idle_timeout_minutes", 5)
    max_lifetime = auto_shutdown.get("max_lifetime_hours", 2)

    # Maintenance mode flag (thread-safe via GIL for simple bool)
    maintenance_mode = {"enabled": False}

    # -----------------------------------------------------------------------
    # Auth middleware
    # -----------------------------------------------------------------------
    @app.before_request
    def check_auth() -> Optional[Response]:
        # Allow health endpoint without auth
        if request.path == "/health":
            return None
        token = request.headers.get("Authorization", "")
        if token != f"Bearer {AUTH_TOKEN}" and token != AUTH_TOKEN:
            return jsonify({"error": "Unauthorized"}), 401
        return None

    # -----------------------------------------------------------------------
    # Health
    # -----------------------------------------------------------------------
    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({"status": "ok", "maintenance": maintenance_mode["enabled"]})

    # -----------------------------------------------------------------------
    # POST /create - Provision a new server
    # -----------------------------------------------------------------------
    @app.route("/create", methods=["POST"])
    def create_server():
        if maintenance_mode["enabled"]:
            return jsonify({"error": "System is in maintenance mode"}), 503

        data = request.get_json(silent=True) or {}
        gamemode = data.get("gamemode")
        if not gamemode:
            return jsonify({"error": "Missing required field: gamemode"}), 400

        memory = data.get("memory", "1G")
        preferred_node = data.get("node_id")

        # Select best node
        node = node_manager.select_node(db, gamemode, preferred_node)
        if node is None:
            return (
                jsonify({"error": "No available nodes with capacity"}),
                503,
            )

        # Generate unique server ID
        server_id = f"{gamemode}-{uuid.uuid4().hex[:8]}"
        container_name = f"mc-{server_id}"

        # Build container creation spec for the node agent
        spec = {
            "server_id": server_id,
            "gamemode": gamemode,
            "container_name": container_name,
            "memory": memory,
        }

        # Request container creation from node agent
        result = node_manager.create_container(node, spec)
        if result is None:
            return (
                jsonify({"error": f"Failed to create container on node {node.id}"}),
                500,
            )

        allocated_port = result.get("port", 25565)
        address = f"{node.host}:{allocated_port}"

        # Persist server instance
        server = ServerInstance(
            id=server_id,
            gamemode=gamemode,
            node_id=node.id,
            container_name=container_name,
            port=allocated_port,
            address=address,
            status="running",
            created_at=time.time(),
            player_count=0,
            memory=memory,
        )
        db.save_server(server)

        # Update AutoServer config so Velocity can route to this server
        autoserver.add_server(server)

        # Write address file for AutoServer to read
        addr_file = f"/tmp/autoserver-{server_id}.addr"
        try:
            with open(addr_file, "w") as f:
                f.write(address)
        except OSError as exc:
            logger.warning("Could not write address file %s: %s", addr_file, exc)

        logger.info(
            "Created server %s on node %s at %s", server_id, node.id, address
        )
        return jsonify(
            {
                "server_id": server_id,
                "node_id": node.id,
                "address": address,
                "port": allocated_port,
                "container_name": container_name,
                "status": "running",
            }
        )

    # -----------------------------------------------------------------------
    # POST /destroy/<server_id> - Decommission a server
    # -----------------------------------------------------------------------
    @app.route("/destroy/<server_id>", methods=["POST"])
    def destroy_server(server_id: str):
        server = db.get_server(server_id)
        if server is None:
            return jsonify({"error": f"Server {server_id} not found"}), 404

        node = node_manager.get_node(server.node_id)
        if node is None:
            # Node config removed — just clean up DB
            logger.warning(
                "Node %s not found for server %s, cleaning up DB only",
                server.node_id,
                server_id,
            )
        else:
            success = node_manager.destroy_container(
                node, server.container_name, server_id
            )
            if not success:
                logger.error(
                    "Failed to destroy container %s on node %s",
                    server.container_name,
                    server.node_id,
                )

        # Clean up AutoServer config and address file
        autoserver.remove_server(server_id)
        addr_file = f"/tmp/autoserver-{server_id}.addr"
        try:
            os.remove(addr_file)
        except OSError:
            pass

        db.remove_server(server_id)
        logger.info("Destroyed server %s", server_id)
        return jsonify({"status": "destroyed", "server_id": server_id})

    # -----------------------------------------------------------------------
    # GET /status - List all active servers
    # -----------------------------------------------------------------------
    @app.route("/status", methods=["GET"])
    def status():
        servers = db.get_all_servers()
        return jsonify(
            {
                "total": len(servers),
                "maintenance": maintenance_mode["enabled"],
                "servers": [asdict(s) for s in servers],
            }
        )

    # -----------------------------------------------------------------------
    # GET /nodes - Node capacity and health
    # -----------------------------------------------------------------------
    @app.route("/nodes", methods=["GET"])
    def nodes():
        result = []
        for node in node_manager.get_all_nodes():
            current = len(db.get_servers_on_node(node.id))
            result.append(
                {
                    "id": node.id,
                    "host": node.host,
                    "port": node.port,
                    "type": node.type,
                    "max_servers": node.max_servers,
                    "current_servers": current,
                    "healthy": node.healthy,
                    "labels": node.labels,
                }
            )
        return jsonify({"nodes": result})

    # -----------------------------------------------------------------------
    # POST /update-all-plugins - Push plugin update to all instances
    # -----------------------------------------------------------------------
    @app.route("/update-all-plugins", methods=["POST"])
    def update_all_plugins():
        data = request.get_json(silent=True) or {}
        plugin_path = data.get("plugin_path")
        plugin_name = data.get("plugin_name")
        restart = data.get("restart", False)

        if not plugin_path or not plugin_name:
            return (
                jsonify(
                    {"error": "Missing required fields: plugin_path, plugin_name"}
                ),
                400,
            )

        servers = db.get_all_servers()
        results = {"success": [], "failed": []}

        for server in servers:
            if server.status != "running":
                continue
            node = node_manager.get_node(server.node_id)
            if node is None:
                results["failed"].append(
                    {"server_id": server.id, "error": "node not found"}
                )
                continue
            try:
                resp = requests.post(
                    f"http://{node.host}:{node.port}/update-plugins",
                    json={
                        "container_name": server.container_name,
                        "plugin_path": plugin_path,
                        "plugin_name": plugin_name,
                        "restart": restart,
                    },
                    headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
                    timeout=30,
                )
                if resp.status_code == 200:
                    results["success"].append(server.id)
                else:
                    results["failed"].append(
                        {"server_id": server.id, "error": resp.text}
                    )
            except requests.RequestException as exc:
                results["failed"].append(
                    {"server_id": server.id, "error": str(exc)}
                )

        return jsonify(results)

    # -----------------------------------------------------------------------
    # POST /rolling-update - Batch rolling update with drain logic
    # -----------------------------------------------------------------------
    @app.route("/rolling-update", methods=["POST"])
    def rolling_update():
        data = request.get_json(silent=True) or {}
        plugin_path = data.get("plugin_path")
        plugin_name = data.get("plugin_name")
        batch_size = data.get("batch_size", 1)
        delay_seconds = data.get("delay_seconds", 10)
        drain_timeout = data.get("drain_timeout", 60)

        if not plugin_path or not plugin_name:
            return (
                jsonify(
                    {"error": "Missing required fields: plugin_path, plugin_name"}
                ),
                400,
            )

        servers = [s for s in db.get_all_servers() if s.status == "running"]
        results = {"updated": [], "failed": [], "total": len(servers)}

        # Process in batches
        for i in range(0, len(servers), batch_size):
            batch = servers[i : i + batch_size]
            for server in batch:
                node = node_manager.get_node(server.node_id)
                if node is None:
                    results["failed"].append(
                        {"server_id": server.id, "error": "node not found"}
                    )
                    continue

                # Drain: wait for players to leave (or timeout)
                db.update_status(server.id, "draining")
                drain_start = time.time()
                while time.time() - drain_start < drain_timeout:
                    current = db.get_server(server.id)
                    if current and current.player_count == 0:
                        break
                    time.sleep(5)

                # Stop container
                node_manager.destroy_container(
                    node, server.container_name, server.id
                )
                db.update_status(server.id, "updating")

                # Update plugin via node agent
                try:
                    resp = requests.post(
                        f"http://{node.host}:{node.port}/update-plugins",
                        json={
                            "container_name": server.container_name,
                            "plugin_path": plugin_path,
                            "plugin_name": plugin_name,
                            "restart": True,
                        },
                        headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
                        timeout=60,
                    )
                    if resp.status_code == 200:
                        db.update_status(server.id, "running")
                        results["updated"].append(server.id)
                    else:
                        db.update_status(server.id, "error")
                        results["failed"].append(
                            {"server_id": server.id, "error": resp.text}
                        )
                except requests.RequestException as exc:
                    db.update_status(server.id, "error")
                    results["failed"].append(
                        {"server_id": server.id, "error": str(exc)}
                    )

            # Delay between batches
            if i + batch_size < len(servers):
                time.sleep(delay_seconds)

        return jsonify(results)

    # -----------------------------------------------------------------------
    # POST /shutdown-all - Global maintenance: stop all servers
    # -----------------------------------------------------------------------
    @app.route("/shutdown-all", methods=["POST"])
    def shutdown_all():
        maintenance_mode["enabled"] = True
        servers = db.get_all_servers()
        results = {"stopped": [], "failed": []}

        for server in servers:
            if server.status in ("stopped", "stopping"):
                continue
            node = node_manager.get_node(server.node_id)
            if node is None:
                results["failed"].append(
                    {"server_id": server.id, "error": "node not found"}
                )
                continue
            success = node_manager.destroy_container(
                node, server.container_name, server.id
            )
            if success:
                db.update_status(server.id, "stopped")
                autoserver.remove_server(server.id)
                results["stopped"].append(server.id)
            else:
                results["failed"].append(
                    {"server_id": server.id, "error": "destroy failed"}
                )

        return jsonify(
            {"maintenance": True, "results": results}
        )

    # -----------------------------------------------------------------------
    # POST /start-all - Exit maintenance: restart persisted servers
    # -----------------------------------------------------------------------
    @app.route("/start-all", methods=["POST"])
    def start_all():
        maintenance_mode["enabled"] = False
        servers = db.get_all_servers()
        results = {"started": [], "failed": []}

        for server in servers:
            if server.status != "stopped":
                continue
            node = node_manager.get_node(server.node_id)
            if node is None:
                results["failed"].append(
                    {"server_id": server.id, "error": "node not found"}
                )
                continue

            spec = {
                "server_id": server.id,
                "gamemode": server.gamemode,
                "container_name": server.container_name,
                "memory": server.memory,
            }
            result = node_manager.create_container(node, spec)
            if result:
                allocated_port = result.get("port", server.port)
                server.port = allocated_port
                server.address = f"{node.host}:{allocated_port}"
                server.status = "running"
                db.save_server(server)
                autoserver.add_server(server)
                results["started"].append(server.id)
            else:
                results["failed"].append(
                    {"server_id": server.id, "error": "create failed"}
                )

        return jsonify({"maintenance": False, "results": results})

    # -----------------------------------------------------------------------
    # Start background threads
    # -----------------------------------------------------------------------
    monitor_thread = threading.Thread(
        target=health_monitor, args=(node_manager,), daemon=True
    )
    monitor_thread.start()

    reaper_thread = threading.Thread(
        target=idle_reaper,
        args=(db, node_manager, autoserver, idle_timeout, max_lifetime),
        daemon=True,
    )
    reaper_thread.start()

    return app


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

app = create_app()

if __name__ == "__main__":
    logger.info("Starting Orchestrator on %s:%d", ORCHESTRATOR_HOST, ORCHESTRATOR_PORT)
    app.run(host=ORCHESTRATOR_HOST, port=ORCHESTRATOR_PORT, threaded=True)
