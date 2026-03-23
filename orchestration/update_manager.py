#!/usr/bin/env python3
"""
Plugin Update Manager

Implements four distinct plugin update procedures for Minecraft server instances
managed by the orchestration system:

1. Hot Swap     - Zero downtime, copy JAR + reload
2. Rolling      - Sequential update with drain logic and configurable batching
3. Template     - Update template directory for future instances
4. Full Restart - Maintenance window with broadcast, parallel stop/start

Also maintains a plugin registry (registry.json) to track plugin versions,
rollout status, and per-server update state.
"""

import json
import logging
import os
import shutil
import time
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
)
logger = logging.getLogger("update_manager")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ORCHESTRATOR_URL = os.environ.get("ORCHESTRATOR_URL", "http://localhost:8080")
AUTH_TOKEN = os.environ.get("AGENT_SECRET", "change-me-in-production")
REGISTRY_PATH = os.environ.get(
    "PLUGIN_REGISTRY", "/opt/minecraft/registry.json"
)
TEMPLATE_DIR = os.environ.get("TEMPLATE_DIR", "/opt/minecraft/templates")

# ---------------------------------------------------------------------------
# Plugin Registry
# ---------------------------------------------------------------------------


@dataclass
class PluginEntry:
    """Tracks the state of a plugin across the fleet."""

    name: str
    current_version: str
    target_version: str
    jar_path: str  # Path to the plugin JAR on the orchestrator host
    rollout_status: str = "pending"  # pending | in_progress | complete
    updated_servers: List[str] = field(default_factory=list)
    pending_servers: List[str] = field(default_factory=list)
    last_updated: float = 0.0


class PluginRegistry:
    """
    Persistent registry of plugin versions and rollout state.
    Stored as JSON at REGISTRY_PATH.
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._plugins: Dict[str, PluginEntry] = {}
        self._load()

    def _load(self) -> None:
        """Load registry from disk."""
        if not os.path.exists(self._path):
            self._plugins = {}
            return
        try:
            with open(self._path, "r") as f:
                data = json.load(f)
            for name, entry in data.items():
                self._plugins[name] = PluginEntry(**entry)
        except (json.JSONDecodeError, OSError, TypeError) as exc:
            logger.warning("Could not load registry: %s", exc)
            self._plugins = {}

    def _save(self) -> None:
        """Persist registry to disk."""
        data = {name: asdict(entry) for name, entry in self._plugins.items()}
        try:
            os.makedirs(os.path.dirname(self._path) or ".", exist_ok=True)
            with open(self._path, "w") as f:
                json.dump(data, f, indent=2)
        except OSError as exc:
            logger.error("Failed to save registry: %s", exc)

    def get(self, plugin_name: str) -> Optional[PluginEntry]:
        return self._plugins.get(plugin_name)

    def register(
        self,
        name: str,
        target_version: str,
        jar_path: str,
        current_version: str = "",
        server_ids: Optional[List[str]] = None,
    ) -> PluginEntry:
        """Register or update a plugin entry for a new rollout."""
        entry = self._plugins.get(name)
        if entry:
            entry.current_version = entry.target_version or current_version
            entry.target_version = target_version
            entry.jar_path = jar_path
            entry.rollout_status = "pending"
            entry.updated_servers = []
            entry.pending_servers = server_ids or []
            entry.last_updated = time.time()
        else:
            entry = PluginEntry(
                name=name,
                current_version=current_version,
                target_version=target_version,
                jar_path=jar_path,
                rollout_status="pending",
                updated_servers=[],
                pending_servers=server_ids or [],
                last_updated=time.time(),
            )
            self._plugins[name] = entry
        self._save()
        return entry

    def mark_server_updated(self, plugin_name: str, server_id: str) -> None:
        """Record that a server has been successfully updated."""
        entry = self._plugins.get(plugin_name)
        if entry:
            if server_id in entry.pending_servers:
                entry.pending_servers.remove(server_id)
            if server_id not in entry.updated_servers:
                entry.updated_servers.append(server_id)
            if not entry.pending_servers:
                entry.rollout_status = "complete"
                entry.current_version = entry.target_version
            else:
                entry.rollout_status = "in_progress"
            self._save()

    def mark_in_progress(self, plugin_name: str) -> None:
        entry = self._plugins.get(plugin_name)
        if entry:
            entry.rollout_status = "in_progress"
            self._save()

    def get_all(self) -> Dict[str, PluginEntry]:
        return dict(self._plugins)

    def to_dict(self) -> Dict[str, Any]:
        return {name: asdict(entry) for name, entry in self._plugins.items()}


# ---------------------------------------------------------------------------
# Helper: API calls
# ---------------------------------------------------------------------------


def _auth_headers() -> Dict[str, str]:
    return {"Authorization": f"Bearer {AUTH_TOKEN}"}


def _get_active_servers() -> List[Dict[str, Any]]:
    """Fetch all running servers from the orchestrator."""
    try:
        resp = requests.get(
            f"{ORCHESTRATOR_URL}/status",
            headers=_auth_headers(),
            timeout=10,
        )
        if resp.status_code == 200:
            return resp.json().get("servers", [])
    except requests.RequestException as exc:
        logger.error("Failed to get server status: %s", exc)
    return []


def _get_node_url(node_id: str) -> Optional[str]:
    """Resolve a node_id to its agent URL via the orchestrator."""
    try:
        resp = requests.get(
            f"{ORCHESTRATOR_URL}/nodes",
            headers=_auth_headers(),
            timeout=10,
        )
        if resp.status_code == 200:
            for node in resp.json().get("nodes", []):
                if node["id"] == node_id:
                    return f"http://{node['host']}:{node['port']}"
    except requests.RequestException as exc:
        logger.error("Failed to get nodes: %s", exc)
    return None


def _update_plugin_on_node(
    node_url: str,
    container_name: str,
    plugin_path: str,
    plugin_name: str,
    restart: bool = False,
) -> bool:
    """Send a plugin update request to a node agent."""
    try:
        resp = requests.post(
            f"{node_url}/update-plugins",
            json={
                "container_name": container_name,
                "plugin_path": plugin_path,
                "plugin_name": plugin_name,
                "restart": restart,
            },
            headers=_auth_headers(),
            timeout=60,
        )
        return resp.status_code == 200
    except requests.RequestException as exc:
        logger.error("Plugin update request failed: %s", exc)
        return False


def _exec_in_container(
    node_url: str, container_name: str, command: str
) -> bool:
    """Execute a command in a container via the node agent."""
    try:
        resp = requests.post(
            f"{node_url}/exec",
            json={"container_name": container_name, "command": command},
            headers=_auth_headers(),
            timeout=30,
        )
        return resp.status_code == 200
    except requests.RequestException:
        return False


def _destroy_server(server_id: str) -> bool:
    """Destroy a server via the orchestrator."""
    try:
        resp = requests.post(
            f"{ORCHESTRATOR_URL}/destroy/{server_id}",
            headers=_auth_headers(),
            timeout=30,
        )
        return resp.status_code == 200
    except requests.RequestException:
        return False


def _create_server(gamemode: str, memory: str = "1G") -> Optional[Dict[str, Any]]:
    """Create a new server via the orchestrator."""
    try:
        resp = requests.post(
            f"{ORCHESTRATOR_URL}/create",
            json={"gamemode": gamemode, "memory": memory},
            headers=_auth_headers(),
            timeout=60,
        )
        if resp.status_code == 200:
            return resp.json()
    except requests.RequestException:
        pass
    return None


# ---------------------------------------------------------------------------
# Update Procedure A: Hot Swap (Zero Downtime)
# ---------------------------------------------------------------------------


def hot_swap(
    plugin_name: str,
    plugin_jar_path: str,
    target_version: str,
    gamemode_filter: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Hot-swap a plugin across all running servers without restarts.

    Steps:
    1. Copy plugin JAR to each running container via podman cp
    2. Trigger plugin reload via RCON command
    3. No container restart required

    Args:
        plugin_name: Name of the plugin JAR file (e.g., "MyPlugin.jar")
        plugin_jar_path: Absolute path to the new plugin JAR on the host
        target_version: Version string for tracking
        gamemode_filter: Optional gamemode to limit the update scope

    Returns:
        Summary dict with success/failure lists
    """
    registry = PluginRegistry(REGISTRY_PATH)
    servers = _get_active_servers()

    if gamemode_filter:
        servers = [s for s in servers if s.get("gamemode") == gamemode_filter]

    server_ids = [s["id"] for s in servers]
    registry.register(
        plugin_name, target_version, plugin_jar_path, server_ids=server_ids
    )
    registry.mark_in_progress(plugin_name)

    results: Dict[str, Any] = {
        "method": "hot_swap",
        "plugin": plugin_name,
        "version": target_version,
        "success": [],
        "failed": [],
    }

    for server in servers:
        if server.get("status") != "running":
            continue

        node_url = _get_node_url(server["node_id"])
        if not node_url:
            results["failed"].append(
                {"server_id": server["id"], "error": "node not found"}
            )
            continue

        # Hot swap: copy JAR + reload, no restart
        success = _update_plugin_on_node(
            node_url,
            server["container_name"],
            plugin_jar_path,
            plugin_name,
            restart=False,
        )

        if success:
            registry.mark_server_updated(plugin_name, server["id"])
            results["success"].append(server["id"])
        else:
            results["failed"].append(
                {"server_id": server["id"], "error": "update failed"}
            )

    logger.info(
        "Hot swap complete: %d success, %d failed",
        len(results["success"]),
        len(results["failed"]),
    )
    return results


# ---------------------------------------------------------------------------
# Update Procedure B: Rolling Update (Minimal Downtime)
# ---------------------------------------------------------------------------


def rolling_update(
    plugin_name: str,
    plugin_jar_path: str,
    target_version: str,
    batch_size: int = 1,
    delay_between_batches: int = 10,
    drain_timeout: int = 60,
    gamemode_filter: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Rolling update: sequentially update servers with drain logic.

    Steps per server:
    1. Set maintenance mode (stop accepting new players)
    2. Wait for server to drain (or timeout)
    3. Stop container
    4. Update plugin JAR
    5. Start container
    6. Health check before proceeding to next

    Args:
        plugin_name: Name of the plugin JAR file
        plugin_jar_path: Absolute path to the new plugin JAR
        target_version: Version string for tracking
        batch_size: Number of servers to update simultaneously
        delay_between_batches: Seconds to wait between batches
        drain_timeout: Max seconds to wait for players to leave
        gamemode_filter: Optional gamemode to limit scope

    Returns:
        Summary dict with success/failure lists
    """
    registry = PluginRegistry(REGISTRY_PATH)
    servers = _get_active_servers()

    if gamemode_filter:
        servers = [s for s in servers if s.get("gamemode") == gamemode_filter]

    server_ids = [s["id"] for s in servers]
    registry.register(
        plugin_name, target_version, plugin_jar_path, server_ids=server_ids
    )
    registry.mark_in_progress(plugin_name)

    results: Dict[str, Any] = {
        "method": "rolling_update",
        "plugin": plugin_name,
        "version": target_version,
        "updated": [],
        "failed": [],
        "total": len(servers),
    }

    for i in range(0, len(servers), batch_size):
        batch = servers[i : i + batch_size]

        for server in batch:
            server_id = server["id"]
            node_url = _get_node_url(server["node_id"])

            if not node_url:
                results["failed"].append(
                    {"server_id": server_id, "error": "node not found"}
                )
                continue

            # Step 1: Broadcast maintenance warning
            _exec_in_container(
                node_url,
                server["container_name"],
                "rcon-cli say Server updating in 30 seconds...",
            )

            # Step 2: Drain - wait for players to leave
            logger.info("Draining server %s (timeout %ds)", server_id, drain_timeout)
            drain_start = time.time()
            drained = False
            while time.time() - drain_start < drain_timeout:
                # Re-check player count
                current_servers = _get_active_servers()
                current = next(
                    (s for s in current_servers if s["id"] == server_id), None
                )
                if current and current.get("player_count", 0) == 0:
                    drained = True
                    break
                time.sleep(5)

            if not drained:
                # Kick remaining players after timeout
                _exec_in_container(
                    node_url,
                    server["container_name"],
                    "rcon-cli kick @a Server updating, please reconnect shortly",
                )
                time.sleep(2)

            # Step 3: Stop, update, and restart via node agent
            success = _update_plugin_on_node(
                node_url,
                server["container_name"],
                plugin_jar_path,
                plugin_name,
                restart=True,
            )

            if success:
                # Step 4: Health check - wait for container to be back up
                healthy = False
                for _ in range(12):  # ~60s timeout
                    time.sleep(5)
                    try:
                        resp = requests.get(
                            f"{node_url}/container/{server['container_name']}",
                            headers=_auth_headers(),
                            timeout=5,
                        )
                        if resp.status_code == 200:
                            info = resp.json()
                            state = info.get("State", {})
                            if state.get("Running", False):
                                healthy = True
                                break
                    except requests.RequestException:
                        pass

                if healthy:
                    registry.mark_server_updated(plugin_name, server_id)
                    results["updated"].append(server_id)
                    logger.info("Server %s updated successfully", server_id)
                else:
                    results["failed"].append(
                        {"server_id": server_id, "error": "health check failed"}
                    )
                    logger.error("Server %s failed health check", server_id)
            else:
                results["failed"].append(
                    {"server_id": server_id, "error": "update failed"}
                )

        # Delay between batches
        if i + batch_size < len(servers):
            logger.info("Waiting %ds before next batch...", delay_between_batches)
            time.sleep(delay_between_batches)

    logger.info(
        "Rolling update complete: %d updated, %d failed",
        len(results["updated"]),
        len(results["failed"]),
    )
    return results


# ---------------------------------------------------------------------------
# Update Procedure C: Template Update
# ---------------------------------------------------------------------------


def template_update(
    gamemode: str,
    plugin_name: str,
    plugin_jar_path: str,
    target_version: str,
) -> Dict[str, Any]:
    """
    Update the template directory so new instances get the updated plugin.

    Existing running instances are NOT affected — only newly created servers
    will use the updated template.

    Args:
        gamemode: The gamemode template to update (e.g., "bedwars")
        plugin_name: Name of the plugin JAR file
        plugin_jar_path: Absolute path to the new plugin JAR
        target_version: Version string for tracking

    Returns:
        Summary dict
    """
    registry = PluginRegistry(REGISTRY_PATH)
    template_path = os.path.join(TEMPLATE_DIR, gamemode, "plugins")

    # Ensure template plugins directory exists
    os.makedirs(template_path, exist_ok=True)

    dest = os.path.join(template_path, plugin_name)

    try:
        shutil.copy2(plugin_jar_path, dest)
    except (OSError, shutil.Error) as exc:
        logger.error("Failed to copy plugin to template: %s", exc)
        return {
            "method": "template_update",
            "plugin": plugin_name,
            "version": target_version,
            "gamemode": gamemode,
            "status": "failed",
            "error": str(exc),
        }

    # Update registry (no servers to track — applies to future instances)
    registry.register(plugin_name, target_version, plugin_jar_path)

    logger.info(
        "Template updated: %s/%s -> %s v%s",
        gamemode,
        plugin_name,
        plugin_name,
        target_version,
    )

    return {
        "method": "template_update",
        "plugin": plugin_name,
        "version": target_version,
        "gamemode": gamemode,
        "template_path": dest,
        "status": "complete",
        "note": "Only new instances will use this version. Existing instances are unaffected.",
    }


# ---------------------------------------------------------------------------
# Update Procedure D: Full Restart (Maintenance Window)
# ---------------------------------------------------------------------------


def full_restart_update(
    plugin_name: str,
    plugin_jar_path: str,
    target_version: str,
    warning_seconds: int = 60,
    gamemode_filter: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Full restart update during a maintenance window.

    Steps:
    1. Broadcast warnings to all servers
    2. Enable maintenance mode (block new server creation)
    3. Stop all servers in parallel
    4. Update plugin on each server sequentially
    5. Start all servers in parallel
    6. Verify each server is healthy
    7. Disable maintenance mode

    Args:
        plugin_name: Name of the plugin JAR file
        plugin_jar_path: Absolute path to the new plugin JAR
        target_version: Version string for tracking
        warning_seconds: Seconds of warning before shutdown
        gamemode_filter: Optional gamemode to limit scope

    Returns:
        Summary dict with detailed results
    """
    registry = PluginRegistry(REGISTRY_PATH)

    results: Dict[str, Any] = {
        "method": "full_restart",
        "plugin": plugin_name,
        "version": target_version,
        "phases": {},
    }

    # Phase 1: Broadcast warnings
    logger.info("Phase 1: Broadcasting maintenance warnings")
    servers = _get_active_servers()
    if gamemode_filter:
        servers = [s for s in servers if s.get("gamemode") == gamemode_filter]

    server_ids = [s["id"] for s in servers]
    registry.register(
        plugin_name, target_version, plugin_jar_path, server_ids=server_ids
    )
    registry.mark_in_progress(plugin_name)

    for server in servers:
        node_url = _get_node_url(server["node_id"])
        if node_url:
            _exec_in_container(
                node_url,
                server["container_name"],
                f"rcon-cli say MAINTENANCE: Server restarting in {warning_seconds} seconds!",
            )

    # Wait for warning period
    if warning_seconds > 30:
        time.sleep(warning_seconds - 30)
        for server in servers:
            node_url = _get_node_url(server["node_id"])
            if node_url:
                _exec_in_container(
                    node_url,
                    server["container_name"],
                    "rcon-cli say MAINTENANCE: Server restarting in 30 seconds!",
                )
        time.sleep(25)
        for server in servers:
            node_url = _get_node_url(server["node_id"])
            if node_url:
                _exec_in_container(
                    node_url,
                    server["container_name"],
                    "rcon-cli say MAINTENANCE: Restarting in 5 seconds!",
                )
        time.sleep(5)
    else:
        time.sleep(warning_seconds)

    results["phases"]["warning"] = "complete"

    # Phase 2: Enable maintenance mode
    logger.info("Phase 2: Enabling maintenance mode")
    try:
        requests.post(
            f"{ORCHESTRATOR_URL}/shutdown-all",
            headers=_auth_headers(),
            timeout=120,
        )
    except requests.RequestException as exc:
        logger.error("Failed to enable maintenance mode: %s", exc)

    results["phases"]["shutdown"] = "complete"

    # Phase 3: Update plugins on each node
    logger.info("Phase 3: Updating plugins")
    update_results = {"updated": [], "failed": []}

    for server in servers:
        node_url = _get_node_url(server["node_id"])
        if not node_url:
            update_results["failed"].append(
                {"server_id": server["id"], "error": "node not found"}
            )
            continue

        success = _update_plugin_on_node(
            node_url,
            server["container_name"],
            plugin_jar_path,
            plugin_name,
            restart=False,  # Already stopped
        )

        if success:
            registry.mark_server_updated(plugin_name, server["id"])
            update_results["updated"].append(server["id"])
        else:
            update_results["failed"].append(
                {"server_id": server["id"], "error": "update failed"}
            )

    results["phases"]["update"] = update_results

    # Phase 4: Start all servers
    logger.info("Phase 4: Starting all servers")
    try:
        resp = requests.post(
            f"{ORCHESTRATOR_URL}/start-all",
            headers=_auth_headers(),
            timeout=120,
        )
        if resp.status_code == 200:
            results["phases"]["start"] = resp.json()
        else:
            results["phases"]["start"] = {"error": resp.text}
    except requests.RequestException as exc:
        results["phases"]["start"] = {"error": str(exc)}

    # Phase 5: Verification
    logger.info("Phase 5: Verifying servers")
    time.sleep(15)  # Give servers time to boot
    verification = {"healthy": [], "unhealthy": []}

    for server in servers:
        node_url = _get_node_url(server["node_id"])
        if not node_url:
            verification["unhealthy"].append(server["id"])
            continue

        try:
            resp = requests.get(
                f"{node_url}/container/{server['container_name']}",
                headers=_auth_headers(),
                timeout=10,
            )
            if resp.status_code == 200:
                info = resp.json()
                state = info.get("State", {})
                if state.get("Running", False):
                    verification["healthy"].append(server["id"])
                else:
                    verification["unhealthy"].append(server["id"])
            else:
                verification["unhealthy"].append(server["id"])
        except requests.RequestException:
            verification["unhealthy"].append(server["id"])

    results["phases"]["verification"] = verification

    logger.info(
        "Full restart update complete: %d healthy, %d unhealthy",
        len(verification["healthy"]),
        len(verification["unhealthy"]),
    )
    return results


# ---------------------------------------------------------------------------
# CLI Interface
# ---------------------------------------------------------------------------


def main() -> None:
    """
    CLI for running update procedures directly.

    Usage:
        python3 update_manager.py hot-swap <plugin_name> <jar_path> <version> [gamemode]
        python3 update_manager.py rolling <plugin_name> <jar_path> <version> [batch_size] [gamemode]
        python3 update_manager.py template <gamemode> <plugin_name> <jar_path> <version>
        python3 update_manager.py full-restart <plugin_name> <jar_path> <version> [warning_secs] [gamemode]
        python3 update_manager.py status
    """
    import sys

    if len(sys.argv) < 2:
        print(__doc__)
        print(main.__doc__)
        sys.exit(1)

    command = sys.argv[1]

    if command == "status":
        registry = PluginRegistry(REGISTRY_PATH)
        print(json.dumps(registry.to_dict(), indent=2))
        return

    if command == "hot-swap":
        if len(sys.argv) < 5:
            print("Usage: update_manager.py hot-swap <plugin_name> <jar_path> <version> [gamemode]")
            sys.exit(1)
        result = hot_swap(
            plugin_name=sys.argv[2],
            plugin_jar_path=sys.argv[3],
            target_version=sys.argv[4],
            gamemode_filter=sys.argv[5] if len(sys.argv) > 5 else None,
        )

    elif command == "rolling":
        if len(sys.argv) < 5:
            print(
                "Usage: update_manager.py rolling <plugin_name> <jar_path> <version> [batch_size] [gamemode]"
            )
            sys.exit(1)
        result = rolling_update(
            plugin_name=sys.argv[2],
            plugin_jar_path=sys.argv[3],
            target_version=sys.argv[4],
            batch_size=int(sys.argv[5]) if len(sys.argv) > 5 else 1,
            gamemode_filter=sys.argv[6] if len(sys.argv) > 6 else None,
        )

    elif command == "template":
        if len(sys.argv) < 6:
            print(
                "Usage: update_manager.py template <gamemode> <plugin_name> <jar_path> <version>"
            )
            sys.exit(1)
        result = template_update(
            gamemode=sys.argv[2],
            plugin_name=sys.argv[3],
            plugin_jar_path=sys.argv[4],
            target_version=sys.argv[5],
        )

    elif command == "full-restart":
        if len(sys.argv) < 5:
            print(
                "Usage: update_manager.py full-restart <plugin_name> <jar_path> <version> [warning_secs] [gamemode]"
            )
            sys.exit(1)
        result = full_restart_update(
            plugin_name=sys.argv[2],
            plugin_jar_path=sys.argv[3],
            target_version=sys.argv[4],
            warning_seconds=int(sys.argv[5]) if len(sys.argv) > 5 else 60,
            gamemode_filter=sys.argv[6] if len(sys.argv) > 6 else None,
        )

    else:
        print(f"Unknown command: {command}")
        print(main.__doc__)
        sys.exit(1)

    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
