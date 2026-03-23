#!/usr/bin/env python3
"""
Minecraft Node Agent Service

Per-node service that manages Podman/Docker containers for Minecraft server
instances. Communicates with the central Orchestrator and handles local
container lifecycle, port allocation, template provisioning, and plugin updates.

Runs on each minigame node (Port 5000).
"""

import json
import logging
import os
import shutil
import subprocess
import threading
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from flask import Flask, jsonify, request

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","module":"%(module)s","message":"%(message)s"}',
)
logger = logging.getLogger("node_agent")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
AGENT_SECRET = os.environ.get("AGENT_SECRET", "change-me-in-production")
ORCHESTRATOR_URL = os.environ.get("ORCHESTRATOR_URL", "http://10.0.0.1:8080")
MAX_SERVERS = int(os.environ.get("MAX_SERVERS", "10"))
BASE_PORT = int(os.environ.get("BASE_PORT", "25566"))
MAX_PORT = int(os.environ.get("MAX_PORT", "25600"))
DATA_DIR = os.environ.get("DATA_DIR", "/opt/minecraft/instances")
TEMPLATE_DIR = os.environ.get("TEMPLATE_DIR", "/opt/minecraft/templates")
CONTAINER_IMAGE = os.environ.get("CONTAINER_IMAGE", "itzg/minecraft-server")
CONTAINER_RUNTIME = os.environ.get("CONTAINER_RUNTIME", "podman")  # or "docker"
HOST = os.environ.get("NODE_HOST", "0.0.0.0")
PORT = int(os.environ.get("NODE_PORT", "5000"))

# Maximum restart attempts for crashed containers
MAX_RESTART_ATTEMPTS = 3

# ---------------------------------------------------------------------------
# Port Allocator
# ---------------------------------------------------------------------------


class PortAllocator:
    """
    Manages dynamic port allocation within a configured range.
    Thread-safe with conflict detection against both our tracking and the OS.
    """

    def __init__(self, base_port: int, max_port: int) -> None:
        self._base = base_port
        self._max = max_port
        self._lock = threading.Lock()
        self._allocated: Set[int] = set()

    def allocate(self) -> Optional[int]:
        """Find and reserve the next available port. Returns None if exhausted."""
        with self._lock:
            for port in range(self._base, self._max + 1):
                if port not in self._allocated and not self._is_port_in_use(port):
                    self._allocated.add(port)
                    return port
        return None

    def release(self, port: int) -> None:
        """Release a previously allocated port."""
        with self._lock:
            self._allocated.discard(port)

    def is_available(self, port: int) -> bool:
        """Check if a specific port is available."""
        with self._lock:
            return port not in self._allocated and not self._is_port_in_use(port)

    @property
    def allocated_count(self) -> int:
        with self._lock:
            return len(self._allocated)

    @staticmethod
    def _is_port_in_use(port: int) -> bool:
        """Check if a port is already bound on the system."""
        import socket

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            try:
                s.bind(("0.0.0.0", port))
                return False
            except OSError:
                return True


# ---------------------------------------------------------------------------
# Container Manager
# ---------------------------------------------------------------------------


class ContainerManager:
    """
    Manages Podman/Docker containers for Minecraft server instances.
    Handles creation, destruction, inspection, and command execution.
    """

    def __init__(self, runtime: str = "podman") -> None:
        self._runtime = runtime
        self._lock = threading.Lock()
        # Track restart attempts per container
        self._restart_counts: Dict[str, int] = {}

    def _run(self, args: List[str], timeout: int = 60) -> Tuple[int, str, str]:
        """Execute a container runtime command. Returns (returncode, stdout, stderr)."""
        cmd = [self._runtime] + args
        logger.info("Running: %s", " ".join(cmd))
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            return result.returncode, result.stdout.strip(), result.stderr.strip()
        except subprocess.TimeoutExpired:
            return -1, "", "Command timed out"
        except FileNotFoundError:
            return -1, "", f"{self._runtime} not found"

    def container_exists(self, name: str) -> bool:
        """Check if a container with the given name already exists."""
        rc, stdout, _ = self._run(["ps", "-a", "--format", "{{.Names}}"])
        if rc != 0:
            return False
        return name in stdout.split("\n")

    def create_container(
        self,
        container_name: str,
        server_id: str,
        gamemode: str,
        port: int,
        instance_path: str,
        memory: str = "1G",
    ) -> bool:
        """
        Create and start a Minecraft server container.

        Uses host networking with the allocated external port mapped to the
        container's internal port 25565. Mounts the instance directory as /data.
        """
        with self._lock:
            # Idempotency: if container already exists, return success
            if self.container_exists(container_name):
                logger.info(
                    "Container %s already exists, skipping creation",
                    container_name,
                )
                return True

            timestamp = str(int(time.time()))

            args = [
                "run",
                "-d",
                "--name",
                container_name,
                "--network",
                "host",
                "-v",
                f"{instance_path}:/data",
                "-e",
                "EULA=TRUE",
                "-e",
                "TYPE=PAPER",
                "-e",
                "ONLINE_MODE=false",
                "-e",
                f"MEMORY={memory}",
                "-e",
                f"SERVER_PORT={port}",
                "--label",
                "managed_by=orchestrator",
                "--label",
                f"gamemode={gamemode}",
                "--label",
                f"server_id={server_id}",
                "--label",
                f"created_at={timestamp}",
                "--restart",
                "no",
                CONTAINER_IMAGE,
            ]

            rc, stdout, stderr = self._run(args, timeout=120)
            if rc != 0:
                logger.error(
                    "Failed to create container %s: %s", container_name, stderr
                )
                return False

            logger.info("Created container %s (id: %s)", container_name, stdout[:12])
            return True

    def stop_container(self, name: str, timeout: int = 30) -> bool:
        """Stop a running container gracefully."""
        rc, _, stderr = self._run(["stop", "-t", str(timeout), name])
        if rc != 0:
            logger.error("Failed to stop container %s: %s", name, stderr)
            return False
        return True

    def remove_container(self, name: str, force: bool = False) -> bool:
        """Remove a container (optionally force-remove if still running)."""
        args = ["rm"]
        if force:
            args.append("-f")
        args.append(name)
        rc, _, stderr = self._run(args)
        if rc != 0:
            logger.error("Failed to remove container %s: %s", name, stderr)
            return False
        return True

    def inspect_container(self, name: str) -> Optional[Dict[str, Any]]:
        """Get detailed container information as JSON."""
        rc, stdout, stderr = self._run(["inspect", name])
        if rc != 0:
            return None
        try:
            data = json.loads(stdout)
            if isinstance(data, list) and data:
                return data[0]
            return data
        except json.JSONDecodeError:
            logger.error("Failed to parse inspect output for %s", name)
            return None

    def is_running(self, name: str) -> bool:
        """Check if a container is currently running."""
        info = self.inspect_container(name)
        if info is None:
            return False
        state = info.get("State", {})
        return state.get("Running", False) or state.get("Status") == "running"

    def exec_in_container(
        self, name: str, command: List[str], timeout: int = 30
    ) -> Tuple[int, str]:
        """Execute a command inside a running container."""
        args = ["exec", name] + command
        rc, stdout, stderr = self._run(args, timeout=timeout)
        return rc, stdout if rc == 0 else stderr

    def copy_to_container(self, name: str, src: str, dest: str) -> bool:
        """Copy a file from the host into a running container."""
        rc, _, stderr = self._run(["cp", src, f"{name}:{dest}"])
        if rc != 0:
            logger.error("Failed to copy %s to %s:%s: %s", src, name, dest, stderr)
            return False
        return True

    def list_managed_containers(self) -> List[Dict[str, str]]:
        """List all containers managed by the orchestrator."""
        rc, stdout, _ = self._run(
            [
                "ps",
                "-a",
                "--filter",
                "label=managed_by=orchestrator",
                "--format",
                "{{.Names}}\t{{.Status}}\t{{.Labels}}",
            ]
        )
        if rc != 0 or not stdout:
            return []
        containers = []
        for line in stdout.split("\n"):
            parts = line.split("\t")
            if len(parts) >= 2:
                containers.append(
                    {"name": parts[0], "status": parts[1], "labels": parts[2] if len(parts) > 2 else ""}
                )
        return containers


# ---------------------------------------------------------------------------
# Template Manager
# ---------------------------------------------------------------------------


class TemplateManager:
    """
    Manages server templates. Copies template files to instance directories
    so each server starts with the correct plugins and configuration.
    """

    def __init__(self, template_dir: str, data_dir: str) -> None:
        self._template_dir = template_dir
        self._data_dir = data_dir

    def provision_instance(self, server_id: str, gamemode: str) -> str:
        """
        Create an instance directory from a template.
        Returns the absolute path to the instance directory.
        """
        instance_path = os.path.join(self._data_dir, server_id)
        template_path = os.path.join(self._template_dir, gamemode)

        # Create instance directory
        os.makedirs(instance_path, exist_ok=True)

        # Copy template if it exists
        if os.path.isdir(template_path):
            logger.info(
                "Copying template %s to instance %s", gamemode, server_id
            )
            for item in os.listdir(template_path):
                src = os.path.join(template_path, item)
                dst = os.path.join(instance_path, item)
                if os.path.isdir(src):
                    shutil.copytree(src, dst, dirs_exist_ok=True)
                else:
                    shutil.copy2(src, dst)
        else:
            logger.warning(
                "No template found for gamemode %s at %s",
                gamemode,
                template_path,
            )
            # Ensure plugins directory exists even without a template
            os.makedirs(os.path.join(instance_path, "plugins"), exist_ok=True)

        return instance_path

    def cleanup_instance(self, server_id: str) -> None:
        """Remove an instance directory and all its contents."""
        instance_path = os.path.join(self._data_dir, server_id)
        if os.path.isdir(instance_path):
            shutil.rmtree(instance_path, ignore_errors=True)
            logger.info("Cleaned up instance directory %s", instance_path)


# ---------------------------------------------------------------------------
# Crash Monitor Background Thread
# ---------------------------------------------------------------------------


def crash_monitor(
    container_mgr: ContainerManager,
    port_allocator: PortAllocator,
    interval: int = 30,
) -> None:
    """
    Periodically check managed containers for crashes and attempt restart.
    Maximum of MAX_RESTART_ATTEMPTS per container.
    """
    while True:
        for container in container_mgr.list_managed_containers():
            name = container["name"]
            status = container.get("status", "").lower()

            if "exited" in status or "dead" in status:
                attempts = container_mgr._restart_counts.get(name, 0)
                if attempts < MAX_RESTART_ATTEMPTS:
                    logger.warning(
                        "Container %s crashed (attempt %d/%d), restarting",
                        name,
                        attempts + 1,
                        MAX_RESTART_ATTEMPTS,
                    )
                    rc, _, _ = container_mgr._run(["start", name])
                    container_mgr._restart_counts[name] = attempts + 1
                else:
                    logger.error(
                        "Container %s exceeded max restart attempts (%d)",
                        name,
                        MAX_RESTART_ATTEMPTS,
                    )
        time.sleep(interval)


# ---------------------------------------------------------------------------
# Flask Application
# ---------------------------------------------------------------------------


def create_app() -> Flask:
    app = Flask(__name__)

    port_allocator = PortAllocator(BASE_PORT, MAX_PORT)
    container_mgr = ContainerManager(CONTAINER_RUNTIME)
    template_mgr = TemplateManager(TEMPLATE_DIR, DATA_DIR)

    # Ensure data directory exists
    os.makedirs(DATA_DIR, exist_ok=True)

    # -----------------------------------------------------------------------
    # Auth middleware
    # -----------------------------------------------------------------------
    @app.before_request
    def check_auth():
        if request.path == "/health":
            return None
        token = request.headers.get("Authorization", "")
        if token != f"Bearer {AGENT_SECRET}" and token != AGENT_SECRET:
            return jsonify({"error": "Unauthorized"}), 401
        return None

    # -----------------------------------------------------------------------
    # Health
    # -----------------------------------------------------------------------
    @app.route("/health", methods=["GET"])
    def health():
        return jsonify({"status": "ok"})

    # -----------------------------------------------------------------------
    # POST /create - Create a container with given spec
    # -----------------------------------------------------------------------
    @app.route("/create", methods=["POST"])
    def create():
        data = request.get_json(silent=True) or {}
        server_id = data.get("server_id")
        gamemode = data.get("gamemode")
        container_name = data.get("container_name")
        memory = data.get("memory", "1G")

        if not all([server_id, gamemode, container_name]):
            return (
                jsonify(
                    {
                        "error": "Missing required fields: server_id, gamemode, container_name"
                    }
                ),
                400,
            )

        # Check capacity
        if port_allocator.allocated_count >= MAX_SERVERS:
            return jsonify({"error": "Node at maximum capacity"}), 503

        # Allocate port
        port = port_allocator.allocate()
        if port is None:
            return jsonify({"error": "No available ports"}), 503

        # Provision instance from template
        instance_path = template_mgr.provision_instance(server_id, gamemode)

        # Create and start container
        success = container_mgr.create_container(
            container_name=container_name,
            server_id=server_id,
            gamemode=gamemode,
            port=port,
            instance_path=instance_path,
            memory=memory,
        )

        if not success:
            port_allocator.release(port)
            template_mgr.cleanup_instance(server_id)
            return jsonify({"error": "Failed to create container"}), 500

        logger.info(
            "Created server %s (%s) on port %d", server_id, gamemode, port
        )
        return jsonify(
            {
                "server_id": server_id,
                "container_name": container_name,
                "port": port,
                "instance_path": instance_path,
                "status": "running",
            }
        )

    # -----------------------------------------------------------------------
    # POST /destroy - Stop and remove a container
    # -----------------------------------------------------------------------
    @app.route("/destroy", methods=["POST"])
    def destroy():
        data = request.get_json(silent=True) or {}
        container_name = data.get("container_name")
        server_id = data.get("server_id")

        if not container_name:
            return jsonify({"error": "Missing required field: container_name"}), 400

        # Get port from container inspect before removing
        port_to_release = None
        info = container_mgr.inspect_container(container_name)
        if info:
            # Try to extract port from environment variables
            env_vars = info.get("Config", {}).get("Env", [])
            for env in env_vars:
                if env.startswith("SERVER_PORT="):
                    try:
                        port_to_release = int(env.split("=", 1)[1])
                    except ValueError:
                        pass

        # Stop and remove container
        if container_mgr.container_exists(container_name):
            container_mgr.stop_container(container_name)
            container_mgr.remove_container(container_name, force=True)

        # Release port
        if port_to_release:
            port_allocator.release(port_to_release)

        # Cleanup instance directory
        if server_id:
            template_mgr.cleanup_instance(server_id)

        # Clean up restart tracking
        container_mgr._restart_counts.pop(container_name, None)

        logger.info("Destroyed container %s (server %s)", container_name, server_id)
        return jsonify({"status": "destroyed", "container_name": container_name})

    # -----------------------------------------------------------------------
    # GET /status - Current capacity and load
    # -----------------------------------------------------------------------
    @app.route("/status", methods=["GET"])
    def status():
        containers = container_mgr.list_managed_containers()
        running = sum(1 for c in containers if "up" in c.get("status", "").lower())
        return jsonify(
            {
                "max_servers": MAX_SERVERS,
                "current_servers": len(containers),
                "running": running,
                "allocated_ports": port_allocator.allocated_count,
                "containers": containers,
            }
        )

    # -----------------------------------------------------------------------
    # GET /container/<name> - Container inspect info
    # -----------------------------------------------------------------------
    @app.route("/container/<name>", methods=["GET"])
    def container_info(name: str):
        info = container_mgr.inspect_container(name)
        if info is None:
            return jsonify({"error": f"Container {name} not found"}), 404
        return jsonify(info)

    # -----------------------------------------------------------------------
    # POST /update-plugins - Install/update JAR in running container
    # -----------------------------------------------------------------------
    @app.route("/update-plugins", methods=["POST"])
    def update_plugins():
        data = request.get_json(silent=True) or {}
        container_name = data.get("container_name")
        plugin_path = data.get("plugin_path")
        plugin_name = data.get("plugin_name")
        restart = data.get("restart", False)

        if not all([container_name, plugin_path, plugin_name]):
            return (
                jsonify(
                    {
                        "error": "Missing required fields: container_name, plugin_path, plugin_name"
                    }
                ),
                400,
            )

        # Validate plugin file exists on host
        if not os.path.isfile(plugin_path):
            return jsonify({"error": f"Plugin file not found: {plugin_path}"}), 404

        # Copy plugin into container
        dest = f"/data/plugins/{plugin_name}"
        success = container_mgr.copy_to_container(container_name, plugin_path, dest)
        if not success:
            return jsonify({"error": "Failed to copy plugin to container"}), 500

        if restart:
            # Full restart: stop and start the container
            container_mgr.stop_container(container_name)
            rc, _, _ = container_mgr._run(["start", container_name])
            if rc != 0:
                return jsonify({"error": "Failed to restart container"}), 500
            logger.info(
                "Plugin %s updated with restart in %s",
                plugin_name,
                container_name,
            )
        else:
            # Hot reload attempt via RCON (best-effort)
            container_mgr.exec_in_container(
                container_name, ["rcon-cli", "reload", "confirm"]
            )
            logger.info(
                "Plugin %s hot-swapped in %s", plugin_name, container_name
            )

        return jsonify(
            {
                "status": "updated",
                "plugin": plugin_name,
                "container": container_name,
                "restarted": restart,
            }
        )

    # -----------------------------------------------------------------------
    # GET /free-port - Return an available port
    # -----------------------------------------------------------------------
    @app.route("/free-port", methods=["GET"])
    def free_port():
        port = port_allocator.allocate()
        if port is None:
            return jsonify({"error": "No available ports"}), 503
        # Release immediately — this is just a query, actual allocation
        # happens during /create
        port_allocator.release(port)
        return jsonify({"port": port})

    # -----------------------------------------------------------------------
    # POST /exec - Execute command in container
    # -----------------------------------------------------------------------
    @app.route("/exec", methods=["POST"])
    def exec_cmd():
        data = request.get_json(silent=True) or {}
        container_name = data.get("container_name")
        command = data.get("command")

        if not container_name or not command:
            return (
                jsonify(
                    {"error": "Missing required fields: container_name, command"}
                ),
                400,
            )

        if isinstance(command, str):
            command = command.split()

        if not container_mgr.is_running(container_name):
            return (
                jsonify({"error": f"Container {container_name} is not running"}),
                400,
            )

        rc, output = container_mgr.exec_in_container(container_name, command)
        return jsonify(
            {"returncode": rc, "output": output, "container": container_name}
        )

    # -----------------------------------------------------------------------
    # Start crash monitor background thread
    # -----------------------------------------------------------------------
    monitor_thread = threading.Thread(
        target=crash_monitor,
        args=(container_mgr, port_allocator),
        daemon=True,
    )
    monitor_thread.start()

    return app


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

app = create_app()

if __name__ == "__main__":
    logger.info("Starting Node Agent on %s:%d", HOST, PORT)
    app.run(host=HOST, port=PORT, threaded=True)
