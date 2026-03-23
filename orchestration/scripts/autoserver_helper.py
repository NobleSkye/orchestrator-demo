#!/usr/bin/env python3
"""
AutoServer Helper Script

Bridge between the Velocity AutoServer plugin and the Orchestrator API.
Called by AutoServer when a player requests a game server that needs
to be dynamically provisioned or torn down.

Usage:
    python3 autoserver_helper.py start <gamemode>
    python3 autoserver_helper.py stop <server_id>

Exit codes:
    0 - Success
    1 - Failure
"""

import os
import sys
import time

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
ORCHESTRATOR_URL = os.environ.get("ORCHESTRATOR_URL", "http://localhost:8080")
AUTH_TOKEN = os.environ.get("AGENT_SECRET", "change-me-in-production")
ADDR_DIR = os.environ.get("ADDR_DIR", "/tmp")

# How long to wait for the server to become available (seconds)
STARTUP_WAIT_TIMEOUT = int(os.environ.get("STARTUP_WAIT_TIMEOUT", "120"))
STARTUP_POLL_INTERVAL = 2


def log(message: str) -> None:
    """Print a timestamped log message to stderr."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] autoserver_helper: {message}", file=sys.stderr)


def start_server(gamemode: str) -> int:
    """
    Request the Orchestrator to provision a new server for the given gamemode.

    On success, writes the server address to a temp file that AutoServer
    can read to route the player.

    Returns:
        0 on success, 1 on failure
    """
    log(f"Requesting new {gamemode} server...")

    try:
        resp = requests.post(
            f"{ORCHESTRATOR_URL}/create",
            json={"gamemode": gamemode},
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
            timeout=60,
        )
    except requests.RequestException as exc:
        log(f"Failed to contact orchestrator: {exc}")
        return 1

    if resp.status_code != 200:
        log(f"Orchestrator returned error {resp.status_code}: {resp.text}")
        return 1

    data = resp.json()
    server_id = data.get("server_id")
    address = data.get("address")

    if not server_id or not address:
        log(f"Invalid response from orchestrator: {data}")
        return 1

    # Write the address file for AutoServer to pick up
    addr_file = os.path.join(ADDR_DIR, f"autoserver-{server_id}.addr")
    try:
        with open(addr_file, "w") as f:
            f.write(address)
    except OSError as exc:
        log(f"Failed to write address file {addr_file}: {exc}")
        return 1

    # Also write a generic gamemode address file for AutoServer's addressFile config
    generic_addr_file = os.path.join(ADDR_DIR, f"autoserver-{gamemode}.addr")
    try:
        with open(generic_addr_file, "w") as f:
            f.write(address)
    except OSError as exc:
        log(f"Warning: could not write generic address file: {exc}")

    log(f"Server {server_id} provisioned at {address}")
    log(f"Address written to {addr_file}")

    # Print the address to stdout for AutoServer to capture
    print(address)
    return 0


def stop_server(server_id: str) -> int:
    """
    Request the Orchestrator to destroy a running server.

    Returns:
        0 on success, 1 on failure
    """
    log(f"Requesting destruction of server {server_id}...")

    try:
        resp = requests.post(
            f"{ORCHESTRATOR_URL}/destroy/{server_id}",
            headers={"Authorization": f"Bearer {AUTH_TOKEN}"},
            timeout=30,
        )
    except requests.RequestException as exc:
        log(f"Failed to contact orchestrator: {exc}")
        return 1

    if resp.status_code == 200:
        log(f"Server {server_id} destroyed successfully")
        # Clean up address file
        addr_file = os.path.join(ADDR_DIR, f"autoserver-{server_id}.addr")
        try:
            os.remove(addr_file)
        except OSError:
            pass
        return 0
    elif resp.status_code == 404:
        log(f"Server {server_id} not found (may already be destroyed)")
        return 0
    else:
        log(f"Orchestrator returned error {resp.status_code}: {resp.text}")
        return 1


def print_usage() -> None:
    """Print usage information."""
    print("Usage:", file=sys.stderr)
    print("  autoserver_helper.py start <gamemode>", file=sys.stderr)
    print("  autoserver_helper.py stop <server_id>", file=sys.stderr)


def main() -> int:
    if len(sys.argv) < 3:
        print_usage()
        return 1

    action = sys.argv[1].lower()
    target = sys.argv[2]

    if action == "start":
        return start_server(target)
    elif action == "stop":
        return stop_server(target)
    else:
        log(f"Unknown action: {action}")
        print_usage()
        return 1


if __name__ == "__main__":
    sys.exit(main())
