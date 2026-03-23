# Testing Guide — Minecraft Server Orchestration System

This document contains `curl` commands to verify every API endpoint across both the **Orchestrator** (port 8080) and **Node Agent** (port 5000) services.

> **Prerequisites**
> 1. Set the auth token: `export TOKEN="your-agent-secret"`
> 2. Orchestrator running at `http://localhost:8080`
> 3. At least one Node Agent running at `http://localhost:5000`

---

## Orchestrator Endpoints (`localhost:8080`)

### Health Check (no auth required)

```bash
curl -s http://localhost:8080/health | python3 -m json.tool
```

Expected: `{"status": "ok", "maintenance": false}`

---

### List Nodes

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/nodes | python3 -m json.tool
```

Expected: JSON with `nodes` array showing each registered node, its capacity, and health status.

---

### Create a Server

```bash
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"gamemode": "bedwars", "memory": "1G"}' \
  http://localhost:8080/create | python3 -m json.tool
```

Expected:
```json
{
  "server_id": "bedwars-a1b2c3d4",
  "node_id": "node-1",
  "address": "10.0.0.2:25566",
  "port": 25566,
  "container_name": "mc-bedwars-a1b2c3d4",
  "status": "running"
}
```

Save the `server_id` for subsequent commands:
```bash
export SERVER_ID="bedwars-a1b2c3d4"
```

---

### Create a Server on a Specific Node

```bash
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"gamemode": "skywars", "memory": "2G", "node_id": "node-2"}' \
  http://localhost:8080/create | python3 -m json.tool
```

---

### List All Active Servers

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/status | python3 -m json.tool
```

Expected: JSON with `total` count and `servers` array.

---

### Destroy a Server

```bash
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/destroy/$SERVER_ID | python3 -m json.tool
```

Expected: `{"status": "destroyed", "server_id": "bedwars-a1b2c3d4"}`

---

### Update All Plugins (Hot Swap)

```bash
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "plugin_path": "/opt/minecraft/plugins/MyPlugin-1.2.jar",
    "plugin_name": "MyPlugin.jar",
    "restart": false
  }' \
  http://localhost:8080/update-all-plugins | python3 -m json.tool
```

Expected: JSON with `success` and `failed` arrays.

---

### Rolling Update

```bash
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "plugin_path": "/opt/minecraft/plugins/MyPlugin-1.3.jar",
    "plugin_name": "MyPlugin.jar",
    "batch_size": 2,
    "delay_seconds": 15,
    "drain_timeout": 60
  }' \
  http://localhost:8080/rolling-update | python3 -m json.tool
```

Expected: JSON with `updated`, `failed`, and `total` fields.

---

### Shutdown All Servers (Maintenance Mode)

```bash
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/shutdown-all | python3 -m json.tool
```

Expected: `{"maintenance": true, "results": {"stopped": [...], "failed": [...]}}`

Verify maintenance mode is active:
```bash
curl -s http://localhost:8080/health | python3 -m json.tool
# Should show "maintenance": true
```

Verify new server creation is blocked:
```bash
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"gamemode": "bedwars"}' \
  http://localhost:8080/create
# Should return 503 with maintenance mode error
```

---

### Start All Servers (Exit Maintenance)

```bash
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/start-all | python3 -m json.tool
```

Expected: `{"maintenance": false, "results": {"started": [...], "failed": [...]}}`

---

## Node Agent Endpoints (`localhost:5000`)

### Health Check (no auth required)

```bash
curl -s http://localhost:5000/health | python3 -m json.tool
```

Expected: `{"status": "ok"}`

---

### Node Status

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  http://localhost:5000/status | python3 -m json.tool
```

Expected: JSON with `max_servers`, `current_servers`, `running`, `allocated_ports`, and `containers`.

---

### Get a Free Port

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  http://localhost:5000/free-port | python3 -m json.tool
```

Expected: `{"port": 25566}` (or next available port in range)

---

### Create a Container Directly

```bash
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "server_id": "test-server-001",
    "gamemode": "bedwars",
    "container_name": "mc-test-server-001",
    "memory": "1G"
  }' \
  http://localhost:5000/create | python3 -m json.tool
```

Expected:
```json
{
  "server_id": "test-server-001",
  "container_name": "mc-test-server-001",
  "port": 25566,
  "instance_path": "/opt/minecraft/instances/test-server-001",
  "status": "running"
}
```

---

### Inspect a Container

```bash
curl -s -H "Authorization: Bearer $TOKEN" \
  http://localhost:5000/container/mc-test-server-001 | python3 -m json.tool
```

Expected: Full Podman/Docker inspect JSON for the container.

---

### Execute Command in Container

```bash
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "container_name": "mc-test-server-001",
    "command": "rcon-cli list"
  }' \
  http://localhost:5000/exec | python3 -m json.tool
```

Expected: `{"returncode": 0, "output": "...", "container": "mc-test-server-001"}`

---

### Update Plugin in Container

```bash
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "container_name": "mc-test-server-001",
    "plugin_path": "/opt/minecraft/plugins/MyPlugin.jar",
    "plugin_name": "MyPlugin.jar",
    "restart": false
  }' \
  http://localhost:5000/update-plugins | python3 -m json.tool
```

Expected: `{"status": "updated", "plugin": "MyPlugin.jar", "container": "mc-test-server-001", "restarted": false}`

---

### Destroy a Container

```bash
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "container_name": "mc-test-server-001",
    "server_id": "test-server-001"
  }' \
  http://localhost:5000/destroy | python3 -m json.tool
```

Expected: `{"status": "destroyed", "container_name": "mc-test-server-001"}`

---

## AutoServer Helper Script

### Start a Server

```bash
export ORCHESTRATOR_URL=http://localhost:8080
export AGENT_SECRET="$TOKEN"

python3 /opt/minecraft/scripts/autoserver_helper.py start bedwars
echo "Exit code: $?"
```

Expected: Prints the server address to stdout. Exit code 0.

---

### Stop a Server

```bash
python3 /opt/minecraft/scripts/autoserver_helper.py stop bedwars-a1b2c3d4
echo "Exit code: $?"
```

Expected: Exit code 0.

---

## Update Manager CLI

### Check Plugin Registry Status

```bash
python3 /opt/minecraft/update_manager.py status
```

Expected: JSON dump of all tracked plugins and their rollout state.

---

### Hot Swap Update

```bash
python3 /opt/minecraft/update_manager.py hot-swap \
  MyPlugin.jar /opt/minecraft/plugins/MyPlugin-2.0.jar 2.0.0
```

---

### Rolling Update

```bash
python3 /opt/minecraft/update_manager.py rolling \
  MyPlugin.jar /opt/minecraft/plugins/MyPlugin-2.0.jar 2.0.0 2
```

(Batch size of 2)

---

### Template Update

```bash
python3 /opt/minecraft/update_manager.py template \
  bedwars MyPlugin.jar /opt/minecraft/plugins/MyPlugin-2.0.jar 2.0.0
```

---

### Full Restart Update

```bash
python3 /opt/minecraft/update_manager.py full-restart \
  MyPlugin.jar /opt/minecraft/plugins/MyPlugin-2.0.jar 2.0.0 60
```

(60-second warning period)

---

## End-to-End Flow Test

This simulates the complete player connection flow:

```bash
# 1. Verify orchestrator is healthy
curl -s http://localhost:8080/health

# 2. Check node availability
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8080/nodes

# 3. Simulate AutoServer triggering a new bedwars server
python3 /opt/minecraft/scripts/autoserver_helper.py start bedwars
# Capture the server_id from the output

# 4. Verify the server appears in orchestrator status
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8080/status

# 5. Verify the container is running on the node
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:5000/status

# 6. Simulate game end — destroy the server
python3 /opt/minecraft/scripts/autoserver_helper.py stop $SERVER_ID

# 7. Verify cleanup
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8080/status
# Server should no longer appear
```

---

## Authentication Error Test

Verify that requests without valid auth are rejected:

```bash
# No auth header
curl -s http://localhost:8080/nodes
# Expected: 401 Unauthorized

# Wrong token
curl -s -H "Authorization: Bearer wrong-token" http://localhost:8080/nodes
# Expected: 401 Unauthorized
```

---

## Error Handling Tests

### Create server with missing gamemode

```bash
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{}' \
  http://localhost:8080/create
# Expected: 400 with "Missing required field: gamemode"
```

### Destroy non-existent server

```bash
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  http://localhost:8080/destroy/nonexistent-server
# Expected: 404 with "Server nonexistent-server not found"
```

### Create container with missing fields

```bash
curl -s -X POST \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"gamemode": "bedwars"}' \
  http://localhost:5000/create
# Expected: 400 with missing fields error
```
