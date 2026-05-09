# gossip-kv

A peer-to-peer distributed key-value store built in C++ that uses a gossip protocol for data replication and speaks the Redis wire protocol (RESP), so standard `redis-cli` works out of the box.

Nodes automatically discover each other on the local network via UDP broadcast, maintain liveness through heartbeats, and replicate KV mutations (SET, DEL, INCR, etc.) across the cluster using epidemic gossip with TTL-based propagation and message deduplication.

## Architecture

```
┌──────────────────────────────────────────────────────┐
│                     Gossip Node                      │
│                                                      │
│  ┌──────────┐  ┌──────────┐  ┌─────────────────────┐│
│  │  Client   │  │   Peer   │  │    RESP Server      ││
│  │  Server   │  │  Server  │  │  (Redis-compatible) ││
│  │ TCP :port │  │TCP :port │  │   TCP :port+2000    ││
│  │          │  │  +1000   │  │                     ││
│  └──────────┘  └──────────┘  └─────────────────────┘│
│                                                      │
│  ┌──────────┐  ┌──────────┐  ┌─────────────────────┐│
│  │ Discovery │  │Heartbeat │  │   KV Store          ││
│  │UDP :7000  │  │ (2s tick)│  │ (replicated via     ││
│  │(broadcast)│  │          │  │  gossip protocol)   ││
│  └──────────┘  └──────────┘  └─────────────────────┘│
└──────────────────────────────────────────────────────┘
```

Each node runs 6 concurrent threads:
- **Client Server** — custom text protocol for interactive CLI sessions
- **Peer Server** — handles gossip messages and heartbeats from other nodes
- **RESP Server** — Redis-compatible interface for `redis-cli` and Redis client libraries
- **Discovery Sender** — broadcasts UDP `HELLO` packets every 3s
- **Discovery Listener** — listens for peer announcements on the LAN
- **Heartbeat** — sends periodic liveness probes, marks peers dead after 8s of silence

## Building

```bash
make
```

Requires a C++17 compiler (g++ or clang++) and pthreads.

## Running

Start a node (default port 8080):

```bash
./node              # port 8080, RESP on 10080
./node 9090         # port 9090, RESP on 11090
```

Start multiple nodes on different ports to form a cluster — they'll discover each other automatically via UDP broadcast.

### Connect with redis-cli

```bash
redis-cli -p 10080
127.0.0.1:10080> SET mykey hello
OK
127.0.0.1:10080> GET mykey
"hello"
127.0.0.1:10080> INCR counter
(integer) 1
127.0.0.1:10080> KEYS *
1) "mykey"
2) "counter"
127.0.0.1:10080> INFO
# Server
gossip_version:1.0.0
...
# Gossip
peers_total:2
peers_alive:2
```

### Connect with the built-in client

```bash
./client 8080                  # connect to localhost:8080
./client 192.168.1.50 9090     # connect to a remote node
```

Available commands: `PING`, `PEERS`, `GOSSIP <msg>`, `PUT <k> <v>`, `GET <k>`, `DEL <k>`, `STORE`, `QUIT`

## Supported Redis Commands

| Category | Commands |
|---|---|
| **Strings** | `SET` `GET` `MSET` `MGET` `APPEND` `STRLEN` `INCR` `DECR` |
| **Keys** | `DEL` `EXISTS` `KEYS *` `TYPE` `TTL` `DBSIZE` `FLUSHDB` |
| **Server** | `PING` `INFO` `QUIT` `SELECT` `COMMAND` `CLIENT` `CONFIG` |

All write operations (`SET`, `DEL`, `INCR`, `FLUSHDB`, etc.) are automatically gossiped to peer nodes for replication. Reads (`GET`, `EXISTS`, `KEYS`) are served locally — the store is **eventually consistent**.

## Gossip Protocol

- **Wire format**: `GOSSIP:<msg_id>:<ttl>:<content>`
- **Message IDs**: `<origin_port>-<counter>` (e.g., `8080-42`)
- **Deduplication**: Each node maintains a seen-message set; duplicate messages are silently dropped
- **TTL**: Messages propagate up to 3 hops (configurable), decremented on each forward
- **Replication**: KV mutations are encoded as `KV_PUT:<key>:<value>` or `KV_DEL:<key>` in the gossip payload

## Port Layout

| Port | Purpose |
|---|---|
| `<port>` | Client TCP server (built-in CLI) |
| `<port> + 1000` | Peer-to-peer TCP (gossip + heartbeats) |
| `<port> + 2000` | RESP server (redis-cli compatible) |
| `7000` (UDP) | Peer discovery broadcast |

## Project Structure

```
├── nodes.cpp    # main node: networking, gossip, KV store, RESP server
├── peers.h      # shared state: Peer struct, KV store, dedup set declarations
├── resp.h       # RESP protocol parser and encoder
├── client.cpp   # interactive CLI client
├── server.cpp   # standalone TCP server (legacy, unused)
└── Makefile
```