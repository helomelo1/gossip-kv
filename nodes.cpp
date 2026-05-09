#include <iostream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <cstring>
#include <ctime>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <algorithm>
#include "peers.h"
#include "resp.h"

using namespace std;

#define BUFFER_SIZE   1024
#define UDP_DISC_PORT 7000
#define HB_INTERVAL   2
#define DEAD_TIMEOUT  8
#define MAX_SEEN      4096   // cap the seen set so it doesn't grow forever

vector<Peer> peers;
mutex peers_mutex;
int node_port;

// --- Dedup state ---
unordered_set<string> seen_messages;
mutex seen_mutex;
atomic<uint64_t> msg_counter{0};

// --- KV Store state ---
unordered_map<string, string> kv_store;
mutex kv_mutex;

// Generate a unique message ID: <port>-<counter>
string generate_msg_id() {
    return to_string(node_port) + "-" + to_string(msg_counter.fetch_add(1));
}

// Returns true if this is a NEW message (first time seen).
// Returns false if already seen (duplicate).
bool mark_seen(const string& id) {
    lock_guard<mutex> lock(seen_mutex);
    if (seen_messages.count(id)) return false;
    // Evict oldest entries if set is too large
    if (seen_messages.size() >= MAX_SEEN) seen_messages.clear();
    seen_messages.insert(id);
    return true;
}

void add_or_update_peer(const string& ip, int port) {
    lock_guard<mutex> lock(peers_mutex);
    for (auto& p : peers) {
        if (p.ip == ip && p.port == port) {
            p.last_seen = time(nullptr);
            if (!p.alive) { p.alive = true; cout << "[+] Peer back: " << ip << ":" << port << "\n"; }
            return;
        }
    }
    peers.push_back({ip, port, time(nullptr), true});
    cout << "[+] New peer: " << ip << ":" << port << "\n";
}

// Send a raw pre-formatted packet to all alive peers (used for re-broadcasting)
void gossip_spread_raw(const string& packet) {
    vector<Peer> alive;
    {
        lock_guard<mutex> lock(peers_mutex);
        for (auto& p : peers)
            if (p.alive) alive.push_back(p);
    }
    for (auto& p : alive) {
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        struct timeval tv{1, 0};
        setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port   = htons(p.port + 1000);
        inet_pton(AF_INET, p.ip.c_str(), &addr.sin_addr);

        if (connect(sock, (sockaddr*)&addr, sizeof(addr)) == 0) {
            send(sock, packet.c_str(), packet.size(), 0);
        }
        close(sock);
    }
}

// Originate a new gossip message (generates a fresh ID)
// Wire format: GOSSIP:<id>:<ttl>:<content>\n
void gossip_spread(const string& msg, int ttl) {
    string id = generate_msg_id();
    mark_seen(id);  // mark our own message as seen
    string packet = "GOSSIP:" + id + ":" + to_string(ttl) + ":" + msg + "\n";
    gossip_spread_raw(packet);
}

void handle_peer_msg(int fd, sockaddr_in peer_addr) {
    char buf[BUFFER_SIZE];
    memset(buf, 0, BUFFER_SIZE);
    if (recv(fd, buf, BUFFER_SIZE - 1, 0) > 0) {
        string msg(buf);
        while (!msg.empty() && (msg.back() == '\n' || msg.back() == '\r')) msg.pop_back();

        if (msg.size() > 7 && msg.substr(0, 7) == "GOSSIP:") {
            // Parse: GOSSIP:<id>:<ttl>:<content>
            size_t c1 = msg.find(':', 7);          // end of id
            size_t c2 = msg.find(':', c1 + 1);     // end of ttl
            if (c1 == string::npos || c2 == string::npos) { close(fd); return; }

            string id      = msg.substr(7, c1 - 7);
            int    ttl     = stoi(msg.substr(c1 + 1, c2 - c1 - 1));
            string content = msg.substr(c2 + 1);

            // --- Dedup check ---
            if (!mark_seen(id)) {
                // Already seen this message, drop it
                close(fd);
                return;
            }

            cout << "[GOSSIP] " << content << " (id=" << id << " ttl=" << ttl << ")\n";

            // Apply KV operations carried via gossip
            if (content.size() > 7 && content.substr(0, 7) == "KV_PUT:") {
                // Format: KV_PUT:<key>:<value>
                size_t sep = content.find(':', 7);
                if (sep != string::npos) {
                    string key = content.substr(7, sep - 7);
                    string val = content.substr(sep + 1);
                    lock_guard<mutex> lock(kv_mutex);
                    kv_store[key] = val;
                    cout << "[KV] Replicated PUT " << key << " = " << val << "\n";
                }
            } else if (content.size() > 7 && content.substr(0, 7) == "KV_DEL:") {
                // Format: KV_DEL:<key>
                string key = content.substr(7);
                lock_guard<mutex> lock(kv_mutex);
                kv_store.erase(key);
                cout << "[KV] Replicated DEL " << key << "\n";
            }

            // Re-broadcast with decremented TTL
            if (ttl > 1) {
                string packet = "GOSSIP:" + id + ":" + to_string(ttl - 1) + ":" + content + "\n";
                gossip_spread_raw(packet);
            }

        } else if (msg.size() > 10 && msg.substr(0, 10) == "HEARTBEAT:") {
            int peer_port  = stoi(msg.substr(10));
            char ip_buf[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &peer_addr.sin_addr, ip_buf, sizeof(ip_buf));
            string peer_ip(ip_buf);
            add_or_update_peer(peer_ip, peer_port);
        }
    }
    close(fd);
}

void handle_client(int fd, sockaddr_in addr) {
    char ip_buf[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &addr.sin_addr, ip_buf, sizeof(ip_buf));
    cout << "[+] Client: " << ip_buf << "\n";

    string welcome = "Node:" + to_string(node_port) +
        "  Commands: PING | PEERS | GOSSIP <msg> | PUT <k> <v> | GET <k> | DEL <k> | STORE | QUIT\n> ";
    send(fd, welcome.c_str(), welcome.size(), 0);

    char buf[BUFFER_SIZE];
    while (true) {
        memset(buf, 0, BUFFER_SIZE);
        if (recv(fd, buf, BUFFER_SIZE - 1, 0) <= 0) break;

        string cmd(buf);
        while (!cmd.empty() && (cmd.back() == '\n' || cmd.back() == '\r')) cmd.pop_back();

        string resp;
        if (cmd == "PING") {
            resp = "PONG\n> ";

        } else if (cmd == "PEERS") {
            lock_guard<mutex> lock(peers_mutex);
            resp = to_string(peers.size()) + " peer(s):\n";
            for (auto& p : peers)
                resp += "  " + p.ip + ":" + to_string(p.port) + (p.alive ? "  ALIVE" : "  DEAD") + "\n";
            resp += "> ";

        } else if (cmd.size() > 7 && cmd.substr(0, 6) == "GOSSIP") {
            gossip_spread(cmd.substr(7), 3);
            resp = "Spreading...\n> ";

        // --- KV Store commands ---
        } else if (cmd.size() > 4 && cmd.substr(0, 4) == "PUT ") {
            // PUT <key> <value>
            size_t sp = cmd.find(' ', 4);
            if (sp == string::npos) {
                resp = "Usage: PUT <key> <value>\n> ";
            } else {
                string key = cmd.substr(4, sp - 4);
                string val = cmd.substr(sp + 1);
                {
                    lock_guard<mutex> lock(kv_mutex);
                    kv_store[key] = val;
                }
                // Gossip the KV update to peers
                gossip_spread("KV_PUT:" + key + ":" + val, 3);
                resp = "OK (" + key + " = " + val + ")\n> ";
            }

        } else if (cmd.size() > 4 && cmd.substr(0, 4) == "GET ") {
            // GET <key>  (local read — eventually consistent)
            string key = cmd.substr(4);
            lock_guard<mutex> lock(kv_mutex);
            auto it = kv_store.find(key);
            if (it != kv_store.end())
                resp = it->second + "\n> ";
            else
                resp = "(nil)\n> ";

        } else if (cmd.size() > 4 && cmd.substr(0, 4) == "DEL ") {
            // DEL <key>
            string key = cmd.substr(4);
            {
                lock_guard<mutex> lock(kv_mutex);
                kv_store.erase(key);
            }
            gossip_spread("KV_DEL:" + key, 3);
            resp = "DELETED\n> ";

        } else if (cmd == "STORE") {
            // Dump the entire local KV store
            lock_guard<mutex> lock(kv_mutex);
            if (kv_store.empty()) {
                resp = "(empty)\n> ";
            } else {
                resp = to_string(kv_store.size()) + " key(s):\n";
                for (auto& [k, v] : kv_store)
                    resp += "  " + k + " = " + v + "\n";
                resp += "> ";
            }

        } else if (cmd == "QUIT") {
            send(fd, "Bye!\n", 5, 0); break;
        } else {
            resp = "Unknown command\n> ";
        }
        send(fd, resp.c_str(), resp.size(), 0);
    }
    close(fd);
}

void tcp_client_server() {
    int sfd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(node_port);
    ::bind(sfd, (sockaddr*)&addr, sizeof(addr));
    listen(sfd, 10);
    cout << "[*] Client server  -> :" << node_port << "\n";

    while (true) {
        sockaddr_in caddr{}; socklen_t len = sizeof(caddr);
        int cfd = accept(sfd, (sockaddr*)&caddr, &len);
        if (cfd > 0) thread(handle_client, cfd, caddr).detach();
    }
}

void tcp_peer_server() {
    int peer_port = node_port + 1000;
    int sfd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(peer_port);
    ::bind(sfd, (sockaddr*)&addr, sizeof(addr));
    listen(sfd, 10);
    cout << "[*] Peer server    -> :" << peer_port << "\n";

    while (true) {
        sockaddr_in paddr{}; socklen_t len = sizeof(paddr);
        int pfd = accept(sfd, (sockaddr*)&paddr, &len);
        if (pfd > 0) thread(handle_peer_msg, pfd, paddr).detach();
    }
}

void discovery_sender() {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    int bc = 1;
    setsockopt(sock, SOL_SOCKET, SO_BROADCAST, &bc, sizeof(bc));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = inet_addr("255.255.255.255");
    addr.sin_port        = htons(UDP_DISC_PORT);

    string msg = "HELLO:" + to_string(node_port);
    while (true) {
        sendto(sock, msg.c_str(), msg.size(), 0, (sockaddr*)&addr, sizeof(addr));
        sleep(3);
    }
}

void discovery_listener() {
    int sock = socket(AF_INET, SOCK_DGRAM, 0);
    int opt = 1; setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    int bc  = 1; setsockopt(sock, SOL_SOCKET, SO_BROADCAST, &bc,  sizeof(bc));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(UDP_DISC_PORT);
    ::bind(sock, (sockaddr*)&addr, sizeof(addr));
    cout << "[*] Discovery      -> UDP :" << UDP_DISC_PORT << "\n";

    char buf[BUFFER_SIZE];
    while (true) {
        sockaddr_in sender{}; socklen_t len = sizeof(sender);
        memset(buf, 0, BUFFER_SIZE);
        int n = recvfrom(sock, buf, BUFFER_SIZE - 1, 0, (sockaddr*)&sender, &len);
        if (n <= 0) continue;

        string msg(buf);
        if (msg.size() > 6 && msg.substr(0, 6) == "HELLO:") {
            int peer_port  = stoi(msg.substr(6));
            char ip_buf[INET_ADDRSTRLEN];
            inet_ntop(AF_INET, &sender.sin_addr, ip_buf, sizeof(ip_buf));
            string peer_ip(ip_buf);
            if (peer_port != node_port)
                add_or_update_peer(peer_ip, peer_port);
        }
    }
}

void heartbeat() {
    while (true) {
        sleep(HB_INTERVAL);

        {
            lock_guard<mutex> lock(peers_mutex);
            time_t now = time(nullptr);
            for (auto& p : peers)
                if (p.alive && now - p.last_seen > DEAD_TIMEOUT) {
                    p.alive = false;
                    cout << "[-] Peer dead: " << p.ip << ":" << p.port << "\n";
                }
        }

        vector<Peer> alive;
        {
            lock_guard<mutex> lock(peers_mutex);
            for (auto& p : peers)
                if (p.alive) alive.push_back(p);
        }

        for (auto& p : alive) {
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            struct timeval tv{1, 0};
            setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));

            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port   = htons(p.port + 1000);
            inet_pton(AF_INET, p.ip.c_str(), &addr.sin_addr);

            if (connect(sock, (sockaddr*)&addr, sizeof(addr)) == 0) {
                string hb = "HEARTBEAT:" + to_string(node_port) + "\n";
                send(sock, hb.c_str(), hb.size(), 0);
            }
            close(sock);
        }
    }
}

// ───────────────────────────────────────────────
//  RESP Server (redis-cli compatible)
// ───────────────────────────────────────────────

void handle_resp_client(int fd) {
    while (true) {
        vector<string> args = resp_parse_command(fd);
        if (args.empty()) break;

        // Uppercase the command
        string cmd = args[0];
        transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);

        string reply;

        // ── Connection ──
        if (cmd == "PING") {
            reply = (args.size() > 1) ? resp_bulk(args[1]) : resp_pong();

        } else if (cmd == "QUIT") {
            send(fd, resp_ok().c_str(), resp_ok().size(), 0);
            break;

        } else if (cmd == "SELECT") {
            reply = resp_ok();   // single-db, accept silently

        } else if (cmd == "COMMAND") {
            reply = resp_empty_array();  // enough for redis-cli handshake

        } else if (cmd == "CLIENT") {
            reply = resp_ok();

        } else if (cmd == "CONFIG") {
            // CONFIG GET <param> → return empty list
            reply = resp_empty_array();

        // ── String commands ──
        } else if (cmd == "SET") {
            if (args.size() < 3) { reply = resp_error("wrong number of arguments for 'set'"); }
            else {
                {
                    lock_guard<mutex> lock(kv_mutex);
                    kv_store[args[1]] = args[2];
                }
                gossip_spread("KV_PUT:" + args[1] + ":" + args[2], 3);
                reply = resp_ok();
            }

        } else if (cmd == "GET") {
            if (args.size() < 2) { reply = resp_error("wrong number of arguments for 'get'"); }
            else {
                lock_guard<mutex> lock(kv_mutex);
                auto it = kv_store.find(args[1]);
                reply = (it != kv_store.end()) ? resp_bulk(it->second) : resp_null();
            }

        } else if (cmd == "DEL") {
            if (args.size() < 2) { reply = resp_error("wrong number of arguments for 'del'"); }
            else {
                int deleted = 0;
                for (size_t i = 1; i < args.size(); i++) {
                    lock_guard<mutex> lock(kv_mutex);
                    if (kv_store.erase(args[i])) {
                        deleted++;
                        gossip_spread("KV_DEL:" + args[i], 3);
                    }
                }
                reply = resp_integer(deleted);
            }

        } else if (cmd == "EXISTS") {
            if (args.size() < 2) { reply = resp_error("wrong number of arguments for 'exists'"); }
            else {
                int count = 0;
                lock_guard<mutex> lock(kv_mutex);
                for (size_t i = 1; i < args.size(); i++)
                    if (kv_store.count(args[i])) count++;
                reply = resp_integer(count);
            }

        } else if (cmd == "KEYS") {
            // Simplified: KEYS * returns all keys, any other pattern returns empty
            lock_guard<mutex> lock(kv_mutex);
            if (args.size() > 1 && args[1] == "*") {
                vector<string> keys;
                for (auto& [k, v] : kv_store) keys.push_back(k);
                reply = resp_array(keys);
            } else {
                reply = resp_empty_array();
            }

        } else if (cmd == "DBSIZE") {
            lock_guard<mutex> lock(kv_mutex);
            reply = resp_integer(kv_store.size());

        } else if (cmd == "FLUSHDB" || cmd == "FLUSHALL") {
            {
                lock_guard<mutex> lock(kv_mutex);
                kv_store.clear();
            }
            reply = resp_ok();

        } else if (cmd == "MSET") {
            if (args.size() < 3 || (args.size() - 1) % 2 != 0) {
                reply = resp_error("wrong number of arguments for 'mset'");
            } else {
                for (size_t i = 1; i < args.size(); i += 2) {
                    {
                        lock_guard<mutex> lock(kv_mutex);
                        kv_store[args[i]] = args[i + 1];
                    }
                    gossip_spread("KV_PUT:" + args[i] + ":" + args[i + 1], 3);
                }
                reply = resp_ok();
            }

        } else if (cmd == "MGET") {
            if (args.size() < 2) { reply = resp_error("wrong number of arguments for 'mget'"); }
            else {
                string out = "*" + to_string(args.size() - 1) + "\r\n";
                lock_guard<mutex> lock(kv_mutex);
                for (size_t i = 1; i < args.size(); i++) {
                    auto it = kv_store.find(args[i]);
                    out += (it != kv_store.end()) ? resp_bulk(it->second) : resp_null();
                }
                reply = out;
            }

        } else if (cmd == "APPEND") {
            if (args.size() < 3) { reply = resp_error("wrong number of arguments for 'append'"); }
            else {
                lock_guard<mutex> lock(kv_mutex);
                kv_store[args[1]] += args[2];
                reply = resp_integer(kv_store[args[1]].size());
            }

        } else if (cmd == "STRLEN") {
            if (args.size() < 2) { reply = resp_error("wrong number of arguments for 'strlen'"); }
            else {
                lock_guard<mutex> lock(kv_mutex);
                auto it = kv_store.find(args[1]);
                reply = resp_integer(it != kv_store.end() ? it->second.size() : 0);
            }

        } else if (cmd == "INCR" || cmd == "DECR") {
            if (args.size() < 2) { reply = resp_error("wrong number of arguments"); }
            else {
                lock_guard<mutex> lock(kv_mutex);
                long long val = 0;
                auto it = kv_store.find(args[1]);
                if (it != kv_store.end()) {
                    try { val = stoll(it->second); }
                    catch (...) { reply = resp_error("value is not an integer"); goto send_reply; }
                }
                val += (cmd == "INCR") ? 1 : -1;
                kv_store[args[1]] = to_string(val);
                reply = resp_integer(val);
            }

        } else if (cmd == "TTL" || cmd == "PTTL") {
            reply = resp_integer(-1);  // no expiry support

        } else if (cmd == "TYPE") {
            if (args.size() < 2) { reply = resp_error("wrong number of arguments"); }
            else {
                lock_guard<mutex> lock(kv_mutex);
                reply = kv_store.count(args[1]) ? resp_simple("string") : resp_simple("none");
            }

        } else if (cmd == "INFO") {
            string info;
            info += "# Server\r\n";
            info += "gossip_version:1.0.0\r\n";
            info += "redis_mode:standalone\r\n";
            info += "tcp_port:" + to_string(node_port + 2000) + "\r\n";
            info += "\r\n# Keyspace\r\n";
            {
                lock_guard<mutex> lock(kv_mutex);
                info += "db0:keys=" + to_string(kv_store.size()) + "\r\n";
            }
            info += "\r\n# Gossip\r\n";
            {
                lock_guard<mutex> lock(peers_mutex);
                int alive_count = 0;
                for (auto& p : peers) if (p.alive) alive_count++;
                info += "peers_total:" + to_string(peers.size()) + "\r\n";
                info += "peers_alive:" + to_string(alive_count) + "\r\n";
            }
            reply = resp_bulk(info);

        } else {
            reply = resp_error("unknown command '" + args[0] + "'");
        }

        send_reply:
        send(fd, reply.c_str(), reply.size(), 0);
    }
    close(fd);
}

void resp_server() {
    int resp_port = node_port + 2000;
    int sfd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(resp_port);
    ::bind(sfd, (sockaddr*)&addr, sizeof(addr));
    listen(sfd, 10);
    cout << "[*] RESP server    -> :" << resp_port << "  (redis-cli -p " << resp_port << ")\n";

    while (true) {
        sockaddr_in caddr{}; socklen_t len = sizeof(caddr);
        int cfd = accept(sfd, (sockaddr*)&caddr, &len);
        if (cfd > 0) thread(handle_resp_client, cfd).detach();
    }
}

// Usage: ./node <port>  (default 8080)
int main(int argc, char* argv[]) {
    node_port = (argc > 1) ? stoi(argv[1]) : 8080;
    cout << "=== Gossip Node on port " << node_port << " ===\n";
    cout << "    Client port : " << node_port << "\n";
    cout << "    Peer port   : " << node_port + 1000 << "\n";
    cout << "    RESP port   : " << node_port + 2000 << "\n\n";

    thread(tcp_client_server).detach();
    thread(tcp_peer_server).detach();
    thread(discovery_sender).detach();
    thread(discovery_listener).detach();
    thread(heartbeat).detach();
    thread(resp_server).detach();

    while (true) sleep(1);
    return 0;
}