#pragma once
#include <string>
#include <vector>
#include <mutex>
#include <ctime>
#include <unordered_map>
#include <unordered_set>
#include <atomic>
#include <queue>

using namespace std;

struct Peer {
    string ip;
    int port;
    time_t last_seen;
    bool alive;
};

extern vector<Peer> peers;
extern mutex peers_mutex;
extern int node_port;

// --- Dedup ---
extern unordered_set<string> seen_messages;
extern queue<string> seen_order;  // FIFO eviction queue
extern mutex seen_mutex;
extern atomic<uint64_t> msg_counter;
string generate_msg_id();
bool mark_seen(const string& id);

// --- KV Store ---
extern unordered_map<string, string> kv_store;
extern mutex kv_mutex;

void add_or_update_peer(const string& ip, int port);
void gossip_spread(const string& msg, int ttl);
void gossip_spread_raw(const string& packet);