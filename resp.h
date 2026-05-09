#pragma once
#include <string>
#include <vector>
#include <algorithm>
#include <sys/socket.h>

using namespace std;

// ───────────────────────────────────────────────
//  RESP Protocol Reader
// ───────────────────────────────────────────────

// Read a single line terminated by \r\n (returns WITHOUT the \r\n)
inline string resp_read_line(int fd) {
    string line;
    char c;
    while (recv(fd, &c, 1, 0) == 1) {
        if (c == '\r') {
            recv(fd, &c, 1, 0);   // consume \n
            break;
        }
        if (c == '\n') break;     // bare \n fallback
        line += c;
    }
    return line;
}

// Read exactly n bytes from socket
inline string resp_read_bytes(int fd, int n) {
    string data(n, '\0');
    int got = 0;
    while (got < n) {
        int r = recv(fd, &data[got], n - got, 0);
        if (r <= 0) break;
        got += r;
    }
    // consume trailing \r\n
    char crlf[2];
    recv(fd, crlf, 2, MSG_WAITALL);
    return data;
}

// Parse one RESP command from the socket.
// Handles both RESP array format (*N\r\n$M\r\n...) and inline format.
// Returns empty vector on disconnect.
inline vector<string> resp_parse_command(int fd) {
    string first = resp_read_line(fd);
    if (first.empty()) return {};

    // RESP Array: *<count>\r\n followed by $<len>\r\n<data>\r\n ...
    if (first[0] == '*') {
        int count = stoi(first.substr(1));
        if (count <= 0) return {};
        vector<string> args;
        for (int i = 0; i < count; i++) {
            string hdr = resp_read_line(fd);
            if (hdr.empty() || hdr[0] != '$') break;
            int len = stoi(hdr.substr(1));
            if (len < 0) { args.emplace_back(""); continue; }
            args.push_back(resp_read_bytes(fd, len));
        }
        return args;
    }

    // Inline command: split by spaces
    vector<string> args;
    size_t pos = 0;
    while (pos < first.size()) {
        while (pos < first.size() && first[pos] == ' ') pos++;
        if (pos >= first.size()) break;
        size_t end = first.find(' ', pos);
        if (end == string::npos) end = first.size();
        args.push_back(first.substr(pos, end - pos));
        pos = end;
    }
    return args;
}

// ───────────────────────────────────────────────
//  RESP Protocol Encoders
// ───────────────────────────────────────────────

inline string resp_ok()                           { return "+OK\r\n"; }
inline string resp_pong()                         { return "+PONG\r\n"; }
inline string resp_simple(const string& s)        { return "+" + s + "\r\n"; }
inline string resp_error(const string& s)         { return "-ERR " + s + "\r\n"; }
inline string resp_integer(long long n)           { return ":" + to_string(n) + "\r\n"; }
inline string resp_null()                         { return "$-1\r\n"; }
inline string resp_empty_array()                  { return "*0\r\n"; }

inline string resp_bulk(const string& s) {
    return "$" + to_string(s.size()) + "\r\n" + s + "\r\n";
}

inline string resp_array(const vector<string>& items) {
    string out = "*" + to_string(items.size()) + "\r\n";
    for (auto& item : items) out += resp_bulk(item);
    return out;
}
