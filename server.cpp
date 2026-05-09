#include <iostream>
#include <string>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

using namespace std;

#define PORT 8080
#define BUFFER_SIZE 1024

void handle_client(int client_fd, struct sockaddr_in client_addr) {
    string ip = inet_ntoa(client_addr.sin_addr);
    int port = ntohs(client_addr.sin_port);
    cout << "[+] Connected: " << ip << ":" << port << "\n";

    string welcome = "Gossip Node v0.1 | Commands: PING, ECHO <msg>, INFO, QUIT\n> ";
    send(client_fd, welcome.c_str(), welcome.size(), 0);

    char buffer[BUFFER_SIZE];

    while (true) {
        memset(buffer, 0, BUFFER_SIZE);
        int bytes = recv(client_fd, buffer, BUFFER_SIZE - 1, 0);

        if (bytes <= 0) {
            cout << "[-] Disconnected: " << ip << ":" << port << "\n";
            break;
        }

        string cmd(buffer);
        while (!cmd.empty() && (cmd.back() == '\n' || cmd.back() == '\r'))
            cmd.pop_back();

        cout << "[" << ip << ":" << port << "] " << cmd << "\n";

        string response;

        if (cmd == "PING") {
            response = "PONG\n> ";
        } else if (cmd.size() > 5 && cmd.substr(0, 4) == "ECHO") {
            response = cmd.substr(5) + "\n> ";
        } else if (cmd == "INFO") {
            response = "Node: node-1 | Port: " + to_string(PORT) + " | Status: ALIVE\n> ";
        } else if (cmd == "QUIT" || cmd == "EXIT") {
            send(client_fd, "Goodbye!\n", 9, 0);
            break;
        } else {
            response = "Unknown command\n> ";
        }

        send(client_fd, response.c_str(), response.size(), 0);
    }

    close(client_fd);
}

int main() {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { cerr << "socket() failed\n"; return 1; }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);

    if (::bind(server_fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        cerr << "bind() failed\n"; return 1;
    }

    if (listen(server_fd, 5) < 0) {
        cerr << "listen() failed\n"; return 1;
    }

    cout << "[*] Listening on port " << PORT << "\n";

    while (true) {
        struct sockaddr_in client_addr{};
        socklen_t client_len = sizeof(client_addr);
        int client_fd = accept(server_fd, (struct sockaddr*)&client_addr, &client_len);
        if (client_fd < 0) continue;
        handle_client(client_fd, client_addr);
    }

    close(server_fd);
    return 0;
}