#include <iostream>
#include <string>
#include <cstring>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>

using namespace std;

#define BUFFER_SIZE 1024

// Usage: ./client <port>        connects to 127.0.0.1:<port>
//        ./client <ip> <port>   connects to <ip>:<port>
int main(int argc, char* argv[]) {
    string ip   = "127.0.0.1";
    int    port = 8080;

    if (argc == 2) port = stoi(argv[1]);
    if (argc == 3) { ip = argv[1]; port = stoi(argv[2]); }

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) { cerr << "socket() failed\n"; return 1; }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    inet_pton(AF_INET, ip.c_str(), &addr.sin_addr);

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        cerr << "connect() failed — is the node running on port " << port << "?\n";
        return 1;
    }

    cout << "[*] Connected to " << ip << ":" << port << "\n";

    char buf[BUFFER_SIZE];
    memset(buf, 0, BUFFER_SIZE);
    recv(sock, buf, BUFFER_SIZE - 1, 0);
    cout << buf;

    string input;
    while (getline(cin, input)) {
        if (input.empty()) { cout << "> "; continue; }

        send(sock, (input + "\n").c_str(), input.size() + 1, 0);
        if (input == "QUIT") break;

        memset(buf, 0, BUFFER_SIZE);
        if (recv(sock, buf, BUFFER_SIZE - 1, 0) <= 0) break;
        cout << buf;
    }

    close(sock);
    return 0;
}