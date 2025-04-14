// client.cpp
#include "common.h"
#include <iostream>
#include <sstream>
#include <cstring>
#include <arpa/inet.h>
#include <unistd.h>

static std::string sendRequest(const std::string& ip, int port, const std::string& command) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return "";
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0) {
        perror("inet_pton");
        close(sock);
        return "";
    }

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror(("connect to " + ip + ":" + std::to_string(port)).c_str());
        close(sock);
        return "";
    }

    std::cerr << "[DBG] Connected to " << ip << ":" << port << "\n";

    if (send(sock, command.c_str(), command.size(), 0) < 0) {
        perror("send");
        close(sock);
        return "";
    }

    std::cerr << "[DBG] Sent: " << command << "\n";

    char buffer[MAX_BUFFER];
    int bytes = recv(sock, buffer, sizeof(buffer) - 1, 0);
    if (bytes <= 0) {
        if (bytes < 0) perror("recv");
        else std::cerr << "[DBG] recv returned 0 bytes\n";
        close(sock);
        return "";
    }

    buffer[bytes] = '\0';
    close(sock);
    std::string response(buffer);
    std::cerr << "[DBG] Received (" << bytes << " bytes): " << response << "\n";
    return response;
}

static bool parseNMResponse(const std::string& response, std::string& ip, int& port) {
    std::istringstream iss(response);
    std::string label;
    return bool(iss >> label >> ip >> label >> port);
}

static void usage(const char* progName) {
    std::cerr << "Usage: " << progName << " <NamingServerIP> <NamingServerPort>\n";
    exit(1);
}

int main(int argc, char** argv) {
    if (argc != 3) usage(argv[0]);

    std::string nm_ip = argv[1];
    int nm_port = std::stoi(argv[2]);

    std::string lineInput;
    while (true) {
        std::cout << "Client> ";
        if (!std::getline(std::cin, lineInput) || lineInput == "exit")
            break;
        if (lineInput.empty())
            continue;

        std::string command = lineInput;

        // Step 1: Send request to Naming Server
        std::string nmResp = sendRequest(nm_ip, nm_port, command);
        if (nmResp.empty()) {
            std::cerr << "ERROR: No response from Naming Server\n";
            continue;
        }

        std::cout << "NM: " << nmResp << "\n";

        // Handle special WRITE_NOTIFICATION responses from NM
        if (nmResp.rfind("WRITE_NOTIFICATION", 0) == 0) {
            std::cout << nmResp.substr(sizeof("WRITE_NOTIFICATION")) << "\n";
            continue;
        }

        // Step 2: Handle special responses or forward to Storage Server
        if (nmResp.rfind("IP:", 0) != 0) continue;

        std::string ss_ip;
        int ss_port;
        if (!parseNMResponse(nmResp, ss_ip, ss_port)) {
            std::cerr << "ERROR: Invalid Naming Server reply: " << nmResp << "\n";
            continue;
        }

        std::cerr << "[DBG] Parsed SS IP: " << ss_ip << ", Port: " << ss_port << "\n";

        // Step 3: Handle STREAM separately if needed
        std::istringstream streamCheckIss(command);
        std::string streamCmdCheck;
        streamCheckIss >> streamCmdCheck;
        if (streamCmdCheck == "STREAM") {
            int sock = socket(AF_INET, SOCK_STREAM, 0);
            if (sock < 0) { perror("socket"); continue; }

            sockaddr_in addr{};
            addr.sin_family = AF_INET;
            addr.sin_port   = htons(ss_port);
            if (inet_pton(AF_INET, ss_ip.c_str(), &addr.sin_addr) <= 0) {
                perror("inet_pton"); 
                close(sock);
                continue;
            }

            if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
                perror(("connect to " + ss_ip + ":" + std::to_string(ss_port)).c_str());
                close(sock);
                continue;
            }
            std::cerr << "[DBG] Connected to " << ss_ip << ":" << ss_port << " for STREAM\n";

            if (send(sock, command.c_str(), command.size(), 0) < 0) {
                perror("send STREAM command");
                close(sock);
                continue;
            }
            std::cerr << "[DBG] Sent STREAM command: " << command << "\n";

            char initialBuffer[1024];
            int initialBytes = recv(sock, initialBuffer, sizeof(initialBuffer) - 1, 0);

            if (initialBytes <= 0) {
                if (initialBytes < 0) perror("recv initial stream data");
                else std::cerr << "ERROR: Storage server closed connection immediately.\n";
                close(sock);
                continue;
            }

            initialBuffer[initialBytes] = '\0';
            std::string initialResponse(initialBuffer);

            if (initialResponse.rfind("ERROR:", 0) == 0) {
                std::cerr << "Storage Server Error: " << initialResponse << "\n";
                close(sock);
                continue;
            }

            FILE *mpvPipe = popen("mpv --really-quiet -", "w");
            if (!mpvPipe) {
                perror("popen(mpv)");
                close(sock);
                continue;
            }

            std::cout << "Streaming audio... (Ctrl+C to stop)\n";

            fwrite(initialBuffer, 1, initialBytes, mpvPipe);

            char streamBuffer[MAX_BUFFER];
            int bytes;
            while ((bytes = recv(sock, streamBuffer, sizeof(streamBuffer), 0)) > 0) {
                size_t written = fwrite(streamBuffer, 1, bytes, mpvPipe);
                if (written < bytes) {
                     perror("fwrite to mpv pipe");
                     break;
                }
            }

            if (bytes < 0) {
                perror("recv stream data");
            }

            pclose(mpvPipe);
            close(sock);
            std::cout << "Stream finished.\n";
            continue;
        }

        // Step 4: Forward to Storage Server
        std::string ssResp = sendRequest(ss_ip, ss_port, command);
        if (ssResp.empty()) {
            std::cerr << "ERROR: No response from Storage Server\n";
            continue;
        }

        std::cout << ssResp << "\n";
    }

    return 0;
}
