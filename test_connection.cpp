#include <iostream>
#include <string>
#include <cstring>
#include <unistd.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <server_ip> <server_port>\n";
        return 1;
    }

    const char* server_ip = argv[1];
    int server_port = std::stoi(argv[2]);
    
    std::cout << "Testing connection to " << server_ip << ":" << server_port << "...\n";
    
    // Create socket
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        std::cerr << "Error creating socket: " << strerror(errno) << std::endl;
        return 1;
    }
    
    // Set socket timeout (5 seconds)
    struct timeval timeout;
    timeout.tv_sec = 5;
    timeout.tv_usec = 0;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    
    // Setup server address structure
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        std::cerr << "Invalid address: " << server_ip << std::endl;
        close(sock);
        return 1;
    }
    
    // Try to connect
    std::cout << "Attempting connection..." << std::endl;
    
    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        std::cerr << "Connection failed: " << strerror(errno) << std::endl;
        std::cout << "\nPossible issues:\n"
                  << "1. Naming server is not running on that IP/port\n"
                  << "2. Firewall is blocking the connection\n"
                  << "3. IP address is incorrect or unreachable\n";
        close(sock);
        return 1;
    }
    
    std::cout << "Connection successful!" << std::endl;
    
    // Try sending a simple message
    const char* msg = "HELLO";
    if (send(sock, msg, strlen(msg), 0) < 0) {
        std::cerr << "Send failed: " << strerror(errno) << std::endl;
        close(sock);
        return 1;
    }
    
    std::cout << "Message sent successfully." << std::endl;
    
    // Try receiving a response
    char buffer[1024] = {0};
    int bytes_received = recv(sock, buffer, sizeof(buffer) - 1, 0);
    if (bytes_received < 0) {
        std::cerr << "Receive failed: " << strerror(errno) << std::endl;
    } else if (bytes_received == 0) {
        std::cout << "Server closed connection." << std::endl;
    } else {
        buffer[bytes_received] = '\0';
        std::cout << "Received: " << buffer << std::endl;
    }
    
    close(sock);
    return 0;
}
