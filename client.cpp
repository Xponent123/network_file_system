// client.cpp
#include "common.h" // Include the common header file for shared constants and structures
#include <iostream> // For input/output operations (e.g., std::cout, std::cerr)
#include <sstream>  // For string stream operations (e.g., parsing strings)
#include <cstring>  // For C-style string operations (e.g., memset, strcmp)
#include <arpa/inet.h> // For socket-related functions and structures (e.g., inet_pton, sockaddr_in)
#include <unistd.h> // For POSIX functions like close() to manage file descriptors

// Function to send a request to a server and receive a response
static std::string sendRequest(const std::string& ip, int port, const std::string& command) {
    // Create a socket for communication
    // AF_INET: IPv4, SOCK_STREAM: TCP, 0: Default protocol
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) { // Check if socket creation failed
        perror("socket"); // Print the error message to stderr
        return ""; // Return an empty string to indicate failure
    }

    sockaddr_in addr{}; // Define a structure to hold the server's address information
    addr.sin_family = AF_INET; // Set the address family to IPv4
    addr.sin_port = htons(port); // Convert the port number from host byte order to network byte order
    if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0) { // Convert the IP address from text to binary form
        perror("inet_pton"); // Print an error message if the conversion fails
        close(sock); // Close the socket to release resources
        return ""; // Return an empty string to indicate failure
    }

    // Attempt to connect to the server using the socket and address information
    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        // Print an error message if the connection fails
        perror(("connect to " + ip + ":" + std::to_string(port)).c_str());
        close(sock); // Close the socket to release resources
        return ""; // Return an empty string to indicate failure
    }

    // Print a debug message indicating a successful connection
    std::cerr << "[DBG] Connected to " << ip << ":" << port << "\n";

    // Send the command to the server
    // command.c_str(): Get the C-style string representation of the command
    // command.size(): Length of the command string
    if (send(sock, command.c_str(), command.size(), 0) < 0) {
        perror("send"); // Print an error message if sending fails
        close(sock); // Close the socket to release resources
        return ""; // Return an empty string to indicate failure
    }

    // Print a debug message indicating the command was sent successfully
    std::cerr << "[DBG] Sent: " << command << "\n";

    char buffer[MAX_BUFFER]; // Define a buffer to store the server's response
    // Receive the server's response
    // recv(): Reads data from the socket into the buffer
    // sizeof(buffer) - 1: Leave space for null-terminating the string
    int bytes = recv(sock, buffer, sizeof(buffer) - 1, 0);
    if (bytes <= 0) { // Check if no data was received or an error occurred
        if (bytes < 0) perror("recv"); // Print an error message if receiving failed
        else std::cerr << "[DBG] recv returned 0 bytes\n"; // Debug message for no data
        close(sock); // Close the socket to release resources
        return ""; // Return an empty string to indicate failure
    }

    buffer[bytes] = '\0'; // Null-terminate the received data to make it a valid C-style string
    close(sock); // Close the socket to release resources
    std::string response(buffer); // Convert the buffer to a C++ string
    // Print a debug message showing the received response and its size
    std::cerr << "[DBG] Received (" << bytes << " bytes): " << response << "\n";
    return response; // Return the server's response as a string
}

// Function to parse the Naming Server's response to extract IP and port
static bool parseNMResponse(const std::string& response, std::string& ip, int& port) {
    std::istringstream iss(response); // Create a string stream from the response
    std::string label; // Temporary variable to hold labels in the response
    // Extract the IP and port from the response
    // Example response: "IP: 192.168.1.1 PORT: 8080"
    return bool(iss >> label >> ip >> label >> port); // Parse and return true if successful
}

// Function to display usage instructions and exit the program
static void usage(const char* progName) {
    // Print the correct usage format to stderr
    std::cerr << "Usage: " << progName << " <NamingServerIP> <NamingServerPort>\n";
    exit(1); // Exit the program with a non-zero status code to indicate an error
}

// Main function: Entry point of the program
int main(int argc, char** argv) {
    if (argc != 3) usage(argv[0]); // Check if the correct number of arguments is provided

    std::string nm_ip = argv[1]; // Get the Naming Server's IP address from the command-line arguments
    int nm_port = std::stoi(argv[2]); // Convert the Naming Server's port number from string to integer

    std::string lineInput; // Variable to store user input
    while (true) { // Infinite loop to process user commands
        std::cout << "Client> "; // Print a prompt for user input
        if (!std::getline(std::cin, lineInput) || lineInput == "exit") // Exit the loop on EOF or "exit"
            break;
        if (lineInput.empty()) // Skip empty input
            continue;

        std::string command = lineInput; // Store the user command

        // Step 1: Send request to Naming Server
        std::string nmResp = sendRequest(nm_ip, nm_port, command); // Send the command to the Naming Server
        if (nmResp.empty()) { // Check if no response was received
            std::cerr << "ERROR: No response from Naming Server\n"; // Print an error message
            continue;
        }

        std::cout << "NM: " << nmResp << "\n"; // Print the Naming Server's response

        // Handle special WRITE_NOTIFICATION responses from Naming Server
        if (nmResp.rfind("WRITE_NOTIFICATION", 0) == 0) { // Check if response starts with "WRITE_NOTIFICATION"
            std::cout << nmResp.substr(sizeof("WRITE_NOTIFICATION")) << "\n"; // Print the notification
            continue;
        }

        // Step 2: Handle special responses or forward to Storage Server
        if (nmResp.rfind("IP:", 0) != 0) continue; // Skip if response does not start with "IP:"

        std::string ss_ip; // Variable to store Storage Server IP
        int ss_port; // Variable to store Storage Server port
        if (!parseNMResponse(nmResp, ss_ip, ss_port)) { // Parse the Naming Server's response
            std::cerr << "ERROR: Invalid Naming Server reply: " << nmResp << "\n"; // Print an error message
            continue;
        }

        std::cerr << "[DBG] Parsed SS IP: " << ss_ip << ", Port: " << ss_port << "\n"; // Debug message

        // Step 3: Handle STREAM separately if needed
        std::istringstream streamCheckIss(command); // Create a string stream from the command
        std::string streamCmdCheck; // Variable to store the first word of the command
        streamCheckIss >> streamCmdCheck; // Extract the first word
        if (streamCmdCheck == "STREAM") { // Check if the command is "STREAM"
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
        std::string ssResp = sendRequest(ss_ip, ss_port, command); // Send the command to the Storage Server
        if (ssResp.empty()) { // Check if no response was received
            std::cerr << "ERROR: No response from Storage Server\n"; // Print an error message
            continue;
        }

        std::cout << ssResp << "\n"; // Print the Storage Server's response
    }

    return 0; // Exit the program successfully
}
