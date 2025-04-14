// naming_server.cpp
// #define _GNU_SOURCE

#include "common.h"
#include <iostream>
#include <thread>
#include <vector>
#include <sstream>
#include <cstring>        // for memset, strcmp, etc.
#include <unistd.h>       // for close
#include <ifaddrs.h>      // for getifaddrs, struct ifaddrs, freeifaddrs
#include <net/if.h>       // for IFF_LOOPBACK
#include <arpa/inet.h>    // for inet_ntop, inet_pton
#include <netinet/in.h>   // for sockaddr_in
#include <sys/socket.h>   // for socket functions
#include <memory>         // for std::shared_ptr
#include <map>            // for std::map
#include <mutex>          // for std::mutex

// Trie implementation for efficient path prefix matching
class PathTrie {
private:
    struct TrieNode {
        bool isEndOfPrefix;
        SSInfo serverInfo;
        std::map<char, std::shared_ptr<TrieNode>> children;
        
        TrieNode() : isEndOfPrefix(false) {}
    };
    
    std::shared_ptr<TrieNode> root;
    
public:
    PathTrie() : root(std::make_shared<TrieNode>()) {}
    
    // Insert a path prefix and its associated server info
    void insert(const std::string& prefix, const SSInfo& info) {
        auto current = root;
        
        for (char c : prefix) {
            if (current->children.find(c) == current->children.end()) {
                current->children[c] = std::make_shared<TrieNode>();
            }
            current = current->children[c];
        }
        
        current->isEndOfPrefix = true;
        current->serverInfo = info;
    }
    
    // Mark a prefix as no longer valid (simpler than full node removal)
    // Returns true if the prefix was found and marked, false otherwise.
    bool markPrefixInvalid(const std::string& prefix) {
        auto current = root;
        for (char c : prefix) {
            if (current->children.find(c) == current->children.end()) {
                return false; // Prefix not found
            }
            current = current->children[c];
        }
        if (current->isEndOfPrefix) {
            current->isEndOfPrefix = false;
            return true;
        }
        return false; // Prefix exists but wasn't marked as end
    }

    // Find the longest matching prefix for a given path
    // Returns true if a match was found, and populates outInfo
    bool findLongestMatch(const std::string& path, SSInfo& outInfo) {
        auto current = root;
        auto lastMatch = root;
        bool foundMatch = false;
        size_t matchedLength = 0;
        size_t i = 0;
        
        for (i = 0; i < path.length(); ++i) {
            char c = path[i];
            
            if (current->children.find(c) == current->children.end()) {
                break; // No more matches possible
            }
            
            current = current->children[c];
            
            // Update last match if this node is an end of a prefix
            if (current->isEndOfPrefix) {
                lastMatch = current;
                foundMatch = true;
                matchedLength = i + 1;
                
                // Special handling for folder boundaries
                // A prefix /foo should match /foo/bar but not /foobar
                if (i + 1 < path.length() && path[i+1] != '/') {
                    // If we're at the end of a prefix and the next char isn't a '/',
                    // then this is only a valid match if the prefix ends with '/'
                    if (i > 0 && path[i] != '/') {
                        // Check if we just got lucky with partial matching
                        // For example, "/foobar" should not match "/foo"
                        // unless we're exactly at a path boundary
                        bool validBoundary = false;
                        if (i + 1 == path.length()) {
                            validBoundary = true; // End of path is a valid boundary
                        } else if (path[i+1] == '/') {
                            validBoundary = true; // Next char is '/' which is a valid boundary
                        }
                        
                        if (!validBoundary) {
                            // We found a match, but it's not at a valid path boundary,
                            // so we cannot use this match
                            continue;
                        }
                    }
                }
            }
        }
        
        if (foundMatch) {
            outInfo = lastMatch->serverInfo;
            return true;
        }
        
        return false;
    }
    
    // Get all registered prefixes for listing 
    std::vector<std::string> getAllPrefixes() {
        std::vector<std::string> prefixes;
        std::string currentPath;
        getAllPrefixesHelper(root, currentPath, prefixes);
        return prefixes;
    }
    
private:
    void getAllPrefixesHelper(std::shared_ptr<TrieNode> node, std::string& currentPath, std::vector<std::string>& prefixes) {
        if (node->isEndOfPrefix) {
            prefixes.push_back(currentPath);
        }
        
        for (const auto& pair : node->children) {
            currentPath.push_back(pair.first);
            getAllPrefixesHelper(pair.second, currentPath, prefixes);
            currentPath.pop_back();
        }
    }
};

// Use a trie instead of a vector for the registry
static PathTrie pathRegistry;

// Also keep track of the mapping from prefix to SSInfo for easy listing
static std::map<std::string, SSInfo> prefixRegistry;

// Mutex to protect access to the registries during updates
static std::mutex registryMutex;

// Structure to track async write operations
struct PendingWrite {
    int clientSocket;      // Socket to client awaiting notification
    std::string clientIP;
    int clientPort;
    bool notificationSent; // Has notification been sent?
};

// Map of write_id to client information
static std::map<std::string, PendingWrite> pendingWrites;
static std::mutex pendingWritesMutex;

// send a string (all at once) over sock
static void sendAll(int sock, const std::string& msg) {
    send(sock, msg.c_str(), msg.size(), 0);
}

// Updated findSSForPath to use the trie
static bool findSSForPath(const std::string& path, SSInfo& out_ssInfo) {
    return pathRegistry.findLongestMatch(path, out_ssInfo);
}

// Handle one client (either a REGISTER, LISTSERVERS, or forward request)
static void handleClient(int clientSock) {
    char buf[MAX_BUFFER];
    int n = recv(clientSock, buf, sizeof(buf)-1, 0);
    if (n <= 0) { 
        close(clientSock); 
        return; 
    }
    buf[n] = '\0';
    std::string req(buf);
    logMessage("NM RX: " + req);

    std::istringstream iss(req);
    std::string op;
    iss >> op;

    // 1) Dynamic registration from storage servers
    if (op == "REGISTER") {
        // Format: REGISTER <ip> <port> <comma,separated,prefixes>
        std::string ip, csv;
        int port;
        iss >> ip >> port >> csv;
        SSInfo serverInfo{ip, port};
        
        std::lock_guard<std::mutex> lock(registryMutex); // Lock before modifying registries

        std::istringstream pss(csv);
        std::string prefix;
        while (std::getline(pss, prefix, ',')) {
            // Ensure prefix starts with '/'
            if (prefix.empty() || prefix[0] != '/') {
                if (prefix.empty()) prefix = "/";
                else prefix = "/" + prefix;
            }
            pathRegistry.insert(prefix, serverInfo);
            prefixRegistry[prefix] = serverInfo;
            logMessage("Registered prefix '" + prefix + "' → " + ip + ":" + std::to_string(port));
        }
        sendAll(clientSock, "REGISTERED");
        close(clientSock);
        return;
    }

    // --- NEW: Handle DEREGISTER command ---
    if (op == "DEREGISTER") {
        std::string ip;
        int port;
        iss >> ip >> port;

        if (ip.empty() || port <= 0) {
            sendAll(clientSock, "ERROR: Invalid DEREGISTER format. Usage: DEREGISTER <ip> <port>");
            logMessage("NM: Invalid DEREGISTER command format.");
            close(clientSock);
            return;
        }

        logMessage("NM: Received DEREGISTER request from " + ip + ":" + std::to_string(port));
        int count = 0;

        { // Scope for the lock
            std::lock_guard<std::mutex> lock(registryMutex); // Lock before modifying registries

            // Iterate through the map to find prefixes associated with this server
            auto it = prefixRegistry.begin();
            while (it != prefixRegistry.end()) {
                if (it->second.ip == ip && it->second.port == port) {
                    logMessage("NM: Deregistering prefix '" + it->first + "'");
                    // Mark prefix as invalid in the trie
                    pathRegistry.markPrefixInvalid(it->first);
                    // Remove from the map
                    it = prefixRegistry.erase(it); // Erase and get next iterator
                    count++;
                } else {
                    ++it; // Move to the next element
                }
            }
        } // Lock released here

        std::string ack = "OK: Deregistered " + std::to_string(count) + " prefixes for " + ip + ":" + std::to_string(port);
        sendAll(clientSock, ack);
        logMessage("NM: " + ack);
        close(clientSock);
        return;
    }

    // Handle WRITE_STATUS updates from storage servers
    if (op == "WRITE_STATUS") {
        // Parse: WRITE_STATUS <writeID> <SUCCESS|FAILURE> <IP> <port> <filename> <bytes>
        std::string writeID, status, serverIP, filename;
        int serverPort;
        size_t bytes;
        
        iss >> writeID >> status >> serverIP >> serverPort >> filename >> bytes;
        
        if (writeID.empty() || (status != "SUCCESS" && status != "FAILURE")) {
            logMessage("NM: Invalid WRITE_STATUS format received");
            close(clientSock);
            return;
        }
        
        logMessage("NM: Received write status update - ID: " + writeID + ", Status: " + status);
        
        // Check if we have a pending write with this ID
        {
            std::lock_guard<std::mutex> lock(pendingWritesMutex);
            auto it = pendingWrites.find(writeID);
            if (it != pendingWrites.end() && !it->second.notificationSent) {
                PendingWrite& pending = it->second;
                
                // Check if we need to forward this to a connected client
                if (pending.clientSocket > 0) {
                    // Fix: Ensure string concatenation starts with std::string
                    std::string notification = std::string("WRITE_NOTIFICATION ") +
                                              (status == "SUCCESS" ? "OK: " : "ERROR: ") +
                                              "Async write to " + filename +
                                              " completed with status " + status +
                                              " (" + std::to_string(bytes) + " bytes)";
                                              
                    sendAll(pending.clientSocket, notification);
                    logMessage("NM: Forwarded write notification to client: " + notification);
                    
                    // In a real system, we might not want to close the client socket here
                    // if it's a persistent connection being used for multiple notifications
                    close(pending.clientSocket);
                    pending.clientSocket = -1;
                }
                
                pending.notificationSent = true;
                // Schedule cleanup of this entry after some time, or do it now
                pendingWrites.erase(it);
            }
        }
        
        // Acknowledge receipt to the storage server
        sendAll(clientSock, "OK: WRITE_STATUS received");
        close(clientSock);
        return;
    }

    // Handle WRITE with optional --SYNC flag
    if (op == "WRITE") {
        // Check if the second token is --SYNC
        std::string pathOrFlag;
        iss >> pathOrFlag;
        
        bool isSync = false;
        std::string path;
        
        if (pathOrFlag == "--SYNC") {
            isSync = true;
            // Get the actual path
            iss >> path;
        } else {
            // The second token was the path
            path = pathOrFlag;
        }
        
        if (path.empty()) {
            std::string err = "ERROR: Missing path for WRITE";
            sendAll(clientSock, err);
            logMessage("NM: " + err);
            close(clientSock);
            return;
        }
        
        // Add leading slash if not present
        if (path[0] != '/') {
            path = "/" + path;
        }
        
        // Rest of the message is the data
        std::string restOfMessage;
        std::getline(iss, restOfMessage);
        
        // Find the storage server for this path
        SSInfo target;
        bool found;
        { 
            std::lock_guard<std::mutex> lock(registryMutex);
            found = findSSForPath(path, target);
        }
        
        if (!found) {
            std::string err = "ERROR: No storage server for path " + path;
            sendAll(clientSock, err);
            logMessage("NM: " + err);
            close(clientSock);
            return;
        }
        
        // Create the full write command
        std::string writeCmd = "WRITE " + path;
        if (isSync) {
            writeCmd += " --SYNC"; // Add SYNC flag if requested
        }
        writeCmd += restOfMessage; // Add the data
        
        // At this point, we would forward the WRITE to the storage server as usual
        std::string out = "IP: " + target.ip + " PORT: " + std::to_string(target.port);
        sendAll(clientSock, out);
        logMessage("NM → client: " + out + (isSync ? " (SYNC write)" : " (ASYNC write)"));
        
        // For async writes, we could add client info to pendingWrites map here
        // But we would need a writeID, which is generated by the storage server
        // This would be handled by a separate client registration for notifications
        
        close(clientSock);
        return;
    }

    // 2) LISTSERVERS: list all registered mount‑point prefixes
    if (op == "LISTSERVERS") {
        std::string out = "SERVERS ";
        bool first = true;

        { // Scope for the lock
            std::lock_guard<std::mutex> lock(registryMutex); // Lock for reading registry
            for (const auto& pair : prefixRegistry) {
                if (!first) out += ",";
                out += pair.first;
                first = false;
            }
        } // Lock released

        logMessage("NM → client (LISTSERVERS): " + out);
        sendAll(clientSock, out);
        close(clientSock);
        return;
    }

    // --- Handle COPY command ---
    if (op == "COPY") {
        std::string sourcePath, destPath;
        iss >> sourcePath >> destPath;
        if (sourcePath.empty() || destPath.empty()) {
            sendAll(clientSock, "ERROR: Usage: COPY <source_path> <destination_path>");
            logMessage("NM: Invalid COPY command format.");
            close(clientSock);
            return;
        }
        // Trim whitespace from sourcePath and destPath
        auto trim = [](std::string &s) {
            while (!s.empty() && isspace(s.front())) s.erase(s.begin());
            while (!s.empty() && isspace(s.back())) s.pop_back();
        };
        trim(sourcePath);
        trim(destPath);
        // Ensure paths start with '/'
        if (sourcePath[0] != '/') sourcePath = "/" + sourcePath;
        if (destPath[0] != '/') destPath = "/" + destPath;
        
        SSInfo sourceSS, destSS;
        if (!findSSForPath(sourcePath, sourceSS)) {
            sendAll(clientSock, "ERROR: No storage server found for source path " + sourcePath);
            logMessage("NM: No SS for source path " + sourcePath);
            close(clientSock);
            return;
        }
        if (!findSSForPath(destPath, destSS)) {
            sendAll(clientSock, "ERROR: No storage server found for destination path " + destPath);
            logMessage("NM: No SS for destination path " + destPath);
            close(clientSock);
            return;
        }
        logMessage("NM: Source SS: " + sourceSS.ip + ":" + std::to_string(sourceSS.port));
        logMessage("NM: Dest SS: " + destSS.ip + ":" + std::to_string(destSS.port));
        
        int destSock = socket(AF_INET, SOCK_STREAM, 0);
        if (destSock < 0) {
             sendAll(clientSock, "ERROR: NM failed to create socket for COPY command.");
             logMessage("NM: Failed to create socket for PULL_COPY_FROM.");
             close(clientSock);
             return;
        }
        sockaddr_in destAddr{};
        destAddr.sin_family = AF_INET;
        destAddr.sin_port = htons(destSS.port);
        inet_pton(AF_INET, destSS.ip.c_str(), &destAddr.sin_addr);
        
        if (connect(destSock, (sockaddr*)&destAddr, sizeof(destAddr)) < 0) {
             perror("NM connect to Dest SS");
             sendAll(clientSock, "ERROR: NM failed to connect to destination storage server.");
             logMessage("NM: Failed to connect to Dest SS " + destSS.ip + ":" + std::to_string(destSS.port));
             close(destSock);
             close(clientSock);
             return;
        }
        
        // Build the pull command with exactly four tokens after the command keyword.
        std::string pullCmd = "PULL_COPY_FROM " + sourceSS.ip + " " + std::to_string(sourceSS.port)
                              + " " + sourcePath + " " + destPath;
        logMessage("NM: Sending to Dest SS: " + pullCmd);
        if (send(destSock, pullCmd.c_str(), pullCmd.size(), 0) < 0) {
             perror("NM send PULL_COPY_FROM");
             sendAll(clientSock, "ERROR: NM failed to send copy command to destination server.");
             logMessage("NM: Failed to send PULL_COPY_FROM to Dest SS.");
             close(destSock);
             close(clientSock);
             return;
        }
        
        char ackBuf[MAX_BUFFER];
        int ackBytes = recv(destSock, ackBuf, sizeof(ackBuf) - 1, 0);
        close(destSock);
        
        if (ackBytes <= 0) {
             std::string errMsg = "ERROR: No response from destination server after copy command.";
             if (ackBytes < 0) {
                 perror("NM recv ACK from Dest SS");
                 errMsg += " (recv error)";
             }
             sendAll(clientSock, errMsg);
             logMessage("NM: " + errMsg);
        } else {
             ackBuf[ackBytes] = '\0';
             std::string destSSResponse(ackBuf);
             logMessage("NM: Received from Dest SS: " + destSSResponse);
             sendAll(clientSock, destSSResponse);
        }
        close(clientSock);
        return;
    }

    // 3) All other ops (READ, WRITE, CREATE, DELETE, LIST <path>, STREAM, etc.)
    //    are forwarded: parse the path argument
    std::string path;
    iss >> path;
    if (path.empty()) {
        std::string err = "ERROR: Missing path for " + op;
        sendAll(clientSock, err);
        logMessage("NM: " + err);
        close(clientSock);
        return;
    }

    // Add leading slash if not present
    if (path[0] != '/') {
        path = "/" + path;
    }

    SSInfo target;
    bool found;
    { // Scope for lock
        std::lock_guard<std::mutex> lock(registryMutex); // Lock for reading registry (trie)
        found = findSSForPath(path, target); // Use the helper function (which uses trie)
    } // Lock released

    if (!found) {
        std::string err = "ERROR: No storage server for path " + path;
        sendAll(clientSock, err);
        logMessage("NM: " + err);
    } else {
        std::string out = "IP: " + target.ip +
                          " PORT: " + std::to_string(target.port);
        sendAll(clientSock, out);
        logMessage("NM → client: " + out);
    }

    close(clientSock);
}

int main(int argc, char* argv[]) {
    int port = 5055;
    if (argc == 2) {
        port = std::stoi(argv[1]);
    } else if (argc > 2) {
        std::cerr << "Usage: " << argv[0] << " [listen_port]\n";
        return 1;
    }

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        perror("socket");
        return 1;
    }

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);

    if (bind(fd, (sockaddr*)&addr, sizeof(addr)) < 0) {
        perror("bind");
        return 1;
    }
    if (listen(fd, 10) < 0) {
        perror("listen");
        return 1;
    }

    logMessage("Naming Server listening on port " + std::to_string(port));

    while (true) {
        int client = accept(fd, NULL, NULL);
        if (client < 0) {
            perror("accept");
            continue;
        }
        std::thread(handleClient, client).detach();
    }
}
