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
#include <list>
#include <unordered_map>
#include <chrono>
#include <future>
#include <set>
#include <atomic>
#include <condition_variable>
#include <algorithm>      // for std::find, std::remove
#include <random>         // for std::random_device, etc.

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

// LRU Cache for path lookups
class LRUCache {
private:
    // Maximum size of the cache
    size_t maxSize;
    
    // List of <path, serverInfo> pairs, ordered by recent access (most recent at front)
    std::list<std::pair<std::string, SSInfo>> accessList;
    
    // Map from path to iterator in the accessList for O(1) lookup and removal
    std::unordered_map<std::string, std::list<std::pair<std::string, SSInfo>>::iterator> cache;
    
    // Mutex to protect access to the cache data structures - make mutable to use in const methods
    mutable std::mutex cacheMutex;

public:
    LRUCache(size_t size) : maxSize(size) {}
    
    // Get server info for a path if it exists in the cache
    bool get(const std::string& path, SSInfo& outInfo) {
        std::lock_guard<std::mutex> lock(cacheMutex);
        
        auto it = cache.find(path);
        if (it == cache.end()) {
            // Not in cache
            return false;
        }
        
        // Found in cache, move to front (most recently used)
        accessList.splice(accessList.begin(), accessList, it->second);
        outInfo = it->second->second;
        return true;
    }
    
    // Put a path->serverInfo mapping into the cache
    void put(const std::string& path, const SSInfo& info) {
        std::lock_guard<std::mutex> lock(cacheMutex);
        
        // Check if already in cache
        auto it = cache.find(path);
        if (it != cache.end()) {
            // Move to front and update
            accessList.splice(accessList.begin(), accessList, it->second);
            it->second->second = info;
            return;
        }
        
        // Check if cache is full
        if (accessList.size() >= maxSize) {
            // Remove least recently used item
            auto lastItem = accessList.back().first;
            cache.erase(lastItem);
            accessList.pop_back();
        }
        
        // Add to front of list (most recently used)
        accessList.push_front({path, info});
        cache[path] = accessList.begin();
    }
    
    // Remove a path from the cache if it exists
    void remove(const std::string& path) {
        std::lock_guard<std::mutex> lock(cacheMutex);
        
        auto it = cache.find(path);
        if (it != cache.end()) {
            accessList.erase(it->second);
            cache.erase(it);
        }
    }
    
    // Clear the entire cache
    void clear() {
        std::lock_guard<std::mutex> lock(cacheMutex);
        accessList.clear();
        cache.clear();
    }
    
    // Get current size of cache
    size_t size() const {
        std::lock_guard<std::mutex> lock(cacheMutex);
        return accessList.size();
    }
};

// ServerStatus tracks the health of each Storage Server
struct ServerStatus {
    std::string ip;
    int port;
    std::atomic<bool> isAlive{true};
    std::chrono::steady_clock::time_point lastHeartbeat;
    std::vector<std::string> prefixes; // Paths this server is responsible for

    // Constructor
    ServerStatus(std::string ip_, int port_) : 
        ip(std::move(ip_)), 
        port(port_),
        lastHeartbeat(std::chrono::steady_clock::now()) {}
        
    // Get server identifier (IP:port)
    std::string getId() const {
        return ip + ":" + std::to_string(port);
    }
};

// Replication group tracks primary and replica servers for a path
struct ReplicationGroup {
    std::string primaryServer; // Primary server ID (IP:port)
    std::vector<std::string> replicaServers; // List of replica server IDs
    std::set<std::string> pendingWrites; // Tracks writes that are being replicated
};

// Use a trie instead of a vector for the registry
static PathTrie pathRegistry;

// Create a global LRU cache with a reasonable size (adjust as needed)
static LRUCache pathCache(500);  // Cache up to 500 path lookups

// Also keep track of the mapping from prefix to SSInfo for easy listing
static std::map<std::string, SSInfo> prefixRegistry;

// Mutex to protect access to the registries during updates
static std::mutex registryMutex;

// Global maps and structures for replication management
static std::map<std::string, std::shared_ptr<ServerStatus>> serverStatusMap; // Maps server ID to status
static std::map<std::string, ReplicationGroup> replicationGroups; // Maps path prefix to replication group
static std::mutex serverStatusMutex; // Protects server status map
static std::mutex replicationMutex; // Protects replication groups

// For heartbeat and failure detection
static std::atomic<bool> runHeartbeatThread{true};
static std::condition_variable heartbeatCV;
static std::mutex heartbeatMutex;

// Constants for failure detection
const int HEARTBEAT_INTERVAL_SEC = 5; // Check every 5 seconds
const int SERVER_TIMEOUT_SEC = 15;    // Mark as failed after 15 seconds of no response

// For replication
const int MIN_REPLICAS = 2; // Minimum number of replicas (in addition to primary)

// Forward declarations of new functions
static void startHeartbeatThread();
static bool checkServerAlive(const std::string& ip, int port);
static void handleServerFailure(const std::string& serverId);
static void replicateWriteAsync(const std::string& primaryServer, const std::vector<std::string>& replicaServers,
                               const std::string& path, const std::string& data, bool isSync);
static std::vector<std::string> selectReplicaServers(const std::string& primaryId, const std::string& pathPrefix);
static void setupReplicationGroups();
static void registerServerStatusAndUpdateReplication(const std::string& ip, int port, const std::vector<std::string>& prefixes);

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

// Updated findSSForPath to use the cache
static bool findSSForPath(const std::string& path, SSInfo& out_ssInfo) {
    // First check cache
    if (pathCache.get(path, out_ssInfo)) {
        // Found in cache
        return true;
    }
    
    // Not in cache, look up in trie
    bool found = pathRegistry.findLongestMatch(path, out_ssInfo);
    
    // If found, update cache
    if (found) {
        pathCache.put(path, out_ssInfo);
    }
    
    return found;
}

// Add a function to check if a server is alive
static bool checkServerAlive(const std::string& ip, int port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        logMessage("ERROR: Failed to create socket for heartbeat check");
        return false;
    }

    // Set socket timeout to avoid blocking too long
    struct timeval timeout;
    timeout.tv_sec = 2;  // 2 seconds timeout
    timeout.tv_usec = 0;
    if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
        logMessage("ERROR: Failed to set socket receive timeout");
    }
    if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) < 0) {
        logMessage("ERROR: Failed to set socket send timeout");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0) {
        logMessage("ERROR: Invalid IP address for heartbeat: " + ip);
        close(sock);
        return false;
    }

    if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
        // Failed to connect
        close(sock);
        return false;
    }

    // Send a simple heartbeat message - we'll use a custom HEARTBEAT command
    std::string heartbeatMsg = "HEARTBEAT";
    if (send(sock, heartbeatMsg.c_str(), heartbeatMsg.size(), 0) < 0) {
        close(sock);
        return false;
    }

    // Read response
    char buffer[128];
    int bytes = recv(sock, buffer, sizeof(buffer) - 1, 0);
    close(sock);

    if (bytes <= 0) {
        return false;
    }

    // If we got any response, consider the server alive
    return true;
}

// The main heartbeat thread function that periodically checks all servers
static void heartbeatThread() {
    logMessage("Starting heartbeat thread");
    
    while (runHeartbeatThread) {
        std::unique_lock<std::mutex> lock(heartbeatMutex);
        // Wait for the specified interval or until signaled to stop
        heartbeatCV.wait_for(lock, std::chrono::seconds(HEARTBEAT_INTERVAL_SEC), 
                            []{ return !runHeartbeatThread; });
        
        if (!runHeartbeatThread) break;
        
        // Check all servers
        std::vector<std::string> failedServers;
        
        {
            std::lock_guard<std::mutex> statusLock(serverStatusMutex);
            auto now = std::chrono::steady_clock::now();
            
            // Replace structured binding with iterator
            for (auto it = serverStatusMap.begin(); it != serverStatusMap.end(); ++it) {
                const std::string& serverId = it->first;
                auto& status = it->second;
                
                // Skip already marked as not alive
                if (!status->isAlive) continue;
                
                // Check if it's been too long since last successful heartbeat
                auto timeSinceLastHeartbeat = std::chrono::duration_cast<std::chrono::seconds>(
                    now - status->lastHeartbeat).count();
                
                if (timeSinceLastHeartbeat > SERVER_TIMEOUT_SEC) {
                    // Try to contact the server directly
                    if (!checkServerAlive(status->ip, status->port)) {
                        // Server is not responding
                        status->isAlive = false;
                        failedServers.push_back(serverId);
                        logMessage("Server " + serverId + " marked as failed (timeout)");
                    } else {
                        // Update last heartbeat time
                        status->lastHeartbeat = now;
                    }
                }
                else if (timeSinceLastHeartbeat > HEARTBEAT_INTERVAL_SEC) {
                    // Check server proactively before timeout
                    if (!checkServerAlive(status->ip, status->port)) {
                        // Server is not responding
                        status->isAlive = false;
                        failedServers.push_back(serverId);
                        logMessage("Server " + serverId + " marked as failed (no response)");
                    } else {
                        // Update last heartbeat time
                        status->lastHeartbeat = now;
                    }
                }
            }
        }
        
        // Handle failed servers outside the lock to avoid deadlocks
        for (const auto& serverId : failedServers) {
            handleServerFailure(serverId);
        }
    }
    
    logMessage("Heartbeat thread terminated");
}

// Initialize and start the heartbeat thread
static void startHeartbeatThread() {
    static std::thread heartbeatThreadInstance;
    static std::once_flag flag;
    
    std::call_once(flag, []() {
        runHeartbeatThread = true;
        heartbeatThreadInstance = std::thread(heartbeatThread);
        heartbeatThreadInstance.detach(); // Run independently
    });
}

// Handle a server failure
static void handleServerFailure(const std::string& serverId) {
    logMessage("Handling failure of server " + serverId);
    
    // 1. Update replication groups where this server was primary
    std::vector<std::string> affectedPrefixes;
    std::vector<std::string> prefixesToReassign;
    
    {
        std::lock_guard<std::mutex> repLock(replicationMutex);
        std::lock_guard<std::mutex> regLock(registryMutex);
        
        // Find all replication groups where this server was involved
        // Replace structured binding with iterator
        for (auto it = replicationGroups.begin(); it != replicationGroups.end(); ++it) {
            const std::string& prefix = it->first;
            ReplicationGroup& group = it->second;
            bool updateNeeded = false;
            
            // If server was primary, promote a replica
            if (group.primaryServer == serverId) {
                updateNeeded = true;
                affectedPrefixes.push_back(prefix);
                
                // Try to find a healthy replica to promote
                bool foundHealthyReplica = false;
                for (const auto& replicaId : group.replicaServers) {
                    std::string replicaIp = replicaId.substr(0, replicaId.find(':'));
                    int replicaPort = std::stoi(replicaId.substr(replicaId.find(':') + 1));
                    
                    auto serverIt = serverStatusMap.find(replicaId);
                    if (serverIt != serverStatusMap.end() && serverIt->second->isAlive) {
                        // Promote this replica to primary
                        group.primaryServer = replicaId;
                        foundHealthyReplica = true;
                        
                        // Update the trie and prefixRegistry
                        SSInfo newPrimaryInfo{replicaIp, replicaPort};
                        pathRegistry.insert(prefix, newPrimaryInfo);
                        prefixRegistry[prefix] = newPrimaryInfo;
                        
                        logMessage("Promoted " + replicaId + " to primary for prefix " + prefix);
                        break;
                    }
                }
                
                if (!foundHealthyReplica) {
                    // No healthy replica available, mark this prefix as needing reassignment
                    logMessage("WARNING: No healthy replica found for " + prefix);
                    prefixesToReassign.push_back(prefix);
                    
                    // Remove from prefixRegistry and mark invalid in trie
                    prefixRegistry.erase(prefix);
                    pathRegistry.markPrefixInvalid(prefix);
                }
            }
            
            // Remove the failed server from replica list if present
            auto& replicas = group.replicaServers;
            replicas.erase(
                std::remove_if(replicas.begin(), replicas.end(), 
                              [&serverId](const std::string& s) { return s == serverId; }),
                replicas.end());
            
            // If we removed a replica or changed primary, we need to find new replicas
            if (updateNeeded) {
                // Will handle outside locks to avoid deadlocks
            }
        }
    }
    
    // Clear the path cache since server assignments may have changed
    pathCache.clear();
    
    // 2. Find new replicas for groups that had this server as primary or replica
    for (const auto& prefix : affectedPrefixes) {
        std::lock_guard<std::mutex> repLock(replicationMutex);
        
        // Skip prefixes that need complete reassignment
        if (std::find(prefixesToReassign.begin(), prefixesToReassign.end(), prefix) != prefixesToReassign.end()) {
            continue;
        }
        
        auto& group = replicationGroups[prefix];
        if (group.replicaServers.size() < MIN_REPLICAS) {
            // Find new replicas to maintain replication factor
            auto newReplicas = selectReplicaServers(group.primaryServer, prefix);
            for (const auto& newReplica : newReplicas) {
                // Use std::find correctly
                if (std::find(group.replicaServers.begin(), group.replicaServers.end(), newReplica) == group.replicaServers.end()) {
                    group.replicaServers.push_back(newReplica);
                    logMessage("Added new replica " + newReplica + " for prefix " + prefix);
                }
                
                // Stop when we've reached the minimum number of replicas
                if (group.replicaServers.size() >= MIN_REPLICAS) break;
            }
        }
    }
    
    // 3. For prefixes that need reassignment, try to find a completely new server
    for (const auto& prefix : prefixesToReassign) {
        // This would need to find a completely new server to host this data
        logMessage("WARNING: Path prefix " + prefix + " is currently unavailable");
    }
}

// Select replica servers for a path
static std::vector<std::string> selectReplicaServers(const std::string& primaryId, const std::string& pathPrefix) {
    std::vector<std::string> selectedReplicas;
    std::vector<std::string> candidateServers;
    
    {
        std::lock_guard<std::mutex> lock(serverStatusMutex);
        
        // Collect all alive servers except the primary
        // Replace structured binding with iterator
        for (auto it = serverStatusMap.begin(); it != serverStatusMap.end(); ++it) {
            const std::string& serverId = it->first;
            const auto& status = it->second;
            
            if (serverId != primaryId && status->isAlive) {
                candidateServers.push_back(serverId);
            }
        }
    }
    
    // If there aren't enough servers, just return what we have
    if (candidateServers.size() <= MIN_REPLICAS) {
        return candidateServers;
    }
    
    // Replace random_shuffle with shuffle + random generator
    std::random_device rd;
    std::mt19937 g(rd());
    std::shuffle(candidateServers.begin(), candidateServers.end(), g);
    
    // Take the first MIN_REPLICAS elements
    candidateServers.resize(std::min(candidateServers.size(), static_cast<size_t>(MIN_REPLICAS)));
    return candidateServers;
}

// Set up replication groups for all registered prefixes
static void setupReplicationGroups() {
    std::lock_guard<std::mutex> regLock(registryMutex);
    std::lock_guard<std::mutex> repLock(replicationMutex);
    
    // Replace structured binding with iterator
    for (auto it = prefixRegistry.begin(); it != prefixRegistry.end(); ++it) {
        const std::string& prefix = it->first;
        const SSInfo& ssInfo = it->second;
        
        std::string primaryId = ssInfo.ip + ":" + std::to_string(ssInfo.port);
        
        // Skip if already in a replication group
        if (replicationGroups.find(prefix) != replicationGroups.end()) {
            continue;
        }
        
        // Create new replication group
        ReplicationGroup group;
        group.primaryServer = primaryId;
        
        // Select replica servers
        group.replicaServers = selectReplicaServers(primaryId, prefix);
        
        replicationGroups[prefix] = group;
        logMessage("Created replication group for " + prefix + " with primary " + primaryId);
    }
}

// Asynchronously replicate a write to all replica servers
static void replicateWriteAsync(const std::string& primaryServer, const std::vector<std::string>& replicaServers,
                               const std::string& path, const std::string& data, bool isSync) {
    // Lambda to run in a separate thread for each replica
    auto replicateToServer = [](const std::string& serverId, const std::string& path, 
                               const std::string& data, bool isSync) -> bool {
        // Parse server ID (IP:port)
        std::string ip = serverId.substr(0, serverId.find(':'));
        int port = std::stoi(serverId.substr(serverId.find(':') + 1));
        
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        if (sock < 0) {
            logMessage("ERROR: Failed to create socket for replication to " + serverId);
            return false;
        }
        
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port = htons(port);
        if (inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) <= 0) {
            logMessage("ERROR: Invalid IP for replication: " + ip);
            close(sock);
            return false;
        }
        
        if (connect(sock, (sockaddr*)&addr, sizeof(addr)) < 0) {
            logMessage("ERROR: Failed to connect to replica " + serverId);
            close(sock);
            return false;
        }
        
        // Build write command
        std::string writeCmd = "WRITE " + path;
        if (isSync) {
            writeCmd += " --SYNC";  // Forward sync flag
        }
        writeCmd += " " + data;
        
        if (send(sock, writeCmd.c_str(), writeCmd.size(), 0) < 0) {
            logMessage("ERROR: Failed to send write command to replica " + serverId);
            close(sock);
            return false;
        }
        
        // For simplicity, we'll just check for a response but not parse it in detail
        char respBuf[128];
        int bytes = recv(sock, respBuf, sizeof(respBuf) - 1, 0);
        close(sock);
        
        if (bytes <= 0) {
            logMessage("ERROR: No response from replica " + serverId);
            return false;
        }
        
        logMessage("Successfully replicated write to " + serverId);
        return true;
    };
    
    // For each replica, launch an async task
    for (const auto& replicaId : replicaServers) {
        std::thread([=]() {
            bool success = replicateToServer(replicaId, path, data, isSync);
            if (!success) {
                logMessage("WARNING: Write replication failed to " + replicaId);
                
                // Check if server is down
                std::string ip = replicaId.substr(0, replicaId.find(':'));
                int port = std::stoi(replicaId.substr(replicaId.find(':') + 1));
                
                if (!checkServerAlive(ip, port)) {
                    // Server seems down, update its status
                    {
                        std::lock_guard<std::mutex> lock(serverStatusMutex);
                        auto it = serverStatusMap.find(replicaId);
                        if (it != serverStatusMap.end()) {
                            it->second->isAlive = false;
                            // Schedule failure handling
                            std::thread([replicaId]() { handleServerFailure(replicaId); }).detach();
                        }
                    }
                }
            }
        }).detach();
    }
}

// Register a server in the status map and update replication
static void registerServerStatusAndUpdateReplication(const std::string& ip, int port, const std::vector<std::string>& prefixes) {
    std::string serverId = ip + ":" + std::to_string(port);
    
    {
        std::lock_guard<std::mutex> lock(serverStatusMutex);
        
        // Add or update server status
        auto it = serverStatusMap.find(serverId);
        if (it == serverStatusMap.end()) {
            // New server
            auto serverStatus = std::make_shared<ServerStatus>(ip, port);
            serverStatus->prefixes = prefixes;
            serverStatusMap[serverId] = serverStatus;
        } else {
            // Existing server - update heartbeat time and prefixes
            it->second->lastHeartbeat = std::chrono::steady_clock::now();
            it->second->isAlive = true;
            it->second->prefixes = prefixes;
        }
    }
    
    // Update replication groups
    setupReplicationGroups();
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
        std::string ip;
        int port;
        iss >> ip >> port;
        std::string csv;
        std::getline(iss, csv);
        if (!csv.empty() && csv[0] == ' ') csv.erase(0, 1);

        SSInfo serverInfo{ip, port};
        
        std::vector<std::string> serverPrefixes;
        {
            std::lock_guard<std::mutex> lock(registryMutex); // Lock for modifying registries

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
                serverPrefixes.push_back(prefix);
                logMessage("Registered prefix '" + prefix + "' → " + ip + ":" + std::to_string(port));
            }
            pathCache.clear();  // Clear cache as paths might resolve differently
        }
        // Move this call OUTSIDE the registryMutex lock to avoid deadlock
        registerServerStatusAndUpdateReplication(ip, port, serverPrefixes);

        sendAll(clientSock, "REGISTERED");
        startHeartbeatThread();
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

        pathCache.clear();  // Clear the cache as paths might now resolve differently
        std::string ack = "OK: Deregistered " + std::to_string(count) + " prefixes for " + ip + ":" + std::to_string(port);
        sendAll(clientSock, ack);
        logMessage("NM: " + ack);
        close(clientSock);
        return;
    }

    // Add a new handler for heartbeat responses
    if (op == "HEARTBEAT_RESPONSE") {
        std::string ip;
        int port;
        iss >> ip >> port;
        
        if (!ip.empty() && port > 0) {
            std::string serverId = ip + ":" + std::to_string(port);
            std::lock_guard<std::mutex> lock(serverStatusMutex);
            
            auto it = serverStatusMap.find(serverId);
            if (it != serverStatusMap.end()) {
                it->second->lastHeartbeat = std::chrono::steady_clock::now();
                it->second->isAlive = true;
                logMessage("Received heartbeat from " + serverId);
            }
        }
        
        sendAll(clientSock, "OK");
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

    // Modify WRITE handler to support replication
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
        std::string primaryServer;
        std::vector<std::string> replicaServers;
        
        { 
            std::lock_guard<std::mutex> regLock(registryMutex);
            found = findSSForPath(path, target);
            
            if (found) {
                // Identify replication group for this path
                std::lock_guard<std::mutex> repLock(replicationMutex);
                
                // Find the prefix that matches this path
                std::string matchedPrefix;
                for (auto it = prefixRegistry.begin(); it != prefixRegistry.end(); ++it) {
                    const std::string& prefix = it->first;
                    if (path.find(prefix) == 0) {
                        if (matchedPrefix.empty() || prefix.length() > matchedPrefix.length()) {
                            matchedPrefix = prefix;
                        }
                    }
                }
                
                if (!matchedPrefix.empty() && replicationGroups.find(matchedPrefix) != replicationGroups.end()) {
                    const auto& group = replicationGroups[matchedPrefix];
                    primaryServer = group.primaryServer;
                    replicaServers = group.replicaServers;
                }
            }
        }
        
        if (!found) {
            std::string err = "ERROR: No storage server for path " + path;
            sendAll(clientSock, err);
            logMessage("NM: " + err);
            close(clientSock);
            return;
        }
        
        // Check if primary server is alive
        {
            std::lock_guard<std::mutex> statusLock(serverStatusMutex);
            std::string serverId = target.ip + ":" + std::to_string(target.port);
            
            auto it = serverStatusMap.find(serverId);
            if (it != serverStatusMap.end() && !it->second->isAlive) {
                // Primary server is down, notify client
                std::string err = "ERROR: Primary server for path " + path + " is currently unavailable";
                sendAll(clientSock, err);
                logMessage("NM: " + err);
                close(clientSock);
                return;
            }
        }
        
        // Create the full write command
        std::string writeCmd = "WRITE " + path;
        if (isSync) {
            writeCmd += " --SYNC"; // Add SYNC flag if requested
        }
        writeCmd += restOfMessage; // Add the data
        
        // At this point, we forward the WRITE to the primary storage server
        std::string out = "IP: " + target.ip + " PORT: " + std::to_string(target.port);
        sendAll(clientSock, out);
        logMessage("NM → client: " + out + (isSync ? " (SYNC write)" : " (ASYNC write)"));
        
        // Asynchronously replicate the write to all replica servers
        if (!replicaServers.empty()) {
            // Use the data from restOfMessage for replication
            replicateWriteAsync(primaryServer, replicaServers, path, restOfMessage, isSync);
        }
        
        close(clientSock);
        return;
    }

    // Modify READ handler to support failover to replicas
    if (op == "READ" || op == "STREAM") {
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
        std::string matchedPrefix;
        std::vector<std::string> replicaServerIds;

        { // Scope for lock
            std::lock_guard<std::mutex> lock(registryMutex); // Lock for reading registry (trie)
            found = findSSForPath(path, target); // Use the helper function (which uses trie)

            if (found) {
                // Find matching prefix and replication group
                for (auto it = prefixRegistry.begin(); it != prefixRegistry.end(); ++it) {
                    const std::string& prefix = it->first;
                    if (path.find(prefix) == 0 && (matchedPrefix.empty() || prefix.length() > matchedPrefix.length())) {
                        matchedPrefix = prefix;
                    }
                }
            }
        }
        
        // If found a prefix, check if the primary server is alive, otherwise try replicas
        bool useReplica = false;
        std::string selectedServer;
        
        if (found && !matchedPrefix.empty()) {
            std::string primaryId = target.ip + ":" + std::to_string(target.port);
            
            {
                std::lock_guard<std::mutex> statusLock(serverStatusMutex);
                
                auto it = serverStatusMap.find(primaryId);
                if (it != serverStatusMap.end() && !it->second->isAlive) {
                    // Primary is down, need to use a replica
                    useReplica = true;
                    
                    {
                        std::lock_guard<std::mutex> repLock(replicationMutex);
                        
                        auto groupIt = replicationGroups.find(matchedPrefix);
                        if (groupIt != replicationGroups.end()) {
                            // Find a healthy replica
                            for (const auto& replicaId : groupIt->second.replicaServers) {
                                auto replicaIt = serverStatusMap.find(replicaId);
                                if (replicaIt != serverStatusMap.end() && replicaIt->second->isAlive) {
                                    // Found a healthy replica
                                    selectedServer = replicaId;
                                    std::string ip = replicaId.substr(0, replicaId.find(':'));
                                    int port = std::stoi(replicaId.substr(replicaId.find(':') + 1));
                                    target.ip = ip;
                                    target.port = port;
                                    break;
                                }
                            }
                        }
                    }
                }
            }
        }
        
        if (!found || (useReplica && selectedServer.empty())) {
            std::string err;
            if (!found) {
                err = "ERROR: No storage server for path " + path;
            } else {
                err = "ERROR: Primary server is down and no healthy replica is available for path " + path;
            }
            sendAll(clientSock, err);
            logMessage("NM: " + err);
            close(clientSock);
            return;
        }

        std::string out = "IP: " + target.ip + " PORT: " + std::to_string(target.port);
        if (useReplica) {
            out += " (REPLICA)";  // Indicate we're using a replica
        }
        sendAll(clientSock, out);
        logMessage("NM → client: " + out + (useReplica ? " (Using replica due to primary failure)" : ""));
        
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
    
    // Start heartbeat thread for failure detection
    startHeartbeatThread();

    while (true) {
        int client = accept(fd, NULL, NULL);
        if (client < 0) {
            perror("accept");
            continue;
        }
        std::thread(handleClient, client).detach();
    }
}

