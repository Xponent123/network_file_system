#include "common.h"
#include <iostream>
#include <thread>
#include <map>
#include <fstream>
#include <sstream>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <cstring>
#include <cerrno>
#include <ctime>      // For formatting timestamps
#include <csignal>    // For signal handling
#include <future>     // For async operations
#include <unordered_map> // For tracking async writes
#include <mutex>      // For thread safety
#include <sys/stat.h> // For filesystem operations
#include <iomanip>    // For formatting time

// --- Global variables for signal handler ---
static std::string g_nmIP;
static int g_nmPort = 0;
static std::string g_myIP;
static int g_myPort = 0;
volatile sig_atomic_t stopFlag = 0; // Flag to signal termination

// Global tracking for async writes
struct AsyncWriteInfo {
    std::string clientIP;
    int clientPort;
    std::string filename;
    size_t dataSize;
    std::string writeID; // Unique ID for this write operation
};

// Map of active async writes 
static std::unordered_map<std::string, AsyncWriteInfo> activeAsyncWrites;
static std::mutex asyncWritesMutex;

// Generate a unique write ID
static std::string generateWriteID() {
    static int counter = 0;
    return "write_" + std::to_string(std::time(nullptr)) + "_" + 
           std::to_string(++counter) + "_" + std::to_string(rand() % 10000);
}

// Send write completion notification to Naming Server
static void notifyWriteCompletion(const std::string& writeID, bool success, 
                                  const std::string& filename, size_t bytes) {
    // Don't notify if NM info is missing
    if (g_nmIP.empty() || g_nmPort <= 0) {
        logMessage("ERROR: Cannot notify write completion, missing NM info");
        return;
    }
    
    // Create socket and connect to NM
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        logMessage("ERROR: Failed to create socket for write notification");
        return;
    }
    
    sockaddr_in nmAddr{};
    nmAddr.sin_family = AF_INET;
    nmAddr.sin_port = htons(g_nmPort);
    if (inet_pton(AF_INET, g_nmIP.c_str(), &nmAddr.sin_addr) <= 0) {
        logMessage("ERROR: Invalid NM IP for write notification");
        close(sock);
        return;
    }
    
    if (connect(sock, (sockaddr*)&nmAddr, sizeof(nmAddr)) < 0) {
        logMessage("ERROR: Failed to connect to NM for write notification");
        close(sock);
        return;
    }
    
    // Prepare notification message
    std::string status = success ? "SUCCESS" : "FAILURE";
    std::string notification = "WRITE_STATUS " + writeID + " " + status + " " + 
                               g_myIP + " " + std::to_string(g_myPort) + " " + 
                               filename + " " + std::to_string(bytes);
    
    // Send notification
    if (send(sock, notification.c_str(), notification.size(), 0) < 0) {
        logMessage("ERROR: Failed to send write notification to NM");
    } else {
        logMessage("Sent write completion notification to NM: " + notification);
    }
    
    close(sock);
}

// --- Signal Handler ---
void signalHandler(int signum) {
    logMessage("Interrupt signal (" + std::to_string(signum) + ") received.");
    stopFlag = 1; // Set flag to allow graceful shutdown in main loop if possible

    // Attempt to deregister from Naming Server
    if (!g_nmIP.empty() && g_nmPort > 0 && !g_myIP.empty() && g_myPort > 0) {
        logMessage("Attempting to deregister from Naming Server...");
        int deregSock = socket(AF_INET, SOCK_STREAM, 0);
        if (deregSock >= 0) {
            sockaddr_in nmAddr{};
            nmAddr.sin_family = AF_INET;
            nmAddr.sin_port   = htons(g_nmPort);
            inet_pton(AF_INET, g_nmIP.c_str(), &nmAddr.sin_addr);

            if (connect(deregSock, (sockaddr*)&nmAddr, sizeof(nmAddr)) == 0) {
                std::string deregMsg = "DEREGISTER " + g_myIP + " " + std::to_string(g_myPort);
                send(deregSock, deregMsg.c_str(), deregMsg.size(), 0);
                logMessage("Sent deregister command: " + deregMsg);
                char ackBuf[128];
                recv(deregSock, ackBuf, sizeof(ackBuf)-1, 0); // Try to receive ACK
            } else {
                logMessage("ERROR: Failed to connect to Naming Server for deregistration.");
                perror("connect to NM for deregister");
            }
            close(deregSock);
        } else {
             logMessage("ERROR: Failed to create socket for deregistration.");
        }
    } else {
        logMessage("Skipping deregistration (missing server info).");
    }

    logMessage("Storage Server shutting down.");
    exit(signum); // Exit with the signal number
}

// Added helper functions to replace filesystem namespace functions
static bool path_exists(const std::string& path) {
    struct stat buffer;
    return (stat(path.c_str(), &buffer) == 0);
}

static bool is_directory(const std::string& path) {
    struct stat buffer;
    if (stat(path.c_str(), &buffer) != 0) return false;
    return S_ISDIR(buffer.st_mode);
}

static std::string get_parent_path(const std::string& path) {
    size_t last_slash = path.find_last_of('/');
    if (last_slash == std::string::npos) return ".";
    if (last_slash == 0) return "/"; // Root directory
    return path.substr(0, last_slash);
}

static bool create_directories(const std::string& dirPath) {
    size_t pos = 0;
    std::string path = dirPath;
    
    // Handle absolute paths
    if (path[0] == '/') pos = 1;
    
    while ((pos = path.find('/', pos)) != std::string::npos) {
        std::string subPath = path.substr(0, pos);
        if (!subPath.empty()) {
            if (mkdir(subPath.c_str(), 0755) != 0 && errno != EEXIST) {
                return false;
            }
        }
        pos++;
    }
    
    // Create the final directory if path doesn't end with '/'
    if (path.back() != '/' && !path.empty()) {
        if (mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
            return false;
        }
    }
    
    return true;
}

// Helper function to format time_t into a readable string
static std::string formatTime(time_t time) {
    char buf[80];
    struct tm timeinfo;
    localtime_r(&time, &timeinfo); // Use thread-safe localtime_r
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &timeinfo);
    return std::string(buf);
}

// Helper function to format permissions (similar to ls -l)
static std::string formatPermissions(mode_t mode) {
    std::string perms = "----------";
    if (S_ISDIR(mode)) perms[0] = 'd';
    if (mode & S_IRUSR) perms[1] = 'r';
    if (mode & S_IWUSR) perms[2] = 'w';
    if (mode & S_IXUSR) perms[3] = 'x';
    if (mode & S_IRGRP) perms[4] = 'r';
    if (mode & S_IWGRP) perms[5] = 'w';
    if (mode & S_IXGRP) perms[6] = 'x';
    if (mode & S_IROTH) perms[7] = 'r';
    if (mode & S_IWOTH) perms[8] = 'w';
    if (mode & S_IXOTH) perms[9] = 'x';
    return perms;
}

// Helper function: Copy a single file from source to destination
static bool copyFile(const std::string& sourceIp, int sourcePort,
                   const std::string& sourcePath, const std::string& destFullPath,
                   long long& totalBytes) {
    logMessage("Copying file from " + sourcePath + " to " + destFullPath);
    
    // 1. Connect to source server
    int sourceSock = socket(AF_INET, SOCK_STREAM, 0);
    if (sourceSock < 0) {
        logMessage("ERROR: Failed to create socket for file copy - " + std::string(strerror(errno)));
        return false;
    }
    
    sockaddr_in sourceAddr{};
    sourceAddr.sin_family = AF_INET;
    sourceAddr.sin_port = htons(sourcePort);
    if (inet_pton(AF_INET, sourceIp.c_str(), &sourceAddr.sin_addr) <= 0) {
        logMessage("ERROR: Invalid source IP for file copy");
        close(sourceSock);
        return false;
    }
    
    if (connect(sourceSock, (sockaddr*)&sourceAddr, sizeof(sourceAddr)) < 0) {
        logMessage("ERROR: Failed to connect to source server for file copy - " + std::string(strerror(errno)));
        close(sourceSock);
        return false;
    }
    
    // 2. Send READ command
    std::string readCmd = "READ " + sourcePath;
    if (send(sourceSock, readCmd.c_str(), readCmd.size(), 0) < 0) {
        logMessage("ERROR: Failed to send READ command for file copy - " + std::string(strerror(errno)));
        close(sourceSock);
        return false;
    }
    
    // 3. Receive the *first* chunk to check for errors BEFORE opening the output file
    char initialBuffer[2048];
    int initialBytesReceived = recv(sourceSock, initialBuffer, sizeof(initialBuffer), 0);

    if (initialBytesReceived <= 0) {
        if (initialBytesReceived < 0) {
            logMessage("ERROR: Failed receiving initial data for file copy - " + std::string(strerror(errno)));
        } else {
            logMessage("ERROR: Received empty response from source server for READ " + sourcePath);
        }
        close(sourceSock);
        return false; // Indicate failure
    }

    // Check if the initial response is an error message from the source server
    std::string initialResponse(initialBuffer, initialBytesReceived);
    if (initialResponse.rfind("ERROR:", 0) == 0) {
        logMessage("ERROR: Source server failed to read file: " + sourcePath + " - " + initialResponse);
        close(sourceSock);
        return false; // Source reported error, fail the copy
    }

    // 4. Ensure destination directory exists *before* opening the file
    std::string destDir = get_parent_path(destFullPath);
    if (!path_exists(destDir)) {
        logMessage("Destination directory " + destDir + " does not exist, attempting to create.");
        if (!create_directories(destDir)) {
            logMessage("ERROR: Failed to create destination directory: " + destDir);
            close(sourceSock);
            return false;
        }
    } else if (!is_directory(destDir)) {
        logMessage("ERROR: Destination path's parent is not a directory: " + destDir);
        close(sourceSock);
        return false;
    }

    // 5. Open destination file *only after confirming no initial error and dir exists*
    std::ofstream outFile(destFullPath, std::ios::binary | std::ios::trunc);
    if (!outFile) {
        logMessage("ERROR: Cannot open destination file: " + destFullPath + " - " + strerror(errno));
        close(sourceSock);
        return false;
    }

    // 5. Write the initial chunk and stream the rest
    outFile.write(initialBuffer, initialBytesReceived);
    if (!outFile) {
        logMessage("ERROR: Failed writing initial chunk to destination file: " + destFullPath);
        outFile.close();
        close(sourceSock);
        return false;
    }
    long long fileSize = initialBytesReceived;

    char streamBuffer[2048];
    int bytesReceived;
    while ((bytesReceived = recv(sourceSock, streamBuffer, sizeof(streamBuffer), 0)) > 0) {
        outFile.write(streamBuffer, bytesReceived);
        if (!outFile) {
            logMessage("ERROR: Failed writing chunk to destination file: " + destFullPath);
            outFile.close();
            close(sourceSock);
            return false;
        }
        fileSize += bytesReceived;
    }

    outFile.close();
    close(sourceSock);

    if (bytesReceived < 0) {
        logMessage("ERROR: Failed receiving subsequent data for file copy - " + std::string(strerror(errno)));
        return false;
    }

    // Update total bytes copied
    totalBytes += fileSize;
    return true;
}

// Helper function: Copy directory recursively
static bool copyDirectoryRecursive(const std::string& sourceIp, int sourcePort, 
                                 const std::string& sourcePath, const std::string& destPath,
                                 const std::string& sharedPath, long long& totalBytes) {
    // Convert source and dest to relative paths for storage server requests
    std::string sourceRelPath = sourcePath;
    std::string destRelPath = destPath;
    
    // Get directory listing from source *first*
    int listSock = socket(AF_INET, SOCK_STREAM, 0);
    if (listSock < 0) {
        logMessage("ERROR: Failed to create socket for directory listing - " + std::string(strerror(errno)));
        return false;
    }
    
    sockaddr_in sourceAddr{};
    sourceAddr.sin_family = AF_INET;
    sourceAddr.sin_port = htons(sourcePort);
    if (inet_pton(AF_INET, sourceIp.c_str(), &sourceAddr.sin_addr) <= 0) {
        logMessage("ERROR: Invalid source IP for directory listing");
        close(listSock);
        return false;
    }
    
    if (connect(listSock, (sockaddr*)&sourceAddr, sizeof(sourceAddr)) < 0) {
        logMessage("ERROR: Failed to connect to source server for directory listing - " + std::string(strerror(errno)));
        close(listSock);
        return false;
    }
    
    // Send LIST command
    std::string listCmd = "LIST " + sourceRelPath;
    if (send(listSock, listCmd.c_str(), listCmd.size(), 0) < 0) {
        logMessage("ERROR: Failed to send LIST command - " + std::string(strerror(errno)));
        close(listSock);
        return false;
    }

    // Receive directory listing response
    char listBuffer[8192];
    std::string listing = "";
    int bytesReceived;

    while ((bytesReceived = recv(listSock, listBuffer, sizeof(listBuffer) - 1, 0)) > 0) {
        listBuffer[bytesReceived] = '\0';
        listing.append(listBuffer, bytesReceived);
    }
    close(listSock);

    if (bytesReceived < 0) {
         logMessage("ERROR: Failed to receive directory listing - " + std::string(strerror(errno)));
         return false;
    }
    if (listing.empty()) {
         logMessage("ERROR: Received empty directory listing response for " + sourceRelPath);
         return false;
    }

    if (listing.rfind("ERROR:", 0) == 0) {
        logMessage("ERROR: Source server failed to list directory: " + sourceRelPath + " - " + listing);
        return false;
    }
    if (listing.rfind("OK:", 0) != 0) {
        logMessage("ERROR: Received unexpected LIST response from source: " + listing);
        return false;
    }

    // Create destination directory *only after confirming source exists and is listable*
    std::string destFullPath = sharedPath;
    // Ensure destRelPath is handled correctly relative to sharedPath
    if (!destRelPath.empty()) {
        if (destRelPath[0] == '/') {
             destFullPath += destRelPath;
        } else {
             destFullPath += "/" + destRelPath;
        }
    }

    // Use our create_directories function instead of std::filesystem
    if (!create_directories(destFullPath)) {
        // Check if it failed because the path already exists and is not a directory
        if (path_exists(destFullPath) && !is_directory(destFullPath)) {
             logMessage("ERROR: Destination path exists but is not a directory: " + destFullPath);
             return false;
        }
        // If path exists and is a directory, this is fine
        if (!path_exists(destFullPath)) {
             logMessage("ERROR: Failed to create destination directory: " + destFullPath);
             return false;
        }
    } else {
        logMessage("Created destination directory: " + destFullPath);
    }

    // Parse listing and process entries
    std::istringstream iss(listing);
    std::string line;
    bool headerSkipped = false;

    while (std::getline(iss, line)) {
        if (!headerSkipped) {
            headerSkipped = true;
            continue;
        }
        if (line.empty() || line == "Directory is empty.")
            continue;

        std::string entrySourcePath = sourceRelPath + "/" + line;
        std::string entryDestPath = destRelPath + "/" + line;

        if (entrySourcePath[0] != '/') entrySourcePath = "/" + entrySourcePath;
        if (entryDestPath[0] != '/') entryDestPath = "/" + entryDestPath;

        int checkSock = socket(AF_INET, SOCK_STREAM, 0);
        if (checkSock < 0) continue;

        if (connect(checkSock, (sockaddr*)&sourceAddr, sizeof(sourceAddr)) < 0) {
            close(checkSock);
            continue;
        }

        std::string checkCmd = "LIST " + entrySourcePath;
        send(checkSock, checkCmd.c_str(), checkCmd.size(), 0);

        char checkBuf[128];
        int checkBytes = recv(checkSock, checkBuf, sizeof(checkBuf) - 1, 0);
        close(checkSock);

        bool isSubDir = false;
        if (checkBytes > 0) {
            checkBuf[checkBytes] = '\0';
            std::string checkResp(checkBuf);
            if (checkResp.rfind("ERROR:", 0) != 0) {
                isSubDir = true;
            }
        }

        if (isSubDir) {
            if (!copyDirectoryRecursive(sourceIp, sourcePort, entrySourcePath, entryDestPath, sharedPath, totalBytes)) {
                logMessage("WARNING: Failed to copy subdirectory: " + entrySourcePath);
            }
        } else {
            std::string entryDestFullPath = destFullPath + "/" + line;
            if (!copyFile(sourceIp, sourcePort, entrySourcePath, entryDestFullPath, totalBytes)) {
                logMessage("WARNING: Failed to copy file: " + entrySourcePath);
            }
        }
    }

    return true;
}

void handleClient(int clientSocket, const std::string& sharedPath) {
    char buffer[2048] = {0};
    int bytesRead = recv(clientSocket, buffer, sizeof(buffer)-1, 0);
    if (bytesRead <= 0) {
        close(clientSocket);
        return;
    }
    buffer[bytesRead] = '\0';
    std::string command(buffer);

    logMessage("Received command: " + command);

    std::istringstream iss(command);
    std::string op, firstArg; // Use firstArg for filename or source IP
    iss >> op >> firstArg;

    // Add handler for heartbeat messages
    if (op == "HEARTBEAT") {
        std::string response = "HEARTBEAT_RESPONSE " + g_myIP + " " + std::to_string(g_myPort);
        send(clientSocket, response.c_str(), response.size(), 0);
        logMessage("Sent heartbeat response");
        close(clientSocket);
        return;
    }

    // --- Existing Command Handling ---
    std::string filename = firstArg; // Use the parsed first argument as filename

    // Strip leading '/' from filename if present (for non-PULL commands)
    if (!filename.empty() && filename[0] == '/') {
        filename.erase(0, 1);
    }

    if (op == "READ") {
        if (filename.empty()) {
            std::string err = "ERROR: Invalid READ. Usage: READ <filename>";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }

        std::string fullPath = sharedPath + "/" + filename;
        struct stat st;
        if (stat(fullPath.c_str(), &st) != 0) { // Check existence
            std::string err = "ERROR: File not found or cannot access: " + filename + " (" + strerror(errno) + ")";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }
        if (S_ISDIR(st.st_mode)) { // Check if it's a directory
             std::string err = "ERROR: Path is a directory, not a file: " + filename;
             send(clientSocket, err.c_str(), err.size(), 0);
             logMessage(err);
             close(clientSocket);
             return;
        }

        std::ifstream file(fullPath, std::ios::binary);
        if (!file) {
            // This check might be redundant due to stat, but kept for safety
            std::string err = "ERROR: Cannot open file for reading: " + filename;
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }

        logMessage("Sending file: " + filename);
        char fileBuffer[2048];
        while (file.read(fileBuffer, sizeof(fileBuffer))) {
            if (send(clientSocket, fileBuffer, file.gcount(), 0) < 0) {
                perror("send file data");
                logMessage("ERROR: Failed sending file data for " + filename);
                file.close(); // Close file stream
                close(clientSocket); // Close socket on error
                return;
            }
        }
        // Send the last chunk if any
        if (file.gcount() > 0) {
             if (send(clientSocket, fileBuffer, file.gcount(), 0) < 0) {
                 perror("send last file chunk");
                 logMessage("ERROR: Failed sending last file chunk for " + filename);
                 file.close(); // Close file stream
                 close(clientSocket); // Close socket on error
                 return;
             }
        }

        file.close(); // Close file stream
        logMessage("Finished sending file: " + filename);
    }
    else if (op == "WRITE") {
        // filename already contains the path from firstArg

        // Check if --SYNC flag is present immediately after the path
        bool syncWrite = false;
        std::string potentialFlag;
        std::streampos flagPos = iss.tellg(); // Remember position before reading flag
        iss >> potentialFlag;
        if (potentialFlag == "--SYNC") {
            syncWrite = true;
        } else {
            // It wasn't the flag, reset stream to read data from where the path ended
            iss.clear();
            iss.seekg(flagPos);
        }

        if (filename.empty()) {
            std::string err = "ERROR: Invalid WRITE. Missing path.";
            if (send(clientSocket, err.c_str(), err.size(), 0) < 0) { perror("send write error"); }
            logMessage(err);
            close(clientSocket);
            return;
        }

        // The rest of the stream is data
        std::string data;
        std::getline(iss, data);
        // Strip leading space if present (getline might include it)
        if (!data.empty() && data[0] == ' ')
            data.erase(0, 1);

        std::string fullPath = sharedPath + "/" + filename;

        // Check parent directory first
        std::string destDir = get_parent_path(fullPath);
        if (!path_exists(destDir)) {
            std::string err = "ERROR: Parent directory does not exist: " + destDir;
            if (send(clientSocket, err.c_str(), err.size(), 0) < 0) { perror("send write error"); }
            logMessage(err);
            close(clientSocket);
            return;
        }
        if (!is_directory(destDir)) {
            std::string err = "ERROR: Parent path is not a directory: " + destDir;
            if (send(clientSocket, err.c_str(), err.size(), 0) < 0) { perror("send write error"); }
            logMessage(err);
            close(clientSocket);
            return;
        }

        if (syncWrite) {
            // --- Synchronous Write ---
            logMessage("Performing synchronous write to: " + filename);
            std::ofstream out(fullPath, std::ios::binary); // Overwrite
            if (!out) {
                std::string err = "ERROR: Cannot open file for writing: " + filename;
                if (send(clientSocket, err.c_str(), err.size(), 0) < 0) { perror("send sync write error"); }
                logMessage(err);
                close(clientSocket);
                return;
            }

            out << data;
            bool writeSuccess = out.good();
            out.close();

            if (!writeSuccess) {
                std::string err = "ERROR: Failed to write data to file: " + filename;
                if (send(clientSocket, err.c_str(), err.size(), 0) < 0) { perror("send sync write error"); }
                logMessage(err);
                close(clientSocket);
                return;
            }

            std::string ack = "OK: Synchronously wrote " + std::to_string(data.size()) + " bytes to " + filename;
            if (send(clientSocket, ack.c_str(), ack.size(), 0) < 0) {
                 perror("send sync write ack");
                 logMessage("ERROR: Failed sending sync write ack for " + filename);
            } else {
                 logMessage(ack);
            }

        } else {
            // --- Asynchronous Write ---
            logMessage("Starting asynchronous write to: " + filename);
            std::string writeID = generateWriteID();
            {
                std::lock_guard<std::mutex> lock(asyncWritesMutex);
                activeAsyncWrites[writeID] = { g_nmIP, g_nmPort, filename, data.size(), writeID };
            }

            // Copy data needed for the detached thread
            std::string asyncFilePath = fullPath;
            std::string asyncData = data;
            std::string asyncFilename = filename;
            std::string asyncWriteID = writeID;

            // Launch background thread
            std::thread([asyncFilePath, asyncData, asyncFilename, asyncWriteID]() {
                std::ofstream out(asyncFilePath, std::ios::binary); // Overwrite file
                bool success = false;
                size_t bytesWritten = 0;
                if (out) {
                    out << asyncData;
                    success = out.good();
                    bytesWritten = asyncData.size(); // Assume all written if stream is good
                    out.close();
                    if (success) {
                        logMessage("Async write completed: " + std::to_string(bytesWritten) + " bytes to " + asyncFilename);
                    } else {
                        logMessage("ERROR [Async]: Failed while writing to file: " + asyncFilename);
                        bytesWritten = 0; // Indicate 0 bytes on failure
                    }
                } else {
                    logMessage("ERROR [Async]: Failed to open file for writing: " + asyncFilename);
                    success = false;
                    bytesWritten = 0;
                }
                // Notify NM about completion status
                notifyWriteCompletion(asyncWriteID, success, asyncFilename, bytesWritten);
                // Clean up tracking info
                {
                    std::lock_guard<std::mutex> lock(asyncWritesMutex);
                    activeAsyncWrites.erase(asyncWriteID);
                }
            }).detach();

            // Send immediate acknowledgment to client
            std::string ack = "OK: Async write of " + std::to_string(data.size()) + " bytes to " + filename + " started (ID: " + writeID + ")";
             if (send(clientSocket, ack.c_str(), ack.size(), 0) < 0) {
                 perror("send async write ack");
                 logMessage("ERROR: Failed sending async write ack for " + filename);
             } else {
                 logMessage(ack);
             }
        }

        // Close socket after handling WRITE (sync or async ack sent)
        close(clientSocket);
        return; // Return early as WRITE handler manages its own socket closing
    }
    else if (op == "CREATE") {
        if (filename.empty()) {
            std::string err = "ERROR: Invalid CREATE. Usage: CREATE <path> [FILE|FOLDER]";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }

        std::string type = "FILE"; // Default to FILE
        iss >> type; // Try to read type

        std::string fullPath = sharedPath + "/" + filename;

        // Check if parent directory exists
        std::string parentDir = get_parent_path(fullPath);
         if (!path_exists(parentDir)) {
             std::string err = "ERROR: Parent directory does not exist: " + parentDir;
             send(clientSocket, err.c_str(), err.size(), 0);
             logMessage(err);
             close(clientSocket);
             return;
         }
         if (!is_directory(parentDir)) {
             std::string err = "ERROR: Parent path is not a directory: " + parentDir;
             send(clientSocket, err.c_str(), err.size(), 0);
             logMessage(err);
             close(clientSocket);
             return;
         }

        // Check if path already exists
        if (path_exists(fullPath)) {
            std::string err = "ERROR: Path already exists: " + filename;
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }

        bool success = false;
        std::string successMsg;
        std::string errorMsg;

        if (type == "FOLDER") {
            if (mkdir(fullPath.c_str(), 0755) == 0) {
                success = true;
                successMsg = "OK: Created folder " + filename;
            } else {
                errorMsg = "ERROR: Failed to create folder " + filename + " (" + strerror(errno) + ")";
            }
        } else { // Default to FILE
            // Create an empty file
            std::ofstream outFile(fullPath);
            if (outFile) {
                outFile.close(); // Close immediately
                success = true;
                successMsg = "OK: Created file " + filename;
            } else {
                errorMsg = "ERROR: Failed to create file " + filename + " (" + strerror(errno) + ")";
            }
        }

        // Send response back to client
        if (success) {
            send(clientSocket, successMsg.c_str(), successMsg.size(), 0);
            logMessage(successMsg);
        } else {
            send(clientSocket, errorMsg.c_str(), errorMsg.size(), 0);
            logMessage(errorMsg);
        }
    }
    else if (op == "DELETE") {
        if (filename.empty()) {
            std::string err = "ERROR: Invalid DELETE. Usage: DELETE <path> [FILE|FOLDER]";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }

        std::string type = "FILE"; // Default to FILE
        iss >> type; // Try to read type

        std::string fullPath = sharedPath + "/" + filename;

        // Check existence before attempting delete for a clearer error message
        struct stat st;
        if (stat(fullPath.c_str(), &st) != 0) {
            std::string err = "ERROR: File or folder not found: " + filename;
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }

        bool success = false;
        std::string successMsg;
        std::string errorMsg;

        if (type == "FOLDER") {
            // Check if it's actually a directory
            if (!S_ISDIR(st.st_mode)) {
                 errorMsg = "ERROR: Path exists but is not a folder: " + filename;
                 send(clientSocket, errorMsg.c_str(), errorMsg.size(), 0);
                 logMessage(errorMsg);
                 close(clientSocket);
                 return;
            }
            // Attempt to remove directory (rmdir fails if not empty)
            if (rmdir(fullPath.c_str()) != 0) {
                errorMsg = "ERROR: Could not delete folder: " + filename + " (" + strerror(errno) + "). Might not be empty.";
            } else {
                success = true;
                successMsg = "OK: Deleted folder " + filename;
            }
        } else { // Default to FILE
             // Check if it's actually a file
            if (S_ISDIR(st.st_mode)) {
                 errorMsg = "ERROR: Path exists but is not a file: " + filename;
                 send(clientSocket, errorMsg.c_str(), errorMsg.size(), 0);
                 logMessage(errorMsg);
                 close(clientSocket);
                 return;
            }
            // Attempt to remove file
            if (remove(fullPath.c_str()) != 0) {
                errorMsg = "ERROR: Could not delete file: " + filename + " (" + strerror(errno) + ")";
            } else {
                success = true;
                successMsg = "OK: Deleted file " + filename;
            }
        }

        // Send response back to client
        if (success) {
            send(clientSocket, successMsg.c_str(), successMsg.size(), 0);
            logMessage(successMsg);
        } else {
            send(clientSocket, errorMsg.c_str(), errorMsg.size(), 0);
            logMessage(errorMsg);
        }
    }
    else if (op == "LIST") {
        // ... existing LIST logic ...
    }
    else if (op == "STREAM") {
        if (filename.empty()) {
            std::string err = "ERROR: Invalid STREAM. Usage: STREAM <filename>";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }

        std::string fullPath = sharedPath + "/" + filename;

        // Check existence and type before attempting stream
        struct stat st;
        if (stat(fullPath.c_str(), &st) != 0) { // Check existence
            std::string err = "ERROR: File not found or cannot access: " + filename + " (" + strerror(errno) + ")";
            send(clientSocket, err.c_str(), err.size(), 0); // Send error
            logMessage(err);
            close(clientSocket); // Close socket after error
            return;
        }
        if (S_ISDIR(st.st_mode)) { // Check if it's a directory
             std::string err = "ERROR: Path is a directory, cannot stream: " + filename;
             send(clientSocket, err.c_str(), err.size(), 0); // Send error
             logMessage(err);
             close(clientSocket); // Close socket after error
             return;
        }

        std::ifstream file(fullPath, std::ios::binary);
        if (!file) {
            std::string err = "ERROR: Cannot open file for streaming: " + filename;
            send(clientSocket, err.c_str(), err.size(), 0); // Send error
            logMessage(err);
            close(clientSocket); // Close socket after error
            return;
        }

        // If file opened successfully, start sending raw data
        logMessage("Streaming file: " + filename);
        char streamBuffer[MAX_BUFFER]; // Use MAX_BUFFER from common.h
        while (file.read(streamBuffer, sizeof(streamBuffer))) {
            if (send(clientSocket, streamBuffer, file.gcount(), 0) < 0) {
                perror("send stream data");
                logMessage("ERROR: Failed sending stream data for " + filename);
                file.close();
                close(clientSocket); // Close on send error
                return;
            }
        }
        // Send the last chunk if any
        if (file.gcount() > 0) {
             if (send(clientSocket, streamBuffer, file.gcount(), 0) < 0) {
                 perror("send last stream chunk");
                 logMessage("ERROR: Failed sending last stream chunk for " + filename);
                 // Fall through to close file and socket
             }
        }

        file.close();
        logMessage("Finished streaming file: " + filename);
        // Close the socket after streaming is complete or if an error occurred during the last send
        close(clientSocket);
        return; // Return early as STREAM handles its own socket closing
    }
    else if (op == "INFO") {
        if (filename.empty()) {
            std::string err = "ERROR: Invalid INFO. Usage: INFO <path>";
            if (send(clientSocket, err.c_str(), err.size(), 0) < 0) { perror("send info error"); }
            logMessage(err);
            close(clientSocket);
            return;
        }

        std::string fullPath = sharedPath + "/" + filename;
        struct stat st;

        if (stat(fullPath.c_str(), &st) != 0) {
            std::string err = "ERROR: Cannot get info for path: " + filename + " (" + strerror(errno) + ")";
            if (send(clientSocket, err.c_str(), err.size(), 0) < 0) { perror("send info error"); }
            logMessage(err);
        } else {
            std::ostringstream oss;
            oss << "OK: Info for " << filename << "\n"
                << "  Type:        " << (S_ISDIR(st.st_mode) ? "Directory" : "File") << "\n"
                << "  Size:        " << st.st_size << " bytes\n"
                << "  Permissions: " << formatPermissions(st.st_mode) << "\n"
                << "  Accessed:    " << formatTime(st.st_atime) << "\n"
                << "  Modified:    " << formatTime(st.st_mtime) << "\n"
                << "  Created:     " << formatTime(st.st_ctime); // Note: ctime is change time on Unix

            std::string infoStr = oss.str();
            if (send(clientSocket, infoStr.c_str(), infoStr.size(), 0) < 0) {
                perror("send info data");
                logMessage("ERROR: Failed sending info data for " + filename);
            } else {
                logMessage("Sent info for: " + filename);
            }
        }
        // INFO handler closes the socket at the end of the function
    }
    else if (op == "PULL_COPY_FROM") {
        // Direct string parsing approach for PULL_COPY_FROM command
        // Format: PULL_COPY_FROM <source_ip> <source_port> <source_path> <dest_path>
        
        // Skip the op part and get the source_ip
        size_t pos = command.find("PULL_COPY_FROM ");
        if (pos == std::string::npos) {
            std::string err = "ERROR: Invalid PULL_COPY_FROM command format";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err + " (command format wrong)");
            close(clientSocket);
            return;
        }
        pos += std::string("PULL_COPY_FROM ").length();
        
        // Extract source_ip (first token after PULL_COPY_FROM)
        size_t spacePos = command.find(' ', pos);
        if (spacePos == std::string::npos) {
            std::string err = "ERROR: Missing parameters in PULL_COPY_FROM command";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }
        std::string sourceIp = command.substr(pos, spacePos - pos);
        pos = spacePos + 1;
        
        // Extract source_port (second token)
        spacePos = command.find(' ', pos);
        if (spacePos == std::string::npos) {
            std::string err = "ERROR: Missing source_path and dest_path in PULL_COPY_FROM command";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }
        std::string sourcePortStr = command.substr(pos, spacePos - pos);
        int sourcePort;
        try {
            sourcePort = std::stoi(sourcePortStr);
        } catch (const std::exception& e) {
            std::string err = "ERROR: Invalid source_port in PULL_COPY_FROM: " + sourcePortStr;
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }
        pos = spacePos + 1;
        
        // Find the position of the last space which separates source_path and dest_path
        size_t lastSpacePos = command.find_last_of(' ');
        if (lastSpacePos == spacePos || lastSpacePos == std::string::npos) {
            std::string err = "ERROR: Missing dest_path in PULL_COPY_FROM command";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }
        
        // Extract source_path (everything between the second and last space)
        std::string sourcePath = command.substr(pos, lastSpacePos - pos);
        
        // Extract dest_path (everything after the last space)
        std::string destPath = command.substr(lastSpacePos + 1);
        
        // Log the parsed components for debugging - FIX: Use std::string for concatenation
        std::string logMsg = "PULL_COPY_FROM parsed: sourceIP=" + sourceIp + 
                             ", sourcePort=" + sourcePortStr + 
                             ", sourcePath='" + sourcePath + "'" + 
                             ", destPath='" + destPath + "'";
        logMessage(logMsg);
        
        // Validate paths
        if (sourcePath.empty() || destPath.empty()) {
            std::string err = "ERROR: Source path or destination path cannot be empty";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }
        
        // Normalize paths: ensure they start with '/'
        if (sourcePath[0] != '/') {
            sourcePath = "/" + sourcePath;
        }
        if (destPath[0] != '/') {
            destPath = "/" + destPath;
        }
        
        // Calculate full destination path
        std::string destFullPath = sharedPath;
        // Remove leading slash from destPath when appending to sharedPath
        if (destPath[0] == '/') {
            destPath = destPath.substr(1);
        }
        destFullPath = sharedPath + "/" + destPath;
        
        logMessage("PULL_COPY_FROM: Copying from " + sourceIp + ":" + std::to_string(sourcePort) +
                   " (" + sourcePath + ") to " + destFullPath);
        
        // Perform the actual file copy
        long long totalBytes = 0;
        bool copied = copyFile(sourceIp, sourcePort, sourcePath, destFullPath, totalBytes);
        
        if (copied) {
            std::string msg = "OK: Copied file (" + std::to_string(totalBytes) + " bytes) from " + sourcePath + " to " + destPath;
            send(clientSocket, msg.c_str(), msg.size(), 0);
            logMessage(msg);
        } else {
            std::string err = "ERROR: Failed to copy file from " + sourcePath + " to " + destPath;
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
        }
        close(clientSocket);
        return;
    }
    else {
        std::string err = "ERROR: Unknown command: " + op;
        send(clientSocket, err.c_str(), err.size(), 0);
        logMessage(err);
    }

    close(clientSocket); // Ensure socket is closed for handlers that don't close it internally
}

static std::string getLocalIP() {
    struct ifaddrs *ifaddr, *ifa;
    if (getifaddrs(&ifaddr) == -1)
        return "";
    for (ifa = ifaddr; ifa; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr || ifa->ifa_addr->sa_family != AF_INET)
            continue;
        if (ifa->ifa_flags & IFF_LOOPBACK)
            continue;
        char buf[INET_ADDRSTRLEN];
        void* addrPtr = &((struct sockaddr_in*)ifa->ifa_addr)->sin_addr;
        inet_ntop(AF_INET, addrPtr, buf, sizeof(buf));
        freeifaddrs(ifaddr);
        return std::string(buf);
    }
    freeifaddrs(ifaddr);
    return "";
}

int main(int argc, char* argv[]) {
    if (argc != 6) {
        std::cerr<<"Usage: "<<argv[0]
                 <<" <listen_port> <shared_folder> "
                    "<comma_paths> <NamingServerIP> <NamingServerPort>\n";
        return 1;
    }
    int        port       = std::stoi(argv[1]);
    std::string sharedPath= argv[2];
    std::string csvPaths  = argv[3];
    std::string nmIP      = argv[4];
    int        nmPort     = std::stoi(argv[5]);

    // --- Store info globally for signal handler ---
    g_nmIP = nmIP;
    g_nmPort = nmPort;
    g_myPort = port;

    // 1) Determine our IP
    std::cerr << "Finding local IP address..." << std::endl;
    std::string myIP = getLocalIP();
    if (myIP.empty()) {
        std::cerr << "ERROR: Could not determine local IP\n";
        return 1;
    }
    g_myIP = myIP; // Store globally
    std::cerr << "Local IP: " << myIP << std::endl;

    // --- Register Signal Handler ---
    signal(SIGINT, signalHandler);  // Handle Ctrl+C
    signal(SIGTERM, signalHandler); // Handle termination signal

    // Initialize random seed for write IDs
    std::srand(std::time(nullptr));

    // 2) REGISTER with Naming Server
    std::cerr << "Connecting to naming server at " << nmIP << ":" << nmPort << "..." << std::endl;
    int regSock = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in nmAddr{};
    nmAddr.sin_family = AF_INET;
    nmAddr.sin_port   = htons(nmPort);
    inet_pton(AF_INET, nmIP.c_str(), &nmAddr.sin_addr);
    
    if (connect(regSock, (sockaddr*)&nmAddr, sizeof(nmAddr)) < 0) {
        std::cerr << "ERROR: Failed to connect to naming server" << std::endl;
        perror("connect to NM");
        return 1;
    }
    std::cerr << "Connected to naming server. Sending registration..." << std::endl;
    
    std::string regMsg = "REGISTER " + myIP + " " +
                         std::to_string(port) + " " + csvPaths;
    std::cerr << "Registration message: " << regMsg << std::endl;
    
    if (send(regSock, regMsg.c_str(), regMsg.size(), 0) < 0) {
        std::cerr << "ERROR: Failed to send registration message" << std::endl;
        perror("send registration");
        close(regSock);
        return 1;
    }
    std::cerr << "Waiting for acknowledgment from naming server..." << std::endl;

    char ackBuf[128];
    int n = recv(regSock, ackBuf, sizeof(ackBuf)-1, 0);
    if (n <= 0) {
        std::cerr << "ERROR: Failed to receive acknowledgment from naming server" << std::endl;
        if (n < 0) perror("recv ack");
        close(regSock);
        return 1;
    }
    ackBuf[n]='\0';
    std::string ack(ackBuf);
    std::cerr << "NM replied: " << ack << std::endl;
    logMessage("NM replied: " + ack);
    close(regSock);

    // 3) Now start serving clients on 'port'
    std::cerr << "Setting up storage server socket on port " << port << "..." << std::endl;
    
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port        = htons(port);

    bind(fd, (sockaddr*)&addr, sizeof(addr));
    listen(fd, 10);
    std::cerr << "Storage Server started successfully at " << myIP << ":" << port << std::endl;
    logMessage("Storage Server ("+myIP+":"+std::to_string(port)+") serving: " + csvPaths);

    while (!stopFlag) { // Check flag if signal handler doesn't exit directly
        int client = accept(fd, NULL, NULL);
        if (client < 0) {
            if (errno == EINTR && stopFlag) {
                 logMessage("Accept interrupted by signal, shutting down.");
                 break; // Exit loop if accept was interrupted by our signal
            }
            perror("accept");
            continue;
        }
        std::thread(handleClient, client, sharedPath).detach();
    }

    logMessage("Main loop finished. Cleaning up.");
    close(fd); // Close listening socket

    return 0;
}
