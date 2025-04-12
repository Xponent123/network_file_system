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
#include <filesystem> // For easier recursive directory operations
#include <ctime>      // For formatting timestamps
#include <csignal>    // For signal handling

// --- Global variables for signal handler ---
static std::string g_nmIP;
static int g_nmPort = 0;
static std::string g_myIP;
static int g_myPort = 0;
volatile sig_atomic_t stopFlag = 0; // Flag to signal termination

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

// --- fetchRemoteContent HELPER FUNCTION (kept for potential other uses, but not used by streaming COPY) ---
static std::string fetchRemoteContent(const std::string& remoteIp, int remotePort, const std::string& command) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
        logMessage("ERROR [fetchRemoteContent]: socket creation failed - " + std::string(strerror(errno)));
        return "";
    }

    sockaddr_in remoteAddr{};
    remoteAddr.sin_family = AF_INET;
    remoteAddr.sin_port   = htons(remotePort);
    if (inet_pton(AF_INET, remoteIp.c_str(), &remoteAddr.sin_addr) <= 0) {
        logMessage("ERROR [fetchRemoteContent]: inet_pton failed for " + remoteIp);
        close(sock);
        return "";
    }

    if (connect(sock, (sockaddr*)&remoteAddr, sizeof(remoteAddr)) < 0) {
        logMessage("ERROR [fetchRemoteContent]: connect failed to " + remoteIp + ":" + std::to_string(remotePort) + " - " + std::string(strerror(errno)));
        close(sock);
        return "";
    }

    if (send(sock, command.c_str(), command.size(), 0) < 0) {
        logMessage("ERROR [fetchRemoteContent]: send failed - " + std::string(strerror(errno)));
        close(sock);
        return "";
    }

    // Receive the entire content (could be large!)
    std::string content;
    char buffer[2048];
    int bytes;
    while ((bytes = recv(sock, buffer, sizeof(buffer), 0)) > 0) {
        content.append(buffer, bytes);
    }

    if (bytes < 0) {
         logMessage("ERROR [fetchRemoteContent]: recv failed - " + std::string(strerror(errno)));
         close(sock);
         return ""; // Indicate error
    }
    // If bytes == 0, connection closed cleanly by peer

    close(sock);

    // Check if the received content starts with "ERROR:", if so, return empty to signal failure upstream
    if (content.rfind("ERROR:", 0) == 0) {
        logMessage("ERROR [fetchRemoteContent]: Source server returned error: " + content);
        return ""; // Return empty string to indicate the source server had an error
    }

    return content;
}

// Helper function: Check if a path is a directory
static bool isDirectory(const std::string& path) {
    struct stat st;
    if (stat(path.c_str(), &st) != 0) {
        return false;  // Path doesn't exist
    }
    return S_ISDIR(st.st_mode);
}

// Helper function: Create directory and its parents if needed
static bool createDirectoryRecursive(const std::string& dirPath) {
    size_t pos = 0;
    std::string path = dirPath;
    
    if (path.back() != '/') {
        path += '/';  // Ensure path ends with /
    }
    
    while ((pos = path.find('/', pos + 1)) != std::string::npos) {
        std::string subPath = path.substr(0, pos);
        if (subPath.empty()) continue;  // Skip root
        
        if (mkdir(subPath.c_str(), 0755) != 0) {
            if (errno != EEXIST) {
                logMessage("ERROR: Failed to create directory: " + subPath + " - " + strerror(errno));
                return false;
            }
        }
    }
    
    return true;
}

// Helper function: List files in directory
static bool listFilesInDirectory(const std::string& dirPath, std::vector<std::string>& files, std::vector<std::string>& dirs) {
    DIR* dir = opendir(dirPath.c_str());
    if (!dir) {
        logMessage("ERROR: Cannot open directory for listing: " + dirPath + " - " + strerror(errno));
        return false;
    }
    
    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        std::string name(entry->d_name);
        if (name == "." || name == "..") continue;
        
        std::string fullPath = dirPath + "/" + name;
        if (isDirectory(fullPath)) {
            dirs.push_back(name);
        } else {
            files.push_back(name);
        }
    }
    
    closedir(dir);
    return true;
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
    
    // 3. Open destination file
    std::ofstream outFile(destFullPath, std::ios::binary | std::ios::trunc);
    if (!outFile) {
        logMessage("ERROR: Cannot open destination file: " + destFullPath);
        close(sourceSock);
        return false;
    }
    
    // 4. Stream data from source to destination
    char buffer[2048];
    int bytesReceived;
    long long fileSize = 0;
    
    while ((bytesReceived = recv(sourceSock, buffer, sizeof(buffer), 0)) > 0) {
        outFile.write(buffer, bytesReceived);
        if (!outFile) {
            logMessage("ERROR: Failed writing to destination file: " + destFullPath);
            outFile.close();
            close(sourceSock);
            return false;
        }
        fileSize += bytesReceived;
    }
    
    outFile.close();
    close(sourceSock);
    
    if (bytesReceived < 0) {
        logMessage("ERROR: Failed receiving data for file copy - " + std::string(strerror(errno)));
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
    
    // Create destination directory
    std::string destFullPath = sharedPath + "/" + destRelPath;
    if (destRelPath[0] == '/') destFullPath = sharedPath + destRelPath; // Handle absolute paths
    
    if (mkdir(destFullPath.c_str(), 0755) != 0 && errno != EEXIST) {
        logMessage("ERROR: Failed to create destination directory: " + destFullPath + " - " + strerror(errno));
        return false;
    }
    
    // Get directory listing from source
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
    
    // Receive directory listing
    char buffer[4096];
    int bytesReceived = recv(listSock, buffer, sizeof(buffer) - 1, 0);
    close(listSock);
    
    if (bytesReceived <= 0) {
        logMessage("ERROR: Failed to receive directory listing");
        return false;
    }
    
    buffer[bytesReceived] = '\0';
    std::string listing(buffer);
    
    // Check for error in response
    if (listing.find("ERROR:") == 0) {
        logMessage("ERROR: Source server reported error during LIST: " + listing);
        return false;
    }
    
    // Parse listing (format is "OK: Listing of /path:\nfile1\nfile2\n...")
    std::istringstream iss(listing);
    std::string line;
    bool headerSkipped = false;
    
    // Process each entry in the directory listing
    while (std::getline(iss, line)) {
        if (!headerSkipped) {
            headerSkipped = true;  // Skip the first line (header)
            continue;
        }
        
        if (line.empty() || line == "Directory is empty.")
            continue;
        
        // Determine if entry is a file or directory (need to use additional requests)
        std::string entrySourcePath = sourceRelPath + "/" + line;
        std::string entryDestPath = destRelPath + "/" + line;
        
        if (entrySourcePath[0] != '/') entrySourcePath = "/" + entrySourcePath;
        if (entryDestPath[0] != '/') entryDestPath = "/" + entryDestPath;
        
        // To determine if directory, we try to LIST it
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
        
        if (checkBytes > 0) {
            checkBuf[checkBytes] = '\0';
            std::string checkResp(checkBuf);
            
            // If LIST succeeds, it's a directory
            if (checkResp.find("ERROR:") != 0) {
                // Recursively copy subdirectory
                if (!copyDirectoryRecursive(sourceIp, sourcePort, entrySourcePath, entryDestPath, sharedPath, totalBytes)) {
                    logMessage("WARNING: Failed to copy subdirectory: " + entrySourcePath);
                }
                continue;
            }
        }
        
        // If we reach here, it's a file or we couldn't determine - try copying as file
        std::string entryDestFullPath = sharedPath + "/" + (entryDestPath[0] == '/' ? entryDestPath.substr(1) : entryDestPath);
        if (!copyFile(sourceIp, sourcePort, entrySourcePath, entryDestFullPath, totalBytes)) {
            logMessage("WARNING: Failed to copy file: " + entrySourcePath);
        }
    }
    
    return true;
}

// Helper function to format permissions from st_mode
static std::string formatPermissions(mode_t mode) {
    std::string perms = "----------";
    if (S_ISDIR(mode)) perms[0] = 'd';
    // User permissions
    if (mode & S_IRUSR) perms[1] = 'r';
    if (mode & S_IWUSR) perms[2] = 'w';
    if (mode & S_IXUSR) perms[3] = 'x';
    // Group permissions
    if (mode & S_IRGRP) perms[4] = 'r';
    if (mode & S_IWGRP) perms[5] = 'w';
    if (mode & S_IXGRP) perms[6] = 'x';
    // Other permissions
    if (mode & S_IROTH) perms[7] = 'r';
    if (mode & S_IWOTH) perms[8] = 'w';
    if (mode & S_IXOTH) perms[9] = 'x';
    return perms;
}

// Helper function to format time_t
static std::string formatTime(time_t time) {
    char buf[100];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", std::localtime(&time));
    return std::string(buf);
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

    // --- UPDATED: Handle PULL_COPY_FROM command with directory support ---
    if (op == "PULL_COPY_FROM") {
        std::string sourceIp = firstArg;
        int sourcePort;
        std::string sourcePath, destPath;
        iss >> sourcePort >> sourcePath >> destPath;

        if (sourceIp.empty() || sourcePort <= 0 || sourcePath.empty() || destPath.empty()) {
            std::string err = "ERROR: Invalid PULL_COPY_FROM format.";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }

        logMessage("PULL_COPY_FROM: Source=" + sourceIp + ":" + std::to_string(sourcePort) + sourcePath + ", Dest=" + destPath);

        // Strip leading '/' for local filesystem paths
        std::string localDestPath = destPath;
        if (!localDestPath.empty() && localDestPath[0] == '/') {
            localDestPath.erase(0, 1);
        }
        std::string fullDestPath = sharedPath + "/" + localDestPath;

        // Check if source is directory by trying to LIST it
        int checkSock = socket(AF_INET, SOCK_STREAM, 0);
        if (checkSock < 0) {
            std::string err = "ERROR: Failed to create socket to check source type.";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }

        sockaddr_in sourceAddr{};
        sourceAddr.sin_family = AF_INET;
        sourceAddr.sin_port = htons(sourcePort);
        if (inet_pton(AF_INET, sourceIp.c_str(), &sourceAddr.sin_addr) <= 0) {
            std::string err = "ERROR: Invalid source server IP address.";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(checkSock);
            close(clientSocket);
            return;
        }

        if (connect(checkSock, (sockaddr*)&sourceAddr, sizeof(sourceAddr)) < 0) {
            std::string err = "ERROR: Failed to connect to source server to check source type.";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(checkSock);
            close(clientSocket);
            return;
        }

        // Try to LIST the sourcePath
        std::string listCmd = "LIST " + sourcePath;
        if (send(checkSock, listCmd.c_str(), listCmd.size(), 0) < 0) {
            std::string err = "ERROR: Failed to send LIST command to check source type.";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(checkSock);
            close(clientSocket);
            return;
        }

        char checkBuf[128];
        int checkBytes = recv(checkSock, checkBuf, sizeof(checkBuf) - 1, 0);
        close(checkSock);

        bool isDir = false;
        if (checkBytes > 0) {
            checkBuf[checkBytes] = '\0';
            std::string checkResp(checkBuf);
            
            // If LIST succeeds, it's a directory
            if (checkResp.find("ERROR:") != 0) {
                isDir = true;
            }
        }

        long long totalBytesWritten = 0;
        bool success = false;

        if (isDir) {
            // Handle directory copy
            logMessage("PULL_COPY_FROM: Source is a directory, performing recursive copy.");
            success = copyDirectoryRecursive(sourceIp, sourcePort, sourcePath, destPath, sharedPath, totalBytesWritten);
        } else {
            // Handle file copy (existing logic for file copies)
            logMessage("PULL_COPY_FROM: Source is a file, performing simple copy.");
            success = copyFile(sourceIp, sourcePort, sourcePath, fullDestPath, totalBytesWritten);
        }

        if (!success) {
            std::string err = "ERROR: Copy operation failed.";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }

        // 6. Send ACK back to the NM
        std::string ack = "OK: Copied " + sourcePath + " to " + destPath + " (" + 
                         std::to_string(totalBytesWritten) + " bytes" + 
                         (isDir ? ", recursive directory copy)" : ")");
        send(clientSocket, ack.c_str(), ack.size(), 0);
        logMessage(ack);
        close(clientSocket); // Close connection to NM
        return; // Handled
    }

    // --- Existing Command Handling ---
    // Rename 'filename' variable for clarity in existing handlers
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
        std::ifstream file(fullPath, std::ios::binary);
        if (!file) {
            std::string err = "ERROR: File not found: " + filename;
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }

        std::ostringstream ss;
        ss << file.rdbuf(); // read file content into stringstream
        std::string content = ss.str(); // convert to string
        file.close();
        if(content.empty()){
            std::string err = "ERROR: File is empty: " + filename;
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }
        send(clientSocket, content.c_str(), content.size(), 0); // send content
        logMessage("Sent content of " + filename);
    }
    else if (op == "WRITE") {
        if (filename.empty()) {
            std::string err = "ERROR: Invalid WRITE. Usage: WRITE <filename> <data>";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }

        // The rest of the line is the data to write
        std::string data;
        std::getline(iss, data);
        if (!data.empty() && data[0] == ' ')
            data.erase(0, 1);  // strip leading space

        std::string fullPath = sharedPath + "/" + filename;
        std::ofstream out(fullPath, std::ios::binary);
        if (!out) {
            std::string err = "ERROR: Cannot open file for writing: " + filename;
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
        } else {
            out << data;
            out.close();
            std::string ack = "OK: Wrote " + std::to_string(data.size()) + " bytes to " + filename;
            send(clientSocket, ack.c_str(), ack.size(), 0);
            logMessage(ack);
        }
    }
    else if (op == "CREATE") {
        if (filename.empty()) {
            std::string err = "ERROR: Invalid CREATE. Usage: CREATE <name> [FOLDER]";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }
        // Check optional token to determine type (FOLDER or FILE)
        std::string type;
        iss >> type;

        std::string fullPath = sharedPath + "/" + filename;
        // For file creation: check existence then create empty file
        if (type != "FOLDER") {
            std::ifstream checkFile(fullPath);
            if (checkFile.good()) {
                std::string err = "ERROR: File already exists: " + filename;
                send(clientSocket, err.c_str(), err.size(), 0);
                logMessage(err);
                close(clientSocket);
                return;
            }
            std::ofstream out(fullPath, std::ios::binary);
            if (!out) {
                std::string err = "ERROR: Cannot create file: " + filename;
                send(clientSocket, err.c_str(), err.size(), 0);
                logMessage(err);
            } else {
                out.close();
                std::string ack = "OK: Created file " + filename;
                send(clientSocket, ack.c_str(), ack.size(), 0);
                logMessage(ack);
            }
        }
        // Folder creation branch
        else {
            // Check if folder exists by attempting to open directory (simplified check)
            struct stat st;
            if (stat(fullPath.c_str(), &st) == 0 && S_ISDIR(st.st_mode)) {
                std::string err = "ERROR: Folder already exists: " + filename;
                send(clientSocket, err.c_str(), err.size(), 0);
                logMessage(err);
                close(clientSocket);
                return;
            }
            if (mkdir(fullPath.c_str(), 0755) != 0) {
                std::string err = "ERROR: Could not create folder: " + filename;
                send(clientSocket, err.c_str(), err.size(), 0);
                logMessage(err);
            } else {
                std::string ack = "OK: Created folder " + filename;
                send(clientSocket, ack.c_str(), ack.size(), 0);
                logMessage(ack);
            }
        }
    }
    else if (op == "DELETE") {
        if (filename.empty()) {
            std::string err = "ERROR: Invalid DELETE. Usage: DELETE <name> [FOLDER]";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }
        std::string type;
        iss >> type;
        std::string fullPath = sharedPath + "/" + filename;
        if (type == "FOLDER") {
            if (rmdir(fullPath.c_str()) != 0) {
                std::string err = "ERROR: Could not delete folder: " + filename;
                send(clientSocket, err.c_str(), err.size(), 0);
                logMessage(err);
            } else {
                std::string ack = "OK: Deleted folder " + filename;
                send(clientSocket, ack.c_str(), ack.size(), 0);
                logMessage(ack);
            }
        } else {
            if (remove(fullPath.c_str()) != 0) {
                std::string err = "ERROR: Could not delete file: " + filename;
                send(clientSocket, err.c_str(), err.size(), 0);
                logMessage(err);
            } else {
                std::string ack = "OK: Deleted file " + filename;
                send(clientSocket, ack.c_str(), ack.size(), 0);
                logMessage(ack);
            }
        }
    }
    else if (op == "LIST") {
        // Use provided folder if given, otherwise use sharedPath
        std::string targetDir;
        if (filename.empty()) { // LIST command without path argument
            targetDir = sharedPath;
        } else {
            targetDir = sharedPath + "/" + filename; // Constructs correctly after stripping leading '/'
        }
        DIR* dir = opendir(targetDir.c_str());
        if (!dir) {
            std::string err = "ERROR: Cannot open directory: " + targetDir;
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }
        std::string listing;
        struct dirent* entry;
        while ((entry = readdir(dir)) != nullptr) {
            std::string name(entry->d_name);
            if (name == "." || name == "..") continue;
            listing += name + "\n";
        }
        closedir(dir);
        if (listing.empty())
            listing = "Directory is empty.";
        std::string ack = "OK: Listing of " + targetDir + ":\n" + listing;
        send(clientSocket, ack.c_str(), ack.size(), 0);
        logMessage("Listed directory: " + targetDir);
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
        std::ifstream file(fullPath, std::ios::binary);
        if (!file) {
            std::string err = "ERROR: File not found: " + filename;
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }
        char streamBuffer[1024];
        while (file.read(streamBuffer, sizeof(streamBuffer)) || file.gcount() > 0) {
            int bytesToSend = file.gcount();
            if (send(clientSocket, streamBuffer, bytesToSend, 0) < 0) {
                logMessage("ERROR: Streaming interrupted for " + filename);
                break;
            }
        }
        logMessage("Streamed file " + filename);
    }
    // --- NEW: Handle INFO command ---
    else if (op == "INFO") {
        if (filename.empty()) {
            std::string err = "ERROR: Invalid INFO. Usage: INFO <path>";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
            close(clientSocket);
            return;
        }

        std::string fullPath = sharedPath + "/" + filename;
        struct stat st;

        if (stat(fullPath.c_str(), &st) != 0) {
            std::string err = "ERROR: File not found or cannot access: " + filename + " (" + strerror(errno) + ")";
            send(clientSocket, err.c_str(), err.size(), 0);
            logMessage(err);
        } else {
            std::ostringstream oss;
            oss << "OK: Info for " << filename << ":\n";
            oss << "  Type:        " << (S_ISDIR(st.st_mode) ? "Directory" : "File") << "\n";
            oss << "  Size:        " << st.st_size << " bytes\n";
            oss << "  Permissions: " << formatPermissions(st.st_mode) << "\n";
            oss << "  Modified:    " << formatTime(st.st_mtime) << "\n";
            oss << "  Accessed:    " << formatTime(st.st_atime) << "\n";
            oss << "  Changed:     " << formatTime(st.st_ctime); // No newline at the end

            std::string infoResp = oss.str();
            send(clientSocket, infoResp.c_str(), infoResp.size(), 0);
            logMessage("Sent info for " + filename);
        }
    }
    else {
        std::string err = "ERROR: Unknown command. Use READ, WRITE, CREATE, DELETE, LIST, STREAM, INFO, or COPY";
        send(clientSocket, err.c_str(), err.size(), 0);
        logMessage(err);
    }

    close(clientSocket);
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
