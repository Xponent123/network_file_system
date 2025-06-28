#ifndef COMMON_H
#define COMMON_H

#include <string> // For using std::string
#include <vector> // For using std::vector
#include <map>    // For using std::map

#define MAX_BUFFER 4096 // Maximum buffer size for data transfer
#define STOP_SIGNAL "STOP" // Signal to indicate stopping of a process

// Enum to represent different types of file requests
enum RequestType {
    READ,    // Request to read a file
    WRITE,   // Request to write to a file
    CREATE,  // Request to create a new file
    DELETE,  // Request to delete a file
    COPY,    // Request to copy a file
    STREAM,  // Request to stream data
    INFO,    // Request to get file information
    LIST,    // Request to list files in a directory
    INVALID  // Invalid request type
};

// Structure to represent a file request
struct FileRequest {
    RequestType type;       // Type of the request (e.g., READ, WRITE)
    std::string path;       // Path of the file involved in the request
    std::string data;       // Data to be written (if applicable)
    bool syncWrite = false; // Flag to indicate if the write should be synchronous
};

// Structure to store server-side information
struct SSInfo {
    std::string ip;                      // IP address of the server
    int port;                            // Port number of the server
    std::vector<std::string> accessiblePaths; // List of paths accessible on the server
};

// Function to convert a RequestType enum to its string representation
std::string requestTypeToString(RequestType type);

// Function to parse a string and convert it to a RequestType enum
RequestType parseRequestType(const std::string& str);

// Function to log a message (e.g., for debugging or monitoring)
void logMessage(const std::string& msg);

#endif // End of COMMON_H guard
