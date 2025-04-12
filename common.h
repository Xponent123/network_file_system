#ifndef COMMON_H
#define COMMON_H

#include <string>
#include <vector>
#include <map>

#define MAX_BUFFER 4096
#define STOP_SIGNAL "STOP"

enum RequestType {
    READ, // done & checked
    WRITE, // done & checked
    CREATE, // done & checked
    DELETE, // done & checked
    COPY, // done & checked
    STREAM, // done & checked
    INFO, // done & checked
    LIST, //done & checked
    INVALID
};

struct FileRequest {
    RequestType type;
    std::string path;
    std::string data;
    bool syncWrite = false;
};

struct SSInfo {
    std::string ip;
    int port;
    std::vector<std::string> accessiblePaths;
};

std::string requestTypeToString(RequestType type);
RequestType parseRequestType(const std::string& str);
void logMessage(const std::string& msg);

#endif
