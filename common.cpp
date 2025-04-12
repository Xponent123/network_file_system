#include "common.h"
#include <iostream>
#include <chrono>
#include <ctime>

std::string requestTypeToString(RequestType type) {
    switch (type) {
        case READ: return "READ";
        case WRITE: return "WRITE";
        case CREATE: return "CREATE";
        case DELETE: return "DELETE";
        case COPY: return "COPY";
        case STREAM: return "STREAM";
        case INFO: return "INFO";
        case LIST: return "LIST";
        default: return "INVALID";
    }
}

RequestType parseRequestType(const std::string& str) {
    if (str == "READ") return READ;
    if (str == "WRITE") return WRITE;
    if (str == "CREATE") return CREATE;
    if (str == "DELETE") return DELETE;
    if (str == "COPY") return COPY;
    if (str == "STREAM") return STREAM;
    if (str == "INFO") return INFO;
    if (str == "LIST") return LIST;
    return INVALID;
}

void logMessage(const std::string& msg) {
    auto now = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::string timeStr = std::ctime(&now);
    timeStr.pop_back(); // Remove trailing newline
    std::cout << "[" << timeStr << "]: " << msg << std::endl;
}
