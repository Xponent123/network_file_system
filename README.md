# Distributed File System - NFS Implementation

This project implements a simplified Network File System (NFS) in C/C++ consisting of the following core components:

- **Naming Server**
- **Storage Servers**
- **Clients**

The system supports asynchronous operations, file streaming, replication, and fault tolerance features, simulating a distributed file management infrastructure.

---

## 🧱 Components

### 1. **Naming Server**
- Acts as the central coordinator.
- Handles namespace management (directory creation, deletion, listing).
- Keeps track of which file is stored on which storage server.
- Maintains a table of files with their associated storage servers and replication info.

### 2. **Storage Server**
- Stores actual file data.
- Interacts with clients and naming server for file read/write operations.
- Capable of sending file chunks for streaming.
- Supports replication upon request from the naming server.

### 3. **Client**
- Provides an interface to interact with the distributed system.
- Supports file and directory operations like:
  - `create(filename)`
  - `delete(filename)`
  - `read(filename)`
  - `write(filename)`
  - `stream(filename)`
  - `mkdir(dirname)`
  - `ls(dirname)`
- Communicates with naming server and appropriate storage server.

---

## 🔁 Replication Strategy
- Ensures at least 2 copies of each file on different storage servers.
- Naming server triggers replication when a new file is created or a server fails.
- Load balancing strategy can be used to decide replication targets.

---

## ⚡ Asynchronous Communication
- File operations (read/write) are handled asynchronously.
- Background threads handle replication and periodic health-checking of storage servers.

---

## 📺 File Streaming
- Files can be streamed from the storage server in chunks.
- Clients request next chunks as needed, allowing efficient audio/video playback.

---

## 🛡️ Fault Tolerance
- Naming server periodically pings storage servers.
- On failure detection:
  - Removes failed server from the active list.
  - Triggers re-replication of affected files from the remaining replicas.

---

## 🛠️ Build & Run

### Requirements
- C/C++ compiler (g++)
- POSIX-compliant system (Linux recommended)
- Threads and sockets libraries

<<<<<<< HEAD
### Build

```sh
# From project root
g++ naming_server.cpp common.cpp -o naming_server -lpthread
g++ storage_server.cpp common.cpp -o storage_server -lpthread
g++ client.cpp common.cpp -o client
```

### Run

```sh
# 1. Start Naming Server (default port 5055)
./naming_server 5055

# 2. Start Storage Server(s)
./storage_server 5050 /path/to/storage1 "/prefix1,prefix2" 127.0.0.1 5055
./storage_server 5051 /path/to/storage2 "/prefix3"         127.0.0.1 5055

# 3. Start Client, pointing at the Naming Server
./client 127.0.0.1 5055
```

Now at the `Client>` prompt you can issue commands like:
```
LISTSERVERS
CREATE /foo/file.txt
WRITE /foo/file.txt HelloWorld
READ /foo/file.txt
EXIT
```
