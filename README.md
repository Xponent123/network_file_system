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

