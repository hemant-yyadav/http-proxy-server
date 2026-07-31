# HTTP Proxy Server

A multithreaded forward HTTP proxy server written in C using POSIX sockets and pthreads. It supports concurrent client connections and uses an in-memory LRU cache to reduce repeated requests for the same resources.

## Features

* Concurrent client handling using POSIX threads
* Thread-safe in-memory LRU response cache
* HTTP/1.0 and HTTP/1.1 GET request support
* Hash-table indexed cache for fast lookups
* Semaphore-limited concurrency (400 worker threads)
* Request validation and HTTP error handling
* Dependency-free implementation using standard POSIX APIs

## Tech Stack

* C
* POSIX Sockets
* POSIX Threads (`pthread`)
* POSIX Semaphores
* GNU Make

## Project Structure

```text
.
├── proxyserver.c      # Proxy server, networking, cache
├── proxy_parse.c      # HTTP request parser
├── proxy_parse.h
├── Makefile
├── LICENSE
└── README.md
```

## Build

### Prerequisites

* POSIX-compatible system (Linux/macOS)
* GCC
* Make

### Compile

```bash
make
```

## Run

```bash
./proxy <port>
```

Example:

```bash
./proxy 8080
```

Configure your browser or HTTP client to use:

```
127.0.0.1:8080
```

as the HTTP proxy.

## How It Works

1. Accepts incoming client connections.
2. Parses HTTP GET requests.
3. Checks the in-memory LRU cache.
4. Returns cached responses when available.
5. Otherwise forwards the request to the origin server.
6. Streams the response back to the client.
7. Stores cacheable responses for future requests.

## Performance

* Thread-per-connection architecture
* 4,096-bucket hash table for fast cache lookup
* O(1) LRU promotion and eviction
* Maximum cache size: **200 MiB**
* Maximum cacheable object: **10 MiB**

## Limitations

* HTTP only (no HTTPS/CONNECT)
* Only supports GET requests
* IPv4 only
* No authentication or access control
* No HTTP cache-control or expiry handling
* No automated test suite

## Future Improvements

* HTTPS (`CONNECT`) support
* IPv6 support
* Configurable cache size
* Smarter cache-control handling
* Graceful shutdown
* Automated testing and benchmarking

## License

Released under the MIT License.
