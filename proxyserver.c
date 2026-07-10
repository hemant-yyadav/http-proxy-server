#include "proxy_parse.h"

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <semaphore.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

#define IO_BUFFER_SIZE 4096
#define MAX_CLIENTS 400
#define MAX_CACHE_SIZE (200U * (1U << 20))
#define MAX_CACHE_ELEMENT_SIZE (10U * (1U << 20))
#define MAX_REQUEST_SIZE 65535U
#define CACHE_BUCKET_COUNT 4096U

typedef struct cache_element {
    char *data;
    size_t len;
    char *url;
    size_t url_len;
    uint64_t hash;
    struct cache_element *hash_prev;
    struct cache_element *hash_next;
    struct cache_element *lru_prev;
    struct cache_element *lru_next;
} cache_element;

static int port_number = 8080;
static int proxy_socket_id = -1;
static sem_t client_limit;
static pthread_mutex_t cache_lock = PTHREAD_MUTEX_INITIALIZER;
static cache_element *cache_table[CACHE_BUCKET_COUNT];
static cache_element *cache_mru = NULL;
static cache_element *cache_lru = NULL;
static size_t cache_size = 0;

static int send_error_message(int socket_fd, int status_code);
static int connect_remote_server(const char *host_addr, int port_num);
static int handle_request(int client_socket, struct ParsedRequest *request,
                          const char *cache_key);
static int check_http_version(const char *msg);
static void *thread_fn(void *socket_arg);
static int send_all(int socket_fd, const void *buffer, size_t length);
static ssize_t recv_retry(int socket_fd, void *buffer, size_t length);
static int cache_find_copy(const char *url, char **data, size_t *len);
static int add_cache_element(const char *data, size_t size, const char *url);
static void remove_cache_element_locked(void);
static void free_cache(void);
static uint64_t hash_url(const char *url);
static size_t cache_entry_size(const cache_element *entry);
static cache_element *cache_lookup_locked(const char *url, uint64_t hash);
static void cache_hash_insert_locked(cache_element *entry);
static void cache_hash_remove_locked(cache_element *entry);
static void cache_lru_insert_front_locked(cache_element *entry);
static void cache_lru_remove_locked(cache_element *entry);
static void cache_lru_move_front_locked(cache_element *entry);
static void cache_free_entry(cache_element *entry);
static int read_client_request(int socket_fd, char **request, size_t *request_len);
static int parse_port(const char *port_text, int *port);
static void close_fd(int *fd);
static void unlock_cache(void);
static int acquire_client_slot(void);
static void release_client_slot(void);
static void destroy_client_limit(void);

static void close_fd(int *fd)
{
    if (*fd >= 0) {
        while (close(*fd) < 0 && errno == EINTR) {
        }
        *fd = -1;
    }
}

static void unlock_cache(void)
{
    int lock_status = pthread_mutex_unlock(&cache_lock);
    if (lock_status != 0) {
        fprintf(stderr, "pthread_mutex_unlock: %s\n", strerror(lock_status));
    }
}

static int acquire_client_slot(void)
{
    while (sem_wait(&client_limit) < 0) {
        if (errno == EINTR) {
            continue;
        }
        perror("sem_wait");
        return -1;
    }
    return 0;
}

static void release_client_slot(void)
{
    if (sem_post(&client_limit) < 0) {
        perror("sem_post");
    }
}

static void destroy_client_limit(void)
{
    if (sem_destroy(&client_limit) < 0) {
        perror("sem_destroy");
    }
}

static int send_all(int socket_fd, const void *buffer, size_t length)
{
    const char *cursor = buffer;
    size_t sent_total = 0;

    while (sent_total < length) {
        ssize_t sent = send(socket_fd, cursor + sent_total, length - sent_total, 0);
        if (sent < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("send");
            return -1;
        }
        if (sent == 0) {
            errno = ECONNRESET;
            perror("send");
            return -1;
        }
        sent_total += (size_t)sent;
    }

    return 0;
}

static ssize_t recv_retry(int socket_fd, void *buffer, size_t length)
{
    ssize_t received;

    do {
        received = recv(socket_fd, buffer, length, 0);
    } while (received < 0 && errno == EINTR);

    if (received < 0) {
        perror("recv");
    }

    return received;
}

static int send_error_message(int socket_fd, int status_code)
{
    struct {
        int code;
        const char *reason;
        const char *body;
    } responses[] = {
        {400, "Bad Request", "<HTML><HEAD><TITLE>400 Bad Request</TITLE></HEAD>\n<BODY><H1>400 Bad Request</H1>\n</BODY></HTML>"},
        {403, "Forbidden", "<HTML><HEAD><TITLE>403 Forbidden</TITLE></HEAD>\n<BODY><H1>403 Forbidden</H1><br>Permission Denied\n</BODY></HTML>"},
        {404, "Not Found", "<HTML><HEAD><TITLE>404 Not Found</TITLE></HEAD>\n<BODY><H1>404 Not Found</H1>\n</BODY></HTML>"},
        {500, "Internal Server Error", "<HTML><HEAD><TITLE>500 Internal Server Error</TITLE></HEAD>\n<BODY><H1>500 Internal Server Error</H1>\n</BODY></HTML>"},
        {501, "Not Implemented", "<HTML><HEAD><TITLE>501 Not Implemented</TITLE></HEAD>\n<BODY><H1>501 Not Implemented</H1>\n</BODY></HTML>"},
        {505, "HTTP Version Not Supported", "<HTML><HEAD><TITLE>505 HTTP Version Not Supported</TITLE></HEAD>\n<BODY><H1>505 HTTP Version Not Supported</H1>\n</BODY></HTML>"},
    };
    const char *reason = NULL;
    const char *body = NULL;
    char response[1024];
    char current_time[64] = "unknown";
    time_t now = time(NULL);
    size_t body_len;
    int written;

    for (size_t i = 0; i < sizeof(responses) / sizeof(responses[0]); i++) {
        if (responses[i].code == status_code) {
            reason = responses[i].reason;
            body = responses[i].body;
            break;
        }
    }
    if (reason == NULL || body == NULL) {
        return -1;
    }

    if (now != (time_t)-1) {
        struct tm tm_data;
        if (gmtime_r(&now, &tm_data) != NULL) {
            (void)strftime(current_time, sizeof(current_time),
                           "%a, %d %b %Y %H:%M:%S GMT", &tm_data);
        }
    }

    body_len = strlen(body);
    written = snprintf(response, sizeof(response),
                       "HTTP/1.1 %d %s\r\n"
                       "Content-Length: %zu\r\n"
                       "Connection: close\r\n"
                       "Content-Type: text/html\r\n"
                       "Date: %s\r\n"
                       "Server: CProxy/1.0\r\n"
                       "\r\n"
                       "%s",
                       status_code, reason, body_len, current_time, body);
    if (written < 0 || (size_t)written >= sizeof(response)) {
        errno = EMSGSIZE;
        perror("snprintf");
        return -1;
    }

    return send_all(socket_fd, response, (size_t)written);
}

static int connect_remote_server(const char *host_addr, int port_num)
{
    struct addrinfo hints;
    struct addrinfo *results = NULL;
    struct addrinfo *entry;
    char port_text[16];
    int remote_socket = -1;
    int gai_status;

    if (host_addr == NULL || *host_addr == '\0' || port_num <= 0 || port_num > 65535) {
        errno = EINVAL;
        perror("invalid remote address");
        return -1;
    }

    if (snprintf(port_text, sizeof(port_text), "%d", port_num) >= (int)sizeof(port_text)) {
        errno = EINVAL;
        perror("invalid remote port");
        return -1;
    }

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    gai_status = getaddrinfo(host_addr, port_text, &hints, &results);
    if (gai_status != 0) {
        fprintf(stderr, "getaddrinfo(%s): %s\n", host_addr, gai_strerror(gai_status));
        return -1;
    }

    for (entry = results; entry != NULL; entry = entry->ai_next) {
        remote_socket = socket(entry->ai_family, entry->ai_socktype, entry->ai_protocol);
        if (remote_socket < 0) {
            perror("socket");
            continue;
        }

        while (connect(remote_socket, entry->ai_addr, entry->ai_addrlen) < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("connect");
            close_fd(&remote_socket);
            break;
        }

        if (remote_socket >= 0) {
            break;
        }
    }

    freeaddrinfo(results);
    return remote_socket;
}

static int append_to_response(char **response, size_t *len, size_t *capacity,
                              const char *chunk, size_t chunk_len)
{
    char *new_response;
    size_t new_capacity;

    if (chunk_len > MAX_CACHE_ELEMENT_SIZE || *len > MAX_CACHE_ELEMENT_SIZE - chunk_len) {
        return 1;
    }

    if (*len + chunk_len <= *capacity) {
        memcpy(*response + *len, chunk, chunk_len);
        *len += chunk_len;
        return 0;
    }

    new_capacity = *capacity == 0 ? IO_BUFFER_SIZE : *capacity;
    while (new_capacity < *len + chunk_len) {
        if (new_capacity > MAX_CACHE_ELEMENT_SIZE / 2) {
            new_capacity = MAX_CACHE_ELEMENT_SIZE;
            break;
        }
        new_capacity *= 2;
    }

    if (new_capacity < *len + chunk_len) {
        return 1;
    }

    new_response = realloc(*response, new_capacity);
    if (new_response == NULL) {
        perror("realloc");
        return -1;
    }

    *response = new_response;
    *capacity = new_capacity;
    memcpy(*response + *len, chunk, chunk_len);
    *len += chunk_len;
    return 0;
}

static int handle_request(int client_socket, struct ParsedRequest *request,
                          const char *cache_key)
{
    char io_buffer[IO_BUFFER_SIZE];
    char *request_buffer = NULL;
    char *response_buffer = NULL;
    size_t response_len = 0;
    size_t response_capacity = 0;
    size_t request_len;
    int server_port = 80;
    int remote_socket = -1;
    int result = -1;

    if (ParsedHeader_set(request, "Connection", "close") < 0 ||
        ParsedHeader_set(request, "Proxy-Connection", "close") < 0) {
        fprintf(stderr, "failed to set connection headers\n");
        return -1;
    }

    if (ParsedHeader_get(request, "Host") == NULL &&
        ParsedHeader_set(request, "Host", request->host) < 0) {
        fprintf(stderr, "failed to set Host header\n");
        return -1;
    }

    if (request->port != NULL && parse_port(request->port, &server_port) < 0) {
        return -1;
    }

    request_len = strlen("GET ") + strlen(request->path) + 1 +
                  strlen(request->version) + 2 + ParsedHeader_headersLen(request);
    request_buffer = malloc(request_len + 1);
    if (request_buffer == NULL) {
        perror("malloc");
        return -1;
    }

    int written = snprintf(request_buffer, request_len + 1, "GET %s %s\r\n",
                           request->path, request->version);
    if (written < 0 || (size_t)written >= request_len + 1) {
        errno = EMSGSIZE;
        perror("snprintf");
        goto cleanup;
    }

    if (ParsedRequest_unparse_headers(request, request_buffer + (size_t)written,
                                      request_len + 1 - (size_t)written) < 0) {
        fprintf(stderr, "failed to serialize request headers\n");
        goto cleanup;
    }
    request_len = strlen(request_buffer);

    remote_socket = connect_remote_server(request->host, server_port);
    if (remote_socket < 0) {
        goto cleanup;
    }

    if (send_all(remote_socket, request_buffer, request_len) < 0) {
        goto cleanup;
    }

    for (;;) {
        ssize_t received = recv_retry(remote_socket, io_buffer, sizeof(io_buffer));
        if (received < 0) {
            goto cleanup;
        }
        if (received == 0) {
            break;
        }

        if (send_all(client_socket, io_buffer, (size_t)received) < 0) {
            goto cleanup;
        }

        int cache_status = append_to_response(&response_buffer, &response_len,
                                              &response_capacity, io_buffer,
                                              (size_t)received);
        if (cache_status < 0) {
            goto cleanup;
        }
    }

    if (response_len > 0) {
        (void)add_cache_element(response_buffer, response_len, cache_key);
    }
    result = 0;

cleanup:
    close_fd(&remote_socket);
    free(response_buffer);
    free(request_buffer);
    return result;
}

static int check_http_version(const char *msg)
{
    return msg != NULL &&
           (strcmp(msg, "HTTP/1.1") == 0 || strcmp(msg, "HTTP/1.0") == 0);
}

static int read_client_request(int socket_fd, char **request, size_t *request_len)
{
    char *buffer = NULL;
    size_t len = 0;
    size_t capacity = IO_BUFFER_SIZE;

    buffer = malloc(capacity + 1);
    if (buffer == NULL) {
        perror("malloc");
        return -1;
    }

    for (;;) {
        ssize_t received;

        if (len == capacity) {
            char *new_buffer;
            if (capacity >= MAX_REQUEST_SIZE) {
                free(buffer);
                return -2;
            }
            capacity *= 2;
            if (capacity > MAX_REQUEST_SIZE) {
                capacity = MAX_REQUEST_SIZE;
            }
            new_buffer = realloc(buffer, capacity + 1);
            if (new_buffer == NULL) {
                perror("realloc");
                free(buffer);
                return -1;
            }
            buffer = new_buffer;
        }

        received = recv_retry(socket_fd, buffer + len, capacity - len);
        if (received < 0) {
            free(buffer);
            return -1;
        }
        if (received == 0) {
            free(buffer);
            return len == 0 ? 0 : -2;
        }

        if (memchr(buffer + len, '\0', (size_t)received) != NULL) {
            free(buffer);
            return -2;
        }

        len += (size_t)received;
        buffer[len] = '\0';
        if (strstr(buffer, "\r\n\r\n") != NULL) {
            *request = buffer;
            *request_len = len;
            return 1;
        }
    }
}

static int parse_port(const char *port_text, int *port)
{
    char *end = NULL;
    long parsed;

    if (port_text == NULL || *port_text == '\0') {
        return -1;
    }

    errno = 0;
    parsed = strtol(port_text, &end, 10);
    if (errno != 0 || *end != '\0' || parsed <= 0 || parsed > 65535) {
        fprintf(stderr, "invalid port: %s\n", port_text);
        return -1;
    }

    *port = (int)parsed;
    return 0;
}

static void *thread_fn(void *socket_arg)
{
    int client_socket = *(int *)socket_arg;
    char *buffer = NULL;
    size_t request_len = 0;

    free(socket_arg);

    int read_status = read_client_request(client_socket, &buffer, &request_len);
    if (read_status == 1) {
        char *cached_data = NULL;
        size_t cached_len = 0;

        if (cache_find_copy(buffer, &cached_data, &cached_len)) {
            if (send_all(client_socket, cached_data, cached_len) == 0) {
                printf("cache hit\n");
            }
            free(cached_data);
        } else {
            struct ParsedRequest *request = ParsedRequest_create();
            if (request == NULL) {
                perror("ParsedRequest_create");
                (void)send_error_message(client_socket, 500);
            } else if (ParsedRequest_parse(request, buffer, (int)request_len) < 0) {
                (void)send_error_message(client_socket, 400);
            } else if (strcmp(request->method, "GET") != 0) {
                (void)send_error_message(client_socket, 501);
            } else if (!request->host || !request->path || !check_http_version(request->version)) {
                (void)send_error_message(client_socket, 505);
            } else if (handle_request(client_socket, request, buffer) < 0) {
                (void)send_error_message(client_socket, 500);
            }

            ParsedRequest_destroy(request);
        }
    } else if (read_status == -2) {
        (void)send_error_message(client_socket, 400);
    }

    if (shutdown(client_socket, SHUT_RDWR) < 0 && errno != ENOTCONN) {
        perror("shutdown");
    }
    close_fd(&client_socket);
    free(buffer);

    release_client_slot();
    return NULL;
}

int main(int argc, char *argv[])
{
    struct sockaddr_in server_addr;
    int reuse = 1;

    if (argc != 2 || parse_port(argv[1], &port_number) < 0) {
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        return EXIT_FAILURE;
    }

    if (signal(SIGPIPE, SIG_IGN) == SIG_ERR) {
        perror("signal");
        return EXIT_FAILURE;
    }

    if (sem_init(&client_limit, 0, MAX_CLIENTS) < 0) {
        perror("sem_init");
        return EXIT_FAILURE;
    }

    proxy_socket_id = socket(AF_INET, SOCK_STREAM, 0);
    if (proxy_socket_id < 0) {
        perror("socket");
        destroy_client_limit();
        return EXIT_FAILURE;
    }

    if (setsockopt(proxy_socket_id, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        perror("setsockopt");
        close_fd(&proxy_socket_id);
        destroy_client_limit();
        return EXIT_FAILURE;
    }

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons((uint16_t)port_number);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    if (bind(proxy_socket_id, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        close_fd(&proxy_socket_id);
        destroy_client_limit();
        return EXIT_FAILURE;
    }

    if (listen(proxy_socket_id, MAX_CLIENTS) < 0) {
        perror("listen");
        close_fd(&proxy_socket_id);
        destroy_client_limit();
        return EXIT_FAILURE;
    }

    printf("Proxy listening on port %d\n", port_number);

    for (;;) {
        struct sockaddr_in client_addr;
        socklen_t client_len = sizeof(client_addr);
        int client_socket;
        int *thread_socket;
        pthread_attr_t thread_attr;
        pthread_t thread_id;
        char client_ip[INET_ADDRSTRLEN] = "unknown";

        client_socket = accept(proxy_socket_id, (struct sockaddr *)&client_addr, &client_len);
        if (client_socket < 0) {
            if (errno == EINTR) {
                continue;
            }
            perror("accept");
            continue;
        }

        if (inet_ntop(AF_INET, &client_addr.sin_addr, client_ip, sizeof(client_ip)) == NULL) {
            perror("inet_ntop");
        }
        printf("Client connected from %s:%u\n", client_ip, ntohs(client_addr.sin_port));

        if (acquire_client_slot() < 0) {
            close_fd(&client_socket);
            continue;
        }

        thread_socket = malloc(sizeof(*thread_socket));
        if (thread_socket == NULL) {
            perror("malloc");
            release_client_slot();
            close_fd(&client_socket);
            continue;
        }
        *thread_socket = client_socket;

        int create_status = pthread_attr_init(&thread_attr);
        if (create_status != 0) {
            fprintf(stderr, "pthread_attr_init: %s\n", strerror(create_status));
            free(thread_socket);
            release_client_slot();
            close_fd(&client_socket);
            continue;
        }

        create_status = pthread_attr_setdetachstate(&thread_attr, PTHREAD_CREATE_DETACHED);
        if (create_status != 0) {
            fprintf(stderr, "pthread_attr_setdetachstate: %s\n", strerror(create_status));
            int destroy_status = pthread_attr_destroy(&thread_attr);
            if (destroy_status != 0) {
                fprintf(stderr, "pthread_attr_destroy: %s\n", strerror(destroy_status));
            }
            free(thread_socket);
            release_client_slot();
            close_fd(&client_socket);
            continue;
        }

        create_status = pthread_create(&thread_id, &thread_attr, thread_fn, thread_socket);
        int destroy_status = pthread_attr_destroy(&thread_attr);
        if (destroy_status != 0) {
            fprintf(stderr, "pthread_attr_destroy: %s\n", strerror(destroy_status));
        }
        if (create_status != 0) {
            fprintf(stderr, "pthread_create: %s\n", strerror(create_status));
            free(thread_socket);
            release_client_slot();
            close_fd(&client_socket);
            continue;
        }
    }

    free_cache();
    close_fd(&proxy_socket_id);
    destroy_client_limit();
    int lock_status = pthread_mutex_destroy(&cache_lock);
    if (lock_status != 0) {
        fprintf(stderr, "pthread_mutex_destroy: %s\n", strerror(lock_status));
    }
    return EXIT_SUCCESS;
}

static uint64_t hash_url(const char *url)
{
    uint64_t hash = UINT64_C(1469598103934665603);

    while (*url != '\0') {
        hash ^= (unsigned char)*url;
        hash *= UINT64_C(1099511628211);
        url++;
    }

    return hash;
}

static size_t cache_entry_size(const cache_element *entry)
{
    return entry->len + entry->url_len + sizeof(*entry);
}

static cache_element *cache_lookup_locked(const char *url, uint64_t hash)
{
    size_t bucket = hash % CACHE_BUCKET_COUNT;

    for (cache_element *entry = cache_table[bucket]; entry != NULL;
         entry = entry->hash_next) {
        if (entry->hash == hash && strcmp(entry->url, url) == 0) {
            return entry;
        }
    }

    return NULL;
}

static void cache_hash_insert_locked(cache_element *entry)
{
    size_t bucket = entry->hash % CACHE_BUCKET_COUNT;

    entry->hash_prev = NULL;
    entry->hash_next = cache_table[bucket];
    if (cache_table[bucket] != NULL) {
        cache_table[bucket]->hash_prev = entry;
    }
    cache_table[bucket] = entry;
}

static void cache_hash_remove_locked(cache_element *entry)
{
    size_t bucket = entry->hash % CACHE_BUCKET_COUNT;

    if (entry->hash_prev != NULL) {
        entry->hash_prev->hash_next = entry->hash_next;
    } else {
        cache_table[bucket] = entry->hash_next;
    }

    if (entry->hash_next != NULL) {
        entry->hash_next->hash_prev = entry->hash_prev;
    }

    entry->hash_prev = NULL;
    entry->hash_next = NULL;
}

static void cache_lru_insert_front_locked(cache_element *entry)
{
    entry->lru_prev = NULL;
    entry->lru_next = cache_mru;

    if (cache_mru != NULL) {
        cache_mru->lru_prev = entry;
    } else {
        cache_lru = entry;
    }

    cache_mru = entry;
}

static void cache_lru_remove_locked(cache_element *entry)
{
    if (entry->lru_prev != NULL) {
        entry->lru_prev->lru_next = entry->lru_next;
    } else {
        cache_mru = entry->lru_next;
    }

    if (entry->lru_next != NULL) {
        entry->lru_next->lru_prev = entry->lru_prev;
    } else {
        cache_lru = entry->lru_prev;
    }

    entry->lru_prev = NULL;
    entry->lru_next = NULL;
}

static void cache_lru_move_front_locked(cache_element *entry)
{
    if (entry == cache_mru) {
        return;
    }

    cache_lru_remove_locked(entry);
    cache_lru_insert_front_locked(entry);
}

static void cache_free_entry(cache_element *entry)
{
    free(entry->data);
    free(entry->url);
    free(entry);
}

static int cache_find_copy(const char *url, char **data, size_t *len)
{
    cache_element *entry;
    int lock_status;
    uint64_t hash;

    if (url == NULL || data == NULL || len == NULL) {
        return 0;
    }

    hash = hash_url(url);
    lock_status = pthread_mutex_lock(&cache_lock);
    if (lock_status != 0) {
        fprintf(stderr, "pthread_mutex_lock: %s\n", strerror(lock_status));
        return 0;
    }

    entry = cache_lookup_locked(url, hash);
    if (entry != NULL) {
        char *copy = malloc(entry->len);
        if (copy == NULL) {
            perror("malloc");
            unlock_cache();
            return 0;
        }
        memcpy(copy, entry->data, entry->len);
        cache_lru_move_front_locked(entry);
        *data = copy;
        *len = entry->len;
        unlock_cache();
        return 1;
    }

    unlock_cache();
    return 0;
}

static void remove_cache_element_locked(void)
{
    cache_element *oldest = cache_lru;

    if (oldest == NULL) {
        return;
    }

    cache_lru_remove_locked(oldest);
    cache_hash_remove_locked(oldest);
    cache_size -= cache_entry_size(oldest);
    cache_free_entry(oldest);
}

static int add_cache_element(const char *data, size_t size, const char *url)
{
    cache_element *entry;
    cache_element *new_entry = NULL;
    size_t url_len;
    size_t element_size;
    int lock_status;
    uint64_t hash;

    if (data == NULL || url == NULL || size == 0) {
        return 0;
    }

    url_len = strlen(url) + 1;
    if (size > MAX_CACHE_ELEMENT_SIZE ||
        url_len > MAX_CACHE_ELEMENT_SIZE - size ||
        sizeof(cache_element) > MAX_CACHE_ELEMENT_SIZE - size - url_len) {
        return 0;
    }
    element_size = size + url_len + sizeof(cache_element);
    hash = hash_url(url);

    lock_status = pthread_mutex_lock(&cache_lock);
    if (lock_status != 0) {
        fprintf(stderr, "pthread_mutex_lock: %s\n", strerror(lock_status));
        return 0;
    }

    entry = cache_lookup_locked(url, hash);
    if (entry != NULL) {
        cache_lru_remove_locked(entry);
        cache_hash_remove_locked(entry);
        cache_size -= cache_entry_size(entry);
        cache_free_entry(entry);
    }

    while (cache_lru != NULL && cache_size + element_size > MAX_CACHE_SIZE) {
        remove_cache_element_locked();
    }

    new_entry = calloc(1, sizeof(*new_entry));
    if (new_entry == NULL) {
        perror("calloc");
        unlock_cache();
        return 0;
    }

    new_entry->data = malloc(size);
    new_entry->url = malloc(url_len);
    if (new_entry->data == NULL || new_entry->url == NULL) {
        perror("malloc");
        free(new_entry->data);
        free(new_entry->url);
        free(new_entry);
        unlock_cache();
        return 0;
    }

    memcpy(new_entry->data, data, size);
    memcpy(new_entry->url, url, url_len);
    new_entry->len = size;
    new_entry->url_len = url_len;
    new_entry->hash = hash;
    cache_hash_insert_locked(new_entry);
    cache_lru_insert_front_locked(new_entry);
    cache_size += element_size;

    unlock_cache();
    return 1;
}

static void free_cache(void)
{
    cache_element *entry = cache_mru;

    while (entry != NULL) {
        cache_element *next = entry->lru_next;
        cache_free_entry(entry);
        entry = next;
    }

    memset(cache_table, 0, sizeof(cache_table));
    cache_mru = NULL;
    cache_lru = NULL;
    cache_size = 0;
}
