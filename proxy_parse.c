#include "proxy_parse.h"

#include <limits.h>
#include <strings.h>

#define DEFAULT_NHDRS 8
#define MAX_REQ_LEN 65535
#define MIN_REQ_LEN 4

static const char *root_abs_path = "/";

static int ParsedRequest_printRequestLine(struct ParsedRequest *pr, char *buf,
                                          size_t buflen, size_t *written);
static size_t ParsedRequest_requestLineLen(struct ParsedRequest *pr);
static void ParsedRequest_reset_parse_fields(struct ParsedRequest *parse);
static int ParsedHeader_reserve(struct ParsedRequest *pr, size_t needed);
static int parse_absolute_uri(struct ParsedRequest *parse, char *uri);
static int copy_path(struct ParsedRequest *parse, const char *path);

void debug(const char *format, ...)
{
    va_list args;

    if (DEBUG) {
        va_start(args, format);
        vfprintf(stderr, format, args);
        va_end(args);
    }
}

static int ParsedHeader_reserve(struct ParsedRequest *pr, size_t needed)
{
    struct ParsedHeader *new_headers;
    size_t new_len;

    if (pr->headerslen >= needed) {
        return 0;
    }

    new_len = pr->headerslen == 0 ? DEFAULT_NHDRS : pr->headerslen;
    while (new_len < needed) {
        if (new_len > SIZE_MAX / 2) {
            return -1;
        }
        new_len *= 2;
    }

    new_headers = realloc(pr->headers, new_len * sizeof(*new_headers));
    if (new_headers == NULL) {
        return -1;
    }

    memset(new_headers + pr->headerslen, 0,
           (new_len - pr->headerslen) * sizeof(*new_headers));
    pr->headers = new_headers;
    pr->headerslen = new_len;
    return 0;
}

int ParsedHeader_set(struct ParsedRequest *pr, const char *key, const char *value)
{
    struct ParsedHeader *ph;
    char *key_copy;
    char *value_copy;
    size_t key_len;
    size_t value_len;

    if (pr == NULL || key == NULL || value == NULL || *key == '\0') {
        return -1;
    }

    (void)ParsedHeader_remove(pr, key);

    if (ParsedHeader_reserve(pr, pr->headersused + 1) < 0) {
        return -1;
    }

    key_len = strlen(key) + 1;
    value_len = strlen(value) + 1;
    key_copy = malloc(key_len);
    value_copy = malloc(value_len);
    if (key_copy == NULL || value_copy == NULL) {
        free(key_copy);
        free(value_copy);
        return -1;
    }

    memcpy(key_copy, key, key_len);
    memcpy(value_copy, value, value_len);

    ph = pr->headers + pr->headersused;
    ph->key = key_copy;
    ph->value = value_copy;
    ph->keylen = key_len;
    ph->valuelen = value_len;
    pr->headersused++;
    return 0;
}

struct ParsedHeader *ParsedHeader_get(struct ParsedRequest *pr, const char *key)
{
    if (pr == NULL || key == NULL) {
        return NULL;
    }

    for (size_t i = 0; i < pr->headersused; i++) {
        struct ParsedHeader *tmp = pr->headers + i;
        if (tmp->key != NULL && strcasecmp(tmp->key, key) == 0) {
            return tmp;
        }
    }
    return NULL;
}

int ParsedHeader_remove(struct ParsedRequest *pr, const char *key)
{
    if (pr == NULL || key == NULL) {
        return -1;
    }

    for (size_t i = 0; i < pr->headersused; i++) {
        if (pr->headers[i].key != NULL && strcasecmp(pr->headers[i].key, key) == 0) {
            free(pr->headers[i].key);
            free(pr->headers[i].value);

            if (i + 1 < pr->headersused) {
                memmove(pr->headers + i, pr->headers + i + 1,
                        (pr->headersused - i - 1) * sizeof(*pr->headers));
            }
            pr->headersused--;
            memset(pr->headers + pr->headersused, 0, sizeof(*pr->headers));
            return 0;
        }
    }

    return -1;
}

static int ParsedHeader_create(struct ParsedRequest *pr)
{
    pr->headers = calloc(DEFAULT_NHDRS, sizeof(*pr->headers));
    if (pr->headers == NULL) {
        pr->headerslen = 0;
        return -1;
    }
    pr->headerslen = DEFAULT_NHDRS;
    pr->headersused = 0;
    return 0;
}

static size_t ParsedHeader_lineLen(struct ParsedHeader *ph)
{
    if (ph == NULL || ph->key == NULL || ph->value == NULL) {
        return 0;
    }
    return strlen(ph->key) + strlen(ph->value) + 4;
}

size_t ParsedHeader_headersLen(struct ParsedRequest *pr)
{
    size_t len = 2;

    if (pr == NULL) {
        return 0;
    }

    for (size_t i = 0; i < pr->headersused; i++) {
        len += ParsedHeader_lineLen(pr->headers + i);
    }
    return len;
}

static int ParsedHeader_printHeaders(struct ParsedRequest *pr, char *buf, size_t len)
{
    char *current = buf;

    if (pr == NULL || buf == NULL || len < ParsedHeader_headersLen(pr)) {
        return -1;
    }

    for (size_t i = 0; i < pr->headersused; i++) {
        struct ParsedHeader *ph = pr->headers + i;
        size_t key_len;
        size_t value_len;

        if (ph->key == NULL || ph->value == NULL) {
            continue;
        }

        key_len = strlen(ph->key);
        value_len = strlen(ph->value);
        memcpy(current, ph->key, key_len);
        current += key_len;
        memcpy(current, ": ", 2);
        current += 2;
        memcpy(current, ph->value, value_len);
        current += value_len;
        memcpy(current, "\r\n", 2);
        current += 2;
    }

    memcpy(current, "\r\n", 2);
    current += 2;
    if ((size_t)(current - buf) < len) {
        *current = '\0';
    }
    return 0;
}

static void ParsedHeader_destroyOne(struct ParsedHeader *ph)
{
    if (ph == NULL) {
        return;
    }

    free(ph->key);
    free(ph->value);
    ph->key = NULL;
    ph->value = NULL;
    ph->keylen = 0;
    ph->valuelen = 0;
}

static void ParsedHeader_destroy(struct ParsedRequest *pr)
{
    if (pr == NULL) {
        return;
    }

    for (size_t i = 0; i < pr->headersused; i++) {
        ParsedHeader_destroyOne(pr->headers + i);
    }

    free(pr->headers);
    pr->headers = NULL;
    pr->headersused = 0;
    pr->headerslen = 0;
}

static int ParsedHeader_parse(struct ParsedRequest *pr, const char *line,
                              size_t line_len)
{
    const char *colon;
    const char *value_start;
    char *key = NULL;
    char *value = NULL;
    size_t key_len;
    size_t value_len;
    int ret;

    colon = memchr(line, ':', line_len);
    if (colon == NULL || colon == line) {
        return -1;
    }

    key_len = (size_t)(colon - line);
    value_start = colon + 1;
    while (value_start < line + line_len &&
           (*value_start == ' ' || *value_start == '\t')) {
        value_start++;
    }
    value_len = (size_t)(line + line_len - value_start);

    key = malloc(key_len + 1);
    value = malloc(value_len + 1);
    if (key == NULL || value == NULL) {
        free(key);
        free(value);
        return -1;
    }

    memcpy(key, line, key_len);
    key[key_len] = '\0';
    memcpy(value, value_start, value_len);
    value[value_len] = '\0';

    ret = ParsedHeader_set(pr, key, value);
    free(key);
    free(value);
    return ret;
}

void ParsedRequest_destroy(struct ParsedRequest *pr)
{
    if (pr == NULL) {
        return;
    }

    free(pr->buf);
    free(pr->path);
    ParsedHeader_destroy(pr);
    free(pr);
}

struct ParsedRequest *ParsedRequest_create(void)
{
    struct ParsedRequest *pr = calloc(1, sizeof(*pr));
    if (pr == NULL) {
        return NULL;
    }

    if (ParsedHeader_create(pr) < 0) {
        free(pr);
        return NULL;
    }

    return pr;
}

int ParsedRequest_unparse(struct ParsedRequest *pr, char *buf, size_t buflen)
{
    size_t written;

    if (ParsedRequest_printRequestLine(pr, buf, buflen, &written) < 0) {
        return -1;
    }
    return ParsedHeader_printHeaders(pr, buf + written, buflen - written);
}

int ParsedRequest_unparse_headers(struct ParsedRequest *pr, char *buf, size_t buflen)
{
    return ParsedHeader_printHeaders(pr, buf, buflen);
}

size_t ParsedRequest_totalLen(struct ParsedRequest *pr)
{
    return ParsedRequest_requestLineLen(pr) + ParsedHeader_headersLen(pr);
}

static void ParsedRequest_reset_parse_fields(struct ParsedRequest *parse)
{
    free(parse->buf);
    free(parse->path);
    parse->buf = NULL;
    parse->path = NULL;
    parse->method = NULL;
    parse->protocol = NULL;
    parse->host = NULL;
    parse->port = NULL;
    parse->version = NULL;
    parse->buflen = 0;
    ParsedHeader_destroy(parse);
    (void)ParsedHeader_create(parse);
}

static int copy_path(struct ParsedRequest *parse, const char *path)
{
    size_t path_len;

    if (path == NULL || *path == '\0') {
        path = root_abs_path;
    }

    if (path[0] == '/') {
        if (path[1] == '/') {
            return -1;
        }
        path_len = strlen(path) + 1;
        parse->path = malloc(path_len);
        if (parse->path == NULL) {
            return -1;
        }
        memcpy(parse->path, path, path_len);
        return 0;
    }

    path_len = strlen(path);
    parse->path = malloc(path_len + 2);
    if (parse->path == NULL) {
        return -1;
    }
    parse->path[0] = '/';
    memcpy(parse->path + 1, path, path_len + 1);
    return 0;
}

static int parse_absolute_uri(struct ParsedRequest *parse, char *uri)
{
    char *scheme_end;
    char *authority;
    char *path;
    char *port;

    scheme_end = strstr(uri, "://");
    if (scheme_end == NULL || scheme_end == uri) {
        return -1;
    }

    *scheme_end = '\0';
    parse->protocol = uri;
    if (strcasecmp(parse->protocol, "http") != 0) {
        return -1;
    }

    authority = scheme_end + 3;
    path = strchr(authority, '/');
    if (path != NULL) {
        if (path[1] == '/') {
            return -1;
        }
        *path = '\0';
        path++;
    }

    if (*authority == '\0') {
        return -1;
    }

    port = strrchr(authority, ':');
    if (port != NULL) {
        char *end = NULL;
        long parsed;

        *port = '\0';
        port++;
        errno = 0;
        parsed = strtol(port, &end, 10);
        if (errno != 0 || end == port || *end != '\0' ||
            parsed <= 0 || parsed > 65535) {
            return -1;
        }
        parse->port = port;
    }

    if (*authority == '\0') {
        return -1;
    }

    parse->host = authority;
    return copy_path(parse, path);
}

int ParsedRequest_parse(struct ParsedRequest *parse, const char *buf, int buflen)
{
    char *headers_start;
    char *headers_end;
    char *request_line_end;
    char *uri;
    char *saveptr = NULL;

    if (parse == NULL || parse->buf != NULL || buflen < MIN_REQ_LEN ||
        buflen > MAX_REQ_LEN || buf == NULL) {
        return -1;
    }

    parse->buf = malloc((size_t)buflen + 1);
    if (parse->buf == NULL) {
        return -1;
    }
    memcpy(parse->buf, buf, (size_t)buflen);
    parse->buf[buflen] = '\0';
    parse->buflen = (size_t)buflen + 1;

    headers_end = strstr(parse->buf, "\r\n\r\n");
    request_line_end = strstr(parse->buf, "\r\n");
    if (headers_end == NULL || request_line_end == NULL || request_line_end == parse->buf) {
        ParsedRequest_reset_parse_fields(parse);
        return -1;
    }
    *request_line_end = '\0';

    parse->method = strtok_r(parse->buf, " ", &saveptr);
    uri = strtok_r(NULL, " ", &saveptr);
    parse->version = strtok_r(NULL, " ", &saveptr);
    if (parse->method == NULL || uri == NULL || parse->version == NULL ||
        strtok_r(NULL, " ", &saveptr) != NULL) {
        ParsedRequest_reset_parse_fields(parse);
        return -1;
    }

    if (strcmp(parse->method, "GET") != 0 ||
        strncmp(parse->version, "HTTP/", 5) != 0 ||
        parse_absolute_uri(parse, uri) < 0) {
        ParsedRequest_reset_parse_fields(parse);
        return -1;
    }

    headers_start = request_line_end + 2;
    while (headers_start < headers_end) {
        char *line_end = strstr(headers_start, "\r\n");
        size_t line_len;

        if (line_end == NULL || line_end > headers_end) {
            ParsedRequest_reset_parse_fields(parse);
            return -1;
        }

        line_len = (size_t)(line_end - headers_start);
        if (line_len > 0 && ParsedHeader_parse(parse, headers_start, line_len) < 0) {
            ParsedRequest_reset_parse_fields(parse);
            return -1;
        }
        headers_start = line_end + 2;
    }

    return 0;
}

static size_t ParsedRequest_requestLineLen(struct ParsedRequest *pr)
{
    size_t len;

    if (pr == NULL || pr->method == NULL || pr->protocol == NULL ||
        pr->host == NULL || pr->path == NULL || pr->version == NULL) {
        return 0;
    }

    len = strlen(pr->method) + 1 + strlen(pr->protocol) + 3 +
          strlen(pr->host) + strlen(pr->path) + 1 + strlen(pr->version) + 2;
    if (pr->port != NULL) {
        len += strlen(pr->port) + 1;
    }
    return len;
}

static int ParsedRequest_printRequestLine(struct ParsedRequest *pr, char *buf,
                                          size_t buflen, size_t *written)
{
    int ret;

    if (pr == NULL || buf == NULL || written == NULL ||
        ParsedRequest_requestLineLen(pr) >= buflen) {
        return -1;
    }

    if (pr->port != NULL) {
        ret = snprintf(buf, buflen, "%s %s://%s:%s%s %s\r\n",
                       pr->method, pr->protocol, pr->host, pr->port,
                       pr->path, pr->version);
    } else {
        ret = snprintf(buf, buflen, "%s %s://%s%s %s\r\n",
                       pr->method, pr->protocol, pr->host,
                       pr->path, pr->version);
    }

    if (ret < 0 || (size_t)ret >= buflen) {
        return -1;
    }

    *written = (size_t)ret;
    return 0;
}
