#ifndef PROXY_PARSE
#define PROXY_PARSE

#include <errno.h>
#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define DEBUG 0

struct ParsedHeader {
    char *key;
    size_t keylen;
    char *value;
    size_t valuelen;
};

struct ParsedRequest {
    char *method;
    char *protocol;
    char *host;
    char *port;
    char *path;
    char *version;
    char *buf;
    size_t buflen;
    struct ParsedHeader *headers;
    size_t headersused;
    size_t headerslen;
};

struct ParsedRequest *ParsedRequest_create(void);
int ParsedRequest_parse(struct ParsedRequest *parse, const char *buf, int buflen);
void ParsedRequest_destroy(struct ParsedRequest *pr);

int ParsedRequest_unparse(struct ParsedRequest *pr, char *buf, size_t buflen);
int ParsedRequest_unparse_headers(struct ParsedRequest *pr, char *buf,
                                  size_t buflen);
size_t ParsedRequest_totalLen(struct ParsedRequest *pr);
size_t ParsedHeader_headersLen(struct ParsedRequest *pr);

int ParsedHeader_set(struct ParsedRequest *pr, const char *key, const char *value);
struct ParsedHeader *ParsedHeader_get(struct ParsedRequest *pr, const char *key);
int ParsedHeader_remove(struct ParsedRequest *pr, const char *key);

void debug(const char *format, ...);

#endif
