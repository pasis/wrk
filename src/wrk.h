#ifndef WRK_H
#define WRK_H

#include "config.h"
#include <pthread.h>
#include <inttypes.h>
#include <sys/types.h>
#include <netdb.h>
#include <sys/socket.h>

#include <openssl/ssl.h>
#include <openssl/err.h>
#include <lua.h>

#include "stats.h"
#include "ae.h"
#include "http_parser.h"

#define RECVBUF  8192

#define MAX_THREAD_RATE_S   10000000
#define SOCKET_TIMEOUT_MS   2000
#define RECORD_INTERVAL_MS  100
#define THREAD_SYNC_INTERVAL_MS 1000

extern const char *VERSION;

typedef struct {
    pthread_t thread;
    aeEventLoop *loop;
    struct addrinfo *addr;
    uint64_t connections;
    uint64_t complete;
    uint64_t requests;
    uint64_t bytes;
    uint64_t start;
    uint64_t phase_normal_start;
    int phase;
    lua_State *L;
    errors errors;
    struct connection *cs;
    char *local_ip;
} thread;

typedef struct {
    char  *buffer;
    size_t length;
    char  *cursor;
} buffer;

typedef struct connection {
    thread *thread;
    http_parser parser;
    enum {
        FIELD, VALUE
    } state;
    int fd;
    int connect_mask;
    SSL *ssl;
    bool is_connected;
    bool delayed;
    uint64_t start;
    uint64_t r_completed;
    uint64_t r_started;
    uint64_t r_read;
    char *request;
    size_t length;
    size_t written;
    uint64_t pending;
    buffer headers;
    buffer body;
    char buf[RECVBUF];
} connection;

extern char *g_local_ip;

void bind_socket(int fd, sa_family_t family, const char *addr);

#endif /* WRK_H */
