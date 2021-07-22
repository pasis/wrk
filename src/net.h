#ifndef NET_H
#define NET_H

#include "config.h"
#include <stdint.h>
#include <openssl/ssl.h>
#include "wrk.h"

typedef enum {
    OK,
    ERROR,
    RETRY
} status;

#define E_WANT_READ  0x1
#define E_WANT_WRITE 0x2

struct sock {
    status ( *connect)(connection *, char *, int *);
    status (   *close)(connection *);
    status (    *read)(connection *, size_t *);
    status (   *write)(connection *, char *, size_t, size_t *);
    size_t (*readable)(connection *);
};

status sock_connect(connection *, char *, int *);
status sock_close(connection *);
status sock_read(connection *, size_t *);
status sock_write(connection *, char *, size_t, size_t *);
size_t sock_readable(connection *);

#endif /* NET_H */
