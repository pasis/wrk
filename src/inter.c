// Copyright (C) 2022 - Alexander Grissik.  All rights reserved.

#include <arpa/inet.h>
#include <netinet/in.h>
#include <net/if.h>
#include <string.h>
#include "script.h"
#include "zmalloc.h"

extern char *copy_url_part(const char *, struct http_parser_url *, enum http_parser_url_fields);

static int *g_sync_sockets;

static in_port_t get_sockaddr_port(const struct sockaddr *addr)
{
    return ntohs((addr->sa_family == AF_INET
                  ? ((const struct sockaddr_in *)addr)->sin_port
                  : ((const struct sockaddr_in6 *)addr)->sin6_port));
}

static void print_sockaddr(const struct sockaddr *addr)
{
    char buffer[INET6_ADDRSTRLEN];

    if (addr->sa_family == AF_INET) {
        if (inet_ntop(AF_INET, &((const struct sockaddr_in *)addr)->sin_addr,
                      buffer, sizeof(buffer))) {
            fprintf(stdout, "%s", buffer);
        }
    } else {
        fprintf(stdout, "[");
        if (inet_ntop(AF_INET6, &((const struct sockaddr_in6 *)addr)->sin6_addr,
                      buffer, sizeof(buffer))) {
            fprintf(stdout, "%s", buffer);
        }
        fprintf(stdout, "]");
    }

    fprintf(stdout, ":%u", (unsigned)get_sockaddr_port(addr));
}

void inter_process_clear_sync_sockets(uint16_t secondaries_num)
{
    if (!g_sync_sockets)
        return;

    size_t socks = MAX(secondaries_num, 1U);
    while (socks-- > 0)
        if (g_sync_sockets[socks] > 0)
            close(g_sync_sockets[socks]);
}

bool inter_process_initiate_sync(const char *sync_ipport, uint16_t secondaries_num)
{
    if (!sync_ipport)
        return true;

    g_sync_sockets = (int *)calloc(MAX(secondaries_num, 1U), sizeof(int));

    struct http_parser_url u;
    memset(&u, 0, sizeof(u));
    u.field_data[UF_HOST].off = 0U;
    u.field_data[UF_HOST].len = strlen(sync_ipport);
    u.field_set = (1 << UF_HOST);

    if (0 != http_parse_host(sync_ipport, &u, false)) {
        fprintf(stderr, "Unable to parse Sync address, errno: %d\n", errno);
        return false;
    }

    char *host = copy_url_part(sync_ipport, &u, UF_HOST);
    char *port = copy_url_part(sync_ipport, &u, UF_PORT);

    struct addrinfo *addrs = NULL;
    struct addrinfo hints = {
        .ai_family   = AF_UNSPEC,
        .ai_socktype = SOCK_STREAM
    }; 

    int rc = getaddrinfo(host, port, &hints, &addrs);
    zfree(host);
    zfree(port);

    if (0 != rc || !addrs) {
        fprintf(stderr, "Unable to resolve Sync %s:%s %d\n", host, port, errno);
        return false;
    }

    struct addrinfo selectedaddr = *addrs;
    struct sockaddr_storage socketstore;
    if (sizeof(socketstore) < selectedaddr.ai_addrlen) {
        freeaddrinfo(addrs);
        fprintf(stderr, "Invalid Sync address %s\n", sync_ipport);
        return false;
    }
    
    selectedaddr.ai_addr = (struct sockaddr *)&socketstore;
    selectedaddr.ai_canonname = NULL;
    memcpy(selectedaddr.ai_addr, addrs->ai_addr, selectedaddr.ai_addrlen);

    freeaddrinfo(addrs);

    fprintf(stdout, "Sync Address: ");
    print_sockaddr(selectedaddr.ai_addr);
    fprintf(stdout, "\n");

    if (secondaries_num > 0) { // Primary
        int listensock = socket(selectedaddr.ai_family, selectedaddr.ai_socktype, selectedaddr.ai_protocol);
        if (listensock <= 0) {
            fprintf(stderr, "Unable to initialize listen Sync socket, errno: %d\n", errno);
            return false;
        }

        if (0 != bind(listensock, selectedaddr.ai_addr, selectedaddr.ai_addrlen)) {
            close(listensock);
            fprintf(stderr, "Unable to bind listen Sync socket, errno: %d\n", errno);
            return false;
        }

        if (0 != listen(listensock, UINT16_MAX)) {
            close(listensock);
            fprintf(stderr, "Unable to listen on listen Sync socket, errno: %d\n", errno);
            return false;
        }

        fprintf(stdout, "Waiting for secondaries to connect ...\n");

        for (uint16_t secondary = 0U; secondary < secondaries_num; ++secondary) {
            g_sync_sockets[secondary] = accept(listensock, NULL, NULL);
            if (g_sync_sockets[secondary] <= 0) {
                close(listensock);
                fprintf(stderr, "Unable to accept Sync connection, errno: %d\n", errno);
                return false;
            }
        }

        close(listensock);

        fprintf(stdout, "All secondaries connected.\n");
    } else { // Secondary
        g_sync_sockets[0] = socket(selectedaddr.ai_family, selectedaddr.ai_socktype, selectedaddr.ai_protocol);
        if (g_sync_sockets[0] <= 0) {
            fprintf(stderr, "Unable to create Sync socket, errno: %d\n", errno);
            return false;
        }

        if (0 != connect(g_sync_sockets[0], selectedaddr.ai_addr, selectedaddr.ai_addrlen)) {
            fprintf(stderr, "Unable to connect to Sync address, errno: %d\n", errno);
            return false;
        }

        fprintf(stdout, "Connected to Primary.\n");
    }

    return true;
}

void inter_process_sync(uint16_t secondaries_num)
{
    if (!g_sync_sockets)
        return;

    int code = 1;
    if (secondaries_num > 0) { // Primary
        for (uint16_t secondary = 0U; secondary < secondaries_num; ++secondary) {
            int rc = recv(g_sync_sockets[secondary], &code, sizeof(code), 0);
            if (rc < sizeof(code) || code != 1) {
                fprintf(stderr, "Unable to recv from secondary, errno: %d, idx: %u, code: %d\n",
                        errno, (unsigned)secondary, code);
            }
        }

        for (uint16_t secondary = 0U; secondary < secondaries_num; ++secondary) {
            int rc = send(g_sync_sockets[secondary], &code, sizeof(code), 0);
            if (rc < sizeof(code)) {
                fprintf(stderr, "Unable to rcv send to secondary, errno: %d, idx: %u\n",
                        errno, (unsigned)secondary);
            }
        }

        fprintf(stdout, "Synced with Secondaries\n");
    } else { // Secondary
        int rc = send(g_sync_sockets[0], &code, sizeof(code), 0);
        if (rc < sizeof(code)) {
            fprintf(stderr, "Unable to send to primary, errno: %d\n",errno);
        } 
            
        rc = recv(g_sync_sockets[0], &code, sizeof(code), 0);
        if (rc < sizeof(code) || code != 1) {
            fprintf(stderr, "Unable to recv from primary, errno: %d, code: %d\n", errno, code);
        } 
            
        fprintf(stdout, "Synced with Primary\n");
    }
}
