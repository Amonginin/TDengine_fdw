#ifndef CONNECTION_HPP
#define CONNECTION_HPP

extern "C" {
#include "tdengine_fdw.h"
#include <taosws.h>
}

/* Get a connection for TDengine server */
extern WS_TAOS* tdengine_get_connection(UserMapping *user, tdengine_opt *options);

/* Create a new TDengine connection */
extern WS_TAOS* create_tdengine_connection(char* dsn);

/* Clean up all connections */
extern void tdengine_cleanup_connection(void);

#endif /* CONNECTION_HPP */