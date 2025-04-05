extern "C" {
#include "postgres.h"
#include "access/htup_details.h"
#include "catalog/pg_user_mapping.h"
#include "commands/defrem.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "storage/fd.h"
#include "storage/latch.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/memutils.h"
#include "utils/syscache.h"
#include "connection.hpp"
}



typedef Oid ConnCacheKey;

typedef struct ConnCacheEntry
{
    ConnCacheKey key;           /* hash key (must be first) */
    WS_TAOS *conn;             /* connection to TDengine server, or NULL */
    bool invalidated;          /* true if reconnect is pending */
    uint32 server_hashvalue;   /* hash value of foreign server OID */
    uint32 mapping_hashvalue;  /* hash value of user mapping OID */
} ConnCacheEntry;

static HTAB *ConnectionHash = NULL;

/* Function prototypes */
static void tdengine_make_new_connection(ConnCacheEntry *entry, UserMapping *user, tdengine_opt *options);
static WS_TAOS* tdengine_connect_server(tdengine_opt *options);
static void tdengine_disconnect_server(ConnCacheEntry *entry);
static void tdengine_inval_callback(Datum arg, int cacheid, uint32 hashvalue);

WS_TAOS*
tdengine_get_connection(UserMapping *user, tdengine_opt *options)
{
    bool found;
    ConnCacheEntry *entry;
    ConnCacheKey key;

    /* First time through, initialize connection cache hashtable */
    if (ConnectionHash == NULL)
    {
        HASHCTL ctl;

        ctl.keysize = sizeof(ConnCacheKey);
        ctl.entrysize = sizeof(ConnCacheEntry);
        ConnectionHash = hash_create("tdengine_fdw connections", 8,
                                   &ctl,
                                   HASH_ELEM | HASH_BLOBS);

        /* Register callbacks for connection cleanup */
        CacheRegisterSyscacheCallback(FOREIGNSERVEROID,
                                    tdengine_inval_callback, (Datum) 0);
        CacheRegisterSyscacheCallback(USERMAPPINGOID,
                                    tdengine_inval_callback, (Datum) 0);
    }

    key = user->umid;

    entry = (ConnCacheEntry *)hash_search(ConnectionHash, &key, HASH_ENTER, &found);
    if (!found)
    {
        entry->conn = NULL;
    }

    if (entry->conn != NULL && entry->invalidated)
    {
        elog(DEBUG3, "tdengine_fdw: closing connection %p for option changes to take effect",
             entry->conn);
        tdengine_disconnect_server(entry);
    }

    if (entry->conn == NULL)
        tdengine_make_new_connection(entry, user, options);

    return entry->conn;
}

static void
tdengine_make_new_connection(ConnCacheEntry *entry, UserMapping *user, tdengine_opt *opts)
{
    ForeignServer *server = GetForeignServer(user->serverid);

    Assert(entry->conn == NULL);

    /* Reset transient state */
    entry->invalidated = false;
    entry->server_hashvalue = GetSysCacheHashValue1(FOREIGNSERVEROID,
                                                   ObjectIdGetDatum(server->serverid));
    entry->mapping_hashvalue = GetSysCacheHashValue1(USERMAPPINGOID,
                                                    ObjectIdGetDatum(user->umid));

    /* Create new connection */
    entry->conn = tdengine_connect_server(opts);

    elog(DEBUG3, "tdengine_fdw: new TDengine connection %p for server \"%s\" (user mapping oid %u, userid %u)",
         entry->conn, server->servername, user->umid, user->userid);
}

WS_TAOS*
create_tdengine_connection(char* dsn)
{
    WS_TAOS* taos = ws_connect(dsn);
    if (taos == NULL)
    {
        int errno = ws_errno(NULL);
        const char* errstr = ws_errstr(NULL);
        elog(ERROR, "could not connect to TDengine: %s (error code: %d)",
             errstr, errno);
    }
    return taos;
}

static WS_TAOS*
tdengine_connect_server(tdengine_opt *opts)
{
    char dsn[1024];
    snprintf(dsn, sizeof(dsn), 
             "%s[+%s]://[%s:%s@]%s:%d/%s?%s",
             opts->driver ? opts->driver : "",
             opts->protocol ? opts->protocol : "",
             opts->svr_username ? opts->svr_username : "",
             opts->svr_password ? opts->svr_password : "",
             opts->svr_address ? opts->svr_address : "localhost",
             opts->svr_port ? opts->svr_port : 6030,
             opts->svr_database ? opts->svr_database : "");
    
    return create_tdengine_connection(dsn);
}


static void
tdengine_disconnect_server(ConnCacheEntry *entry)
{
    if (entry && entry->conn != NULL)
    {
        ws_close(entry->conn);
        entry->conn = NULL;
    }
}

static void
tdengine_inval_callback(Datum arg, int cacheid, uint32 hashvalue)
{
    HASH_SEQ_STATUS scan;
    ConnCacheEntry *entry;

    Assert(cacheid == FOREIGNSERVEROID || cacheid == USERMAPPINGOID);

    hash_seq_init(&scan, ConnectionHash);
    while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
    {
        if (entry->conn == NULL)
            continue;

        if (hashvalue == 0 ||
            (cacheid == FOREIGNSERVEROID &&
             entry->server_hashvalue == hashvalue) ||
            (cacheid == USERMAPPINGOID &&
             entry->mapping_hashvalue == hashvalue))
        {
            entry->invalidated = true;
            elog(DEBUG3, "tdengine_fdw: discarding connection %p", entry->conn);
            tdengine_disconnect_server(entry);
        }
    }
}

void
tdengine_cleanup_connection(void)
{
    HASH_SEQ_STATUS scan;
    ConnCacheEntry *entry;

    if (ConnectionHash == NULL)
        return;

    hash_seq_init(&scan, ConnectionHash);
    while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
    {
        if (entry->conn == NULL)
            continue;

        tdengine_disconnect_server(entry);
    }
}
