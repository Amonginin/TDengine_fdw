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
}

#include "connection.hpp"

typedef Oid ConnCacheKey;

/*
 * 连接缓存条目结构体，用于管理TDengine连接缓存
 * 
 * 成员说明：
 * @key 哈希键值，必须是第一个成员，用于在哈希表中快速查找
 * @conn TDengine连接指针，NULL表示无有效连接
 * @invalidated 连接失效标志，true表示需要重新建立连接
 * @server_hashvalue 外部服务器OID的哈希值，用于缓存失效检测
 * @mapping_hashvalue 用户映射OID的哈希值，用于缓存失效检测
 * 
 * 设计说明：
 * 1. 遵循PostgreSQL连接缓存设计规范
 * 2. 使用哈希值优化缓存失效检测性能
 * 3. 支持连接重用和按需重建
 */
typedef struct ConnCacheEntry
{
    ConnCacheKey key;           /* 哈希键值(必须是第一个成员) */
    WS_TAOS *conn;             /* TDengine服务器连接指针，NULL表示无有效连接 */
    bool invalidated;          /* 连接失效标志，true表示需要重新连接 */
    uint32 server_hashvalue;   /* 外部服务器OID的哈希值，用于缓存失效检测 */
    uint32 mapping_hashvalue;  /* 用户映射OID的哈希值，用于缓存失效检测 */
} ConnCacheEntry;

static HTAB *ConnectionHash = NULL;

/* Function prototypes */
static void tdengine_make_new_connection(ConnCacheEntry *entry, UserMapping *user, tdengine_opt *options);
static WS_TAOS* tdengine_connect_server(tdengine_opt *options);
static void tdengine_disconnect_server(ConnCacheEntry *entry);
static void tdengine_inval_callback(Datum arg, int cacheid, uint32 hashvalue);

/*
 * 获取或创建与TDengine服务器的连接
 * 
 * @param user 用户映射信息，包含服务器ID和用户ID
 * @param options 连接选项，包含主机、端口、用户名密码等信息
 * @return 返回已建立的WS_TAOS连接对象
 * 
 * 功能说明：
 * 1. 首次调用时初始化连接缓存哈希表
 * 2. 根据用户映射ID查找或创建连接缓存项
 * 3. 如果连接已存在但被标记为无效(如配置变更)，则关闭旧连接
 * 4. 如果没有有效连接，则创建新连接
 */
WS_TAOS*
tdengine_get_connection(UserMapping *user, tdengine_opt *options)
{
    bool found;
    ConnCacheEntry *entry;
    ConnCacheKey key;

    /* 首次调用时初始化连接缓存哈希表 */
    if (ConnectionHash == NULL)
    {
        HASHCTL ctl;

        ctl.keysize = sizeof(ConnCacheKey);
        ctl.entrysize = sizeof(ConnCacheEntry);
        ConnectionHash = hash_create("tdengine_fdw connections", 8,
                                   &ctl,
                                   HASH_ELEM | HASH_BLOBS);

        /* 注册回调函数用于连接清理 */
        CacheRegisterSyscacheCallback(FOREIGNSERVEROID,
                                    tdengine_inval_callback, (Datum) 0);
        CacheRegisterSyscacheCallback(USERMAPPINGOID,
                                    tdengine_inval_callback, (Datum) 0);
    }

    /* 使用用户映射ID作为哈希键 */
    key = user->umid;

    /* 在哈希表中查找或创建项 */
    entry = (ConnCacheEntry *)hash_search(ConnectionHash, &key, HASH_ENTER, &found);
    if (!found)
    {
        /* 新项初始化连接为NULL */
        entry->conn = NULL;
    }

    /* 检查连接是否无效(如配置变更) */
    if (entry->conn != NULL && entry->invalidated)
    {
        elog(DEBUG3, "tdengine_fdw: closing connection %p for option changes to take effect",
             entry->conn);
        tdengine_disconnect_server(entry);
    }

    /* 如果没有有效连接，则创建新连接 */
    if (entry->conn == NULL)
        tdengine_make_new_connection(entry, user, options);

    return entry->conn;
}

/*
 * 创建新的TDengine服务器连接并初始化连接缓存项
 * 
 * @param entry 连接缓存项指针，用于存储新连接信息
 * @param user 用户映射信息，包含服务器ID和用户ID
 * @param opts 连接选项，包含主机、端口等配置信息
 * 
 * 功能说明：
 * 1. 获取外部服务器信息
 * 2. 重置连接项的临时状态
 * 3. 计算服务器和用户映射的哈希值用于缓存管理
 * 4. 创建新的TDengine服务器连接
 * 5. 记录调试日志
 */
static void
tdengine_make_new_connection(ConnCacheEntry *entry, UserMapping *user, tdengine_opt *opts)
{
    /* 获取外部服务器信息 */
    ForeignServer *server = GetForeignServer(user->serverid);

    /* 确保当前连接为空 */
    Assert(entry->conn == NULL);

    /* 重置连接项的临时状态 */
    entry->invalidated = false;
    /* 计算服务器对象的哈希值用于缓存管理 */
    entry->server_hashvalue = GetSysCacheHashValue1(FOREIGNSERVEROID,
                                                   ObjectIdGetDatum(server->serverid));
    /* 计算用户映射的哈希值用于缓存管理 */
    entry->mapping_hashvalue = GetSysCacheHashValue1(USERMAPPINGOID,
                                                    ObjectIdGetDatum(user->umid));

    /* 创建新的TDengine服务器连接 */
    entry->conn = tdengine_connect_server(opts);

    /* 记录调试日志，包含连接指针、服务器名和用户信息 */
    elog(DEBUG3, "tdengine_fdw: new TDengine connection %p for server \"%s\" (user mapping oid %u, userid %u)",
         entry->conn, server->servername, user->umid, user->userid);
}

/*
 * 创建并返回一个TDengine服务器连接
 * 
 * @param dsn 连接字符串，包含服务器地址、端口、认证信息等
 * @return 成功返回WS_TAOS连接对象，失败抛出ERROR异常
 * 
 * 功能说明：
 * 1. 使用提供的DSN字符串建立TDengine连接
 * 2. 检查连接是否成功建立
 * 3. 连接失败时获取错误信息并抛出PostgreSQL异常
 * 4. 返回已建立的连接对象
 */
WS_TAOS*
create_tdengine_connection(char* dsn)
{
    /* 尝试建立TDengine连接 */
    WS_TAOS* taos = ws_connect(dsn);
    
    /* 检查连接是否成功 */
    if (taos == NULL)
    {
        /* 获取连接错误信息 */
        int errno = ws_errno(NULL);
        const char* errstr = ws_errstr(NULL);
        
        /* 抛出PostgreSQL错误，包含错误描述和错误码 */
        elog(ERROR, "could not connect to TDengine: %s (error code: %d)",
             errstr, errno);
    }
    
    /* 返回已建立的连接对象 */
    return taos;
}

// TODO: 添加对table字段的处理
/*
 * 根据连接选项创建TDengine服务器连接
 * 
 * @param opts 连接选项结构体，包含驱动、协议、认证信息等
 * @return 返回已建立的WS_TAOS连接对象
 * 
 * 功能说明：
 * 1. 格式化生成TDengine连接字符串(DSN)
 * 2. 使用默认值填充可选参数
 * 3. 调用create_tdengine_connection创建实际连接
 * 
 * 连接字符串格式说明：
 * %s[+%s]://[%s:%s@]%s:%d/%s?%s
 * 对应字段：
 * 1. 驱动类型
 * 2. 协议类型
 * 3. 用户名
 * 4. 密码
 * 5. 服务器地址(默认localhost)
 * 6. 服务器端口(默认6030)
 * 7. 数据库名称
 */
static WS_TAOS*
tdengine_connect_server(tdengine_opt *opts)
{
    /* 分配缓冲区用于存储连接字符串 */
    char dsn[1024];
    
    /* 格式化连接字符串，使用三元运算符处理空指针情况 */
    snprintf(dsn, sizeof(dsn), 
             "%s[+%s]://[%s:%s@]%s:%d/%s?%s",
             opts->driver ? opts->driver : "",          // 驱动类型
             opts->protocol ? opts->protocol : "",      // 协议类型
             opts->svr_username ? opts->svr_username : "", // 用户名
             opts->svr_password ? opts->svr_password : "", // 密码
             opts->svr_address ? opts->svr_address : "localhost", // 服务器地址
             opts->svr_port ? opts->svr_port : 6030,    // 服务器端口
             opts->svr_database ? opts->svr_database : ""); // 数据库名称
    
    /* 调用底层连接创建函数 */
    return create_tdengine_connection(dsn);
}

/*
 * 关闭与TDengine服务器的连接并清理连接缓存项
 * 
 * @param entry 连接缓存项指针，包含要关闭的连接信息
 * 
 * 功能说明：
 * 1. 检查连接项和连接对象是否有效
 * 2. 调用ws_close关闭底层连接
 * 3. 将连接指针置为NULL防止重复关闭
 * 4. 安全处理可能的空指针情况
 */
static void
tdengine_disconnect_server(ConnCacheEntry *entry)
{
    /* 检查连接项和连接对象是否有效 */
    if (entry && entry->conn != NULL)
    {
        /* 关闭底层TDengine连接 */
        ws_close(entry->conn);
        /* 清空连接指针防止重复关闭 */
        entry->conn = NULL;
    }
}

/*
 * 连接失效回调函数，用于处理服务器或用户映射变更时的连接清理
 * 
 * @param arg 回调数据(未使用)
 * @param cacheid 触发回调的系统缓存ID(FOREIGNSERVEROID/USERMAPPINGOID)
 * @param hashvalue 变更对象的哈希值(0表示所有对象)
 * 
 * 功能说明：
 * 1. 遍历连接缓存哈希表
 * 2. 检查每个连接项是否需要失效
 * 3. 匹配条件时标记连接为失效并关闭
 * 4. 记录调试日志
 * 
 * 触发条件：
 * - 服务器配置变更(FOREIGNSERVEROID)
 * - 用户映射变更(USERMAPPINGOID)
 * - 全局失效(hashvalue=0)
 */
static void
tdengine_inval_callback(Datum arg, int cacheid, uint32 hashvalue)
{
    HASH_SEQ_STATUS scan;
    ConnCacheEntry *entry;

    /* 确保cacheid是预期的类型 */
    Assert(cacheid == FOREIGNSERVEROID || cacheid == USERMAPPINGOID);

    /* 初始化哈希表遍历 */
    hash_seq_init(&scan, ConnectionHash);
    
    /* 遍历所有连接缓存项 */
    while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
    {
        /* 跳过空连接 */
        if (entry->conn == NULL)
            continue;

        /* 检查是否匹配失效条件 */
        if (hashvalue == 0 || /* 全局失效 */
            (cacheid == FOREIGNSERVEROID && entry->server_hashvalue == hashvalue) || /* 特定服务器失效 */
            (cacheid == USERMAPPINGOID && entry->mapping_hashvalue == hashvalue))   /* 特定用户映射失效 */
        {
            /* 标记连接为失效状态 */
            entry->invalidated = true;
            /* 记录调试日志 */
            elog(DEBUG3, "tdengine_fdw: discarding connection %p", entry->conn);
            /* 关闭连接 */
            tdengine_disconnect_server(entry);
        }
    }
}

/*
 * 清理所有TDengine服务器连接
 * 
 * 功能说明：
 * 1. 检查连接缓存哈希表是否已初始化
 * 2. 遍历哈希表中的所有连接项
 * 3. 关闭所有活跃的连接
 * 4. 安全处理空连接项
 * 
 * 使用场景：
 * - 模块卸载时
 * - 进程退出时
 * - 需要强制清理所有连接时
 */
void
tdengine_cleanup_connection(void)
{
    HASH_SEQ_STATUS scan;
    ConnCacheEntry *entry;

    /* 检查连接哈希表是否已初始化 */
    if (ConnectionHash == NULL)
        return;

    /* 初始化哈希表遍历 */
    hash_seq_init(&scan, ConnectionHash);
    
    /* 遍历所有连接缓存项 */
    while ((entry = (ConnCacheEntry *) hash_seq_search(&scan)))
    {
        /* 跳过空连接项 */
        if (entry->conn == NULL)
            continue;

        /* 关闭当前连接 */
        tdengine_disconnect_server(entry);
    }
}
