#ifndef QUERY_CXX_H
#define QUERY_CXX_H

#include "postgres.h"
#include "tdengine_fdw.h"

/* 表示数据类型的信息 */
typedef enum TDengineType
{
    TDENGINE_INT64,   // 64位整数类型
    TDENGINE_DOUBLE,  // 双精度浮点数类型
    TDENGINE_BOOLEAN, // 布尔类型
    TDENGINE_STRING,  // 字符串类型
    TDENGINE_TIME,    // 时间类型
    TDENGINE_NULL,    // 空值类型
} TDengineType;

/* 表示 TDengine 中的值的信息 */
typedef union TDengineValue
{
    long long int i; // 64位整数值
    double d;        // 双精度浮点数值
    int b;           // 布尔值（用整数表示）
    char *s;         // 字符串值
} TDengineValue;

/* 表示一个度量（表）的模式信息 */
typedef struct TableInfo
{
    char *measurement; /* 度量（表）的名称 */
    char **tag;        /* 标签名数组 */
    char **field;      /* 字段名数组 */
    char **field_type; /* 字段类型数组 */
    int tag_len;       /* 标签的数量 */
    int field_len;     /* 字段的数量 */
} TableInfo;

/* 表示一行数据的信息 */
typedef struct TDengineRow
{
    char **tuple; // 元组数据数组
} TDengineRow;

/* 表示 TDengine 查询结果集的信息 */
typedef struct TDengineResult
{
    TDengineRow *rows; // 行数据数组
    int ncol;          // 列的数量
    int nrow;          // 行的数量
    char **columns;    // 列名数组
    char **tagkeys;    // 标签键数组
    int ntag;          // 标签的数量
} TDengineResult;

/* 表示表的列类型信息 */
typedef enum TDengineColumnType
{
    TDENGINE_UNKNOWN_KEY, // 未知类型的列
    TDENGINE_TIME_KEY,    // 时间类型的列
    TDENGINE_TAG_KEY,     // 标签类型的列
    TDENGINE_FIELD_KEY,   // 字段类型的列
} TDengineColumnType;

/* 表示表的列信息 */
typedef struct TDengineColumnInfo
{
    char *column_name;              /* 列名 */
    TDengineColumnType column_type; /* 列的类型 */
} TDengineColumnInfo;

/* TDengineSchemaInfo 函数的返回类型 */
struct TDengineSchemaInfo_return
{
    struct TableInfo *r0; /* 返回的表信息 */
    long long r1;         /* 长度 */
    char *r2;             /* 错误信息 */
};

/* TDengineQuery 函数的返回类型 */
struct TDengineQuery_return
{
    TDengineResult *r0; // 查询结果集
    char *r1;           // 错误信息
};

/* 执行 TDengine 的 DDL 命令。
   参数依次为：地址、端口、用户名、密码、数据库名、DDL 查询语句、版本、认证令牌、保留策略
   返回值为错误信息字符串，如果执行成功则可能返回空指针。
*/
extern char *TDengineExecDDLCommand(char *addr,
                                    int port,
                                    char *user,
                                    char *pass,
                                    char *db,
                                    char *cquery,
                                    int version,
                                    char *auth_token,
                                    char *retention_policy);

#endif /* QUERY_CXX_H */