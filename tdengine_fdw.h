#include "query_cxx.h"

#include "foreign/foreign.h"
#include "lib/stringinfo.h"

#include "nodes/pathnodes.h"
#include "utils/float.h"
#include "optimizer/optimizer.h"
#include "access/table.h"
#include "fmgr.h"

#include "utils/rel.h"

/* 等待超时时间设置(毫秒)，0表示无限等待 */
#define WAIT_TIMEOUT 0
/* 交互式查询超时时间设置(毫秒)，0表示不超时 */
#define INTERACTIVE_TIMEOUT 0

/* TDengine兼容模式下的时间列名定义 */
#define TDENGINE_TIME_COLUMN "time"
/* TDengine兼容模式下的文本格式时间列名 */
#define TDENGINE_TIME_TEXT_COLUMN "time_text"
/* TDengine兼容模式下的标签列名 */
#define TDENGINE_TAGS_COLUMN "tags"
/* TDengine兼容模式下的字段列名 */
#define TDENGINE_FIELDS_COLUMN "fields"

/* TDengine标签列在PostgreSQL中的数据类型 */
#define TDENGINE_TAGS_PGTYPE "jsonb"
/* TDengine字段列在PostgreSQL中的数据类型 */
#define TDENGINE_FIELDS_PGTYPE "jsonb"

/* 判断列名是否为时间列(TDengine兼容模式) */
#define TDENGINE_IS_TIME_COLUMN(X) (strcmp(X, TDENGINE_TIME_COLUMN) == 0 || \
                                    strcmp(X, TDENGINE_TIME_TEXT_COLUMN) == 0)
/* 判断类型是否为时间类型 */
#define TDENGINE_IS_TIME_TYPE(typeoid) ((typeoid == TIMESTAMPTZOID) || \
                                        (typeoid == TIMEOID) ||        \
                                        (typeoid == TIMESTAMPOID))
/* 无错误返回码定义 */
#define CR_NO_ERROR 0

/*
 * 宏定义：用于检查目标列表中聚合函数和非聚合函数的混合情况
 *
 * 定义说明:
 *   TDENGINE_TARGETS_MARK_COLUMN - 标记目标列表中的普通列(非聚合) (二进制位0)
 *   TDENGINE_TARGETS_MARK_AGGREF - 标记目标列表中的聚合函数 (二进制位1)
 *   TDENGINE_TARGETS_MIXING_AGGREF_UNSAFE - 表示同时包含普通列和聚合函数的不安全混合状态
 *   TDENGINE_TARGETS_MIXING_AGGREF_SAFE - 表示安全状态(无混合或仅单一种类)
 *
 * 使用场景:
 *   在查询计划阶段检查目标列表是否可以安全下推到TDengine服务器执行
 *   当同时存在普通列和聚合函数时(TDENGINE_TARGETS_MIXING_AGGREF_UNSAFE)，
 *   表示这种混合状态不能安全下推，需要在PostgreSQL端处理
 */
#define TDENGINE_TARGETS_MARK_COLUMN (1u << 0)
#define TDENGINE_TARGETS_MARK_AGGREF (1u << 1)
#define TDENGINE_TARGETS_MIXING_AGGREF_UNSAFE (TDENGINE_TARGETS_MARK_COLUMN | TDENGINE_TARGETS_MARK_AGGREF)
#define TDENGINE_TARGETS_MIXING_AGGREF_SAFE (0u)

#define CODE_VERSION 20200

/*
 * 用于存储 TDengine 服务器信息的选项结构体
 * TODO: 支持超级表
 */
typedef struct tdengine_opt
{
    char *driver;       /* TDengine 驱动名，如 "taos" 或 "tmq" */
    char *protocol;     /* 连接协议，如 "taos+ws" 等 */
    char *svr_database; /* TDengine 数据库名称（可选） */
    char *svr_table;    /* TDengine 表名称（可选） */
    char *svr_address;  /* TDengine 服务器 IP 地址 */
    int svr_port;       /* TDengine 端口号 */
    char *svr_username; /* TDengine 用户名 */
    char *svr_password; /* TDengine 密码 */
    List *tags_list;    /* 外部表的标签键（若有其他业务需求保留，DSN 中无直接对应） */
    int schemaless;     /* 无模式模式（若有其他业务需求保留，DSN 中无直接对应） */
} tdengine_opt;

typedef struct schemaless_info
{
    bool schemaless;    /* 启用无模式 */
    Oid slcol_type_oid; /* 无模式列的 jsonb 类型的对象标识符（OID） */
    Oid jsonb_op_oid;   /* jsonb 类型 "->>" 箭头操作符的对象标识符（OID） */

    Oid relid; /* 关系的对象标识符（OID） */
} schemaless_info;

/*
 * 用于 ForeignScanState 中 fdw_state 的特定于 FDW 的信息
 */
typedef struct TDengineFdwExecState
{
    char *query;           /* 查询字符串 */
    Relation rel;          /* 外部表的关系缓存条目 */
    Oid relid;             /* 关系的对象标识符（OID） */
    UserMapping *user;     /* 外部服务器的用户映射 */
    List *retrieved_attrs; /* 目标属性编号的列表 */

    char **params;
    bool cursor_exists;                    /* 是否已经创建了游标？ */
    int numParams;                         /* 传递给查询的参数数量 */
    FmgrInfo *param_flinfo;                /* 参数的输出转换函数 */
    List *param_exprs;                     /* 参数值的可执行表达式 */
    const char **param_values;             /* 查询参数的文本值 */
    Oid *param_types;                      /* 查询参数的类型 */
    TDengineType *param_tdengine_types;    /* 查询参数的 TDengine 类型 */
    TDengineValue *param_tdengine_values;  /* 用于 TDengine 的参数值 */
    TDengineColumnInfo *param_column_info; /* 列的信息 */
    int p_nums;                            /* 要传输的参数数量 */
    FmgrInfo *p_flinfo;                    /* 参数的输出转换函数 */

    tdengine_opt *tdengineFdwOptions; /* TDengine FDW 选项 */

    int batch_size;    /* FDW 选项 "batch_size" 的值 */
    List *attr_list;   /* 查询属性列表 */
    List *column_list; /* TDengine 列结构的列列表 */

    int64 row_nums;     /* 行数 */
    Datum **rows;       /* 扫描的所有行 */
    int64 rowidx;       /* 当前行的索引 */
    bool **rows_isnull; /* 值是否为空 */
    bool for_update;    /* 如果此扫描是更新目标，则为 true */
    bool is_agg;        /* 扫描是否为聚合操作 */
    List *tlist;        /* 目标列表 */

    /* 工作内存上下文 */
    MemoryContext temp_cxt; /* 用于每行临时数据的上下文 */
    AttrNumber *junk_idx;

    /* 如果是子计划结果关系，用于更新行移动 */
    struct TDengineFdwExecState *aux_fmstate; /* 如果已创建，为外部插入状态 */

    /* 目标列表中的函数下推支持 */
    bool is_tlist_func_pushdown;

    /* 无模式信息 */
    schemaless_info slinfo;

    void *temp_result;
} TDengineFdwExecState;

typedef struct TDengineFdwRelationInfo
{
    /*
     * 为 true 表示该关系可以下推。对于简单的外部扫描，此值始终为 true。
     */
    bool pushdown_safe;

    /* baserestrictinfo 子句，分为安全和不安全的子集。 */
    List *remote_conds;
    List *local_conds;

    /* 实际用于扫描的远程约束子句（不包含 RestrictInfos） */
    List *final_remote_exprs;

    /* 我们需要从远程服务器获取的属性编号的位图。 */
    Bitmapset *attrs_used;

    /* 为 true 表示 query_pathkeys 可以安全下推 */
    bool qp_is_pushdown_safe;

    /* 本地条件的成本和选择性。 */
    QualCost local_conds_cost;
    Selectivity local_conds_sel;

    /* 连接条件的选择性 */
    Selectivity joinclause_sel;

    /* 扫描或连接的估计大小和成本。 */
    double rows;
    int width;
    Cost startup_cost;
    Cost total_cost;

    /* 不包含从远程服务器传输数据成本的成本 */
    double retrieved_rows;
    Cost rel_startup_cost;
    Cost rel_total_cost;

    /* 从目录中提取的选项。 */
    bool use_remote_estimate;
    Cost fdw_startup_cost;
    Cost fdw_tuple_cost;
    List *shippable_extensions; /* 白名单扩展的 OID */

    /* 缓存的目录信息。 */
    ForeignTable *table;
    ForeignServer *server;
    UserMapping *user; /* 仅在使用远程估计模式下设置 */

    int fetch_size; /* 此远程表的获取大小 */

    /*
     * 关系的名称，用于在 EXPLAIN ForeignScan 时使用。它用于连接和上层关系，但会为所有关系设置。
     * 对于基础关系，这实际上只是 RT 索引的字符串表示；我们在生成 EXPLAIN 输出时会进行转换。
     * 对于连接和上层关系，该名称表示包含哪些基础外部表以及使用的连接类型或聚合类型。
     */
    char *relation_name;

    /* 连接信息 */
    RelOptInfo *outerrel;
    RelOptInfo *innerrel;
    JoinType jointype;
    /* joinclauses 仅包含外部连接的 JOIN/ON 条件 */
    List *joinclauses; /* RestrictInfo 列表 */

    /* 上层关系信息 */
    UpperRelationKind stage;

    /* 分组信息 */
    List *grouped_tlist;

    /* 子查询信息 */
    bool make_outerrel_subquery; /* 我们是否将外部关系解析为子查询？ */
    bool make_innerrel_subquery; /* 我们是否将内部关系解析为子查询？ */
    Relids lower_subquery_rels;  /* 所有出现在下层子查询中的关系 ID */

    /*
     * 关系的索引。它用于为表示该关系的子查询创建别名。
     */
    int relation_index;

    /* 目标列表中的函数下推支持 */
    bool is_tlist_func_pushdown;

    /* 为 true 表示目标列表中除了时间列之外的所有列 */
    bool all_fieldtag;
    /* 无模式信息 */
    schemaless_info slinfo;
    /* JsonB 列列表 */
    List *slcols;
} TDengineFdwRelationInfo;

extern bool tdengine_is_foreign_expr(PlannerInfo *root,
                                     RelOptInfo *baserel,
                                     Expr *expr,
                                     bool for_tlist);

extern bool tdengine_is_foreign_function_tlist(PlannerInfo *root,
                                               RelOptInfo *baserel,
                                               List *tlist);

/* option.c headers */

extern tdengine_opt *tdengine_get_options(Oid foreigntableid, Oid userid);
extern void tdengine_deparse_insert(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, List *targetAttrs);
extern void tdengine_deparse_update(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, List *targetAttrs, List *attname);
extern void tdengine_deparse_delete(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel, List *attname);
extern bool tdengine_deparse_direct_delete_sql(StringInfo buf, PlannerInfo *root,
                                               Index rtindex, Relation rel,
                                               RelOptInfo *foreignrel,
                                               List *remote_conds,
                                               List **params_list,
                                               List **retrieved_attrs);
extern void tdengine_deparse_drop_measurement_stmt(StringInfo buf, Relation rel);

/* deparse.c headers */

extern void tdengine_deparse_select_stmt_for_rel(StringInfo buf, PlannerInfo *root, RelOptInfo *rel,
                                                 List *tlist, List *remote_conds, List *pathkeys,
                                                 bool is_subquery, List **retrieved_attrs,
                                                 List **params_list, bool has_limit);
extern void tdengine_deparse_analyze(StringInfo buf, char *dbname, char *relname);
extern void tdengine_deparse_string_literal(StringInfo buf, const char *val);
extern List *tdengine_build_tlist_to_deparse(RelOptInfo *foreignrel);
extern int tdengine_set_transmission_modes(void);
extern void tdengine_reset_transmission_modes(int nestlevel);
extern bool tdengine_is_select_all(RangeTblEntry *rte, List *tlist, schemaless_info *pslinfo);
extern List *tdengine_pull_func_clause(Node *node);
extern List *tdengine_build_tlist_to_deparse(RelOptInfo *foreignrel);

extern char *tdengine_get_data_type_name(Oid data_type_id);
extern char *tdengine_get_column_name(Oid relid, int attnum);
extern char *tdengine_get_table_name(Relation rel);

extern bool tdengine_is_tag_key(const char *colname, Oid reloid);

/* slvars.c headers */

extern List *tdengine_pull_slvars(Expr *expr, Index varno, List *columns,
                                  bool extract_raw, List *remote_exprs, schemaless_info *pslinfo);
extern void tdengine_get_schemaless_info(schemaless_info *pslinfo, bool schemaless, Oid reloid);
extern char *tdengine_get_slvar(Expr *node, schemaless_info *slinfo);
extern bool tdengine_is_slvar(Oid oid, int attnum, schemaless_info *pslinfo, bool *is_tags, bool *is_fields);
extern bool tdengine_is_slvar_fetch(Node *node, schemaless_info *pslinfo);
extern bool tdengine_is_param_fetch(Node *node, schemaless_info *pslinfo);

/* tdengine_query.c headers */
extern Datum tdengine_convert_to_pg(Oid pgtyp, int pgtypmod, char *value);
extern Datum tdengine_convert_record_to_datum(Oid pgtyp, int pgtypmod, char **row, int attnum, int ntags, int nfield,
											  char **column, char *opername, Oid relid, int ncol, bool is_schemaless);

extern void tdengine_bind_sql_var(Oid type, int attnum, Datum value, TDengineColumnInfo *param_column_info,
								  TDengineType * param_tdengine_types, TDengineValue * param_tdengine_values);

/* query.cpp headers */                        
/* 获取表结构信息，成功返回表信息结构体 */
extern struct TDengineSchemaInfo_return TDengineSchemaInfo(UserMapping *user, tdengine_opt *opts);
/* 释放表结构信息内存 */
extern void TDengineFreeSchemaInfo(struct TableInfo* tableInfo, long long length);
/* 执行查询并返回结果集 */
extern struct TDengineQuery_return TDengineQuery(char *query, UserMapping *user, tdengine_opt *opts, TDengineType* ctypes, TDengineValue* cvalues, int cparamNum);
/* 释放查询结果内存 */
extern void TDengineFreeResult(TDengineResult* result);
/* 执行数据插入操作，成功返回NULL */
extern char* TDengineInsert(char *table_name, UserMapping *user, tdengine_opt *opts, struct TDengineColumnInfo* ccolumns, TDengineType* ctypes, TDengineValue* cvalues, int cparamNum, int cnumSlots);
/* 检查可连接的TDengine版本信息 */
extern int check_connected_tdengine_version(char* addr, int port, char* user, char* pass, char* db, char* auth_token, char* retention_policy);
/* 清理所有客户端缓存连接 */
extern void cleanup_cxx_client_connection(void);
