// #ifdef GO_CLIENT
// #include "_obj/_cgo_export.h"
// #else
// #ifdef CXX_CLIENT
#include "query_cxx.h"
// #endif
// #endif

#include "foreign/foreign.h"
#include "lib/stringinfo.h"

// #if (PG_VERSION_NUM >= 120000)
#include "nodes/pathnodes.h"
#include "utils/float.h"
#include "optimizer/optimizer.h"
#include "access/table.h"
#include "fmgr.h"
// #else
// #include "nodes/relation.h"
// #include "optimizer/var.h"
// #endif

#include "utils/rel.h"

#define CODE_VERSION 20200

/*
 * 用于存储 TDengine 服务器信息的选项结构体
 */
typedef struct tdengine_opt {
    char    *driver;        /* TDengine 驱动名，如 "taos" 或 "tmq" */
    char    *protocol;      /* 连接协议，如 "taos+ws" 等 */
    char    *svr_database;  /* TDengine 数据库名称（可选） */
    char    *svr_address;   /* TDengine 服务器 IP 地址 */
    int      svr_port;      /* TDengine 端口号 */
    char    *svr_username;  /* TDengine 用户名 */
    char    *svr_password;  /* TDengine 密码 */  
    List    *tags_list;     /* 外部表的标签键（若有其他业务需求保留，DSN 中无直接对应） */
    int      schemaless;    /* 无模式模式（若有其他业务需求保留，DSN 中无直接对应） */
} tdengine_opt;

typedef struct schemaless_info
{
    bool		schemaless;		/* 启用无模式检查 */
    Oid			slcol_type_oid; /* 无模式列的 jsonb 类型的对象标识符（OID） */
    Oid			jsonb_op_oid;	/* jsonb 类型 "->>" 箭头操作符的对象标识符（OID） */

    Oid			relid;			/* 关系的对象标识符（OID） */
}			schemaless_info;


/*
 * 用于 ForeignScanState 中 fdw_state 的特定于 FDW 的信息
 */
typedef struct TDengineFdwExecState
{
    char	   *query;			/* 查询字符串 */
    Relation	rel;			/* 外部表的关系缓存条目 */
    Oid			relid;			/* 关系的对象标识符（OID） */
    UserMapping *user;			/* 外部服务器的用户映射 */
    List	   *retrieved_attrs;	/* 目标属性编号的列表 */

    char	  **params;
    bool		cursor_exists;	/* 是否已经创建了游标？ */
    int			numParams;		/* 传递给查询的参数数量 */
    FmgrInfo   *param_flinfo;	/* 参数的输出转换函数 */
    List	   *param_exprs;	/* 参数值的可执行表达式 */
    const char **param_values;	/* 查询参数的文本值 */
    Oid		   *param_types;	/* 查询参数的类型 */
    TDengineType *param_tdengine_types; /* 查询参数的 TDengine 类型 */
    TDengineValue *param_tdengine_values;	/* 用于 TDengine 的参数值 */
    TDengineColumnInfo *param_column_info;	/* 列的信息 */
    int			p_nums;			/* 要传输的参数数量 */
    FmgrInfo   *p_flinfo;		/* 参数的输出转换函数 */

    tdengine_opt *tdengineFdwOptions;	/* TDengine FDW 选项 */

    int			batch_size;		/* FDW 选项 "batch_size" 的值 */
    List	   *attr_list;		/* 查询属性列表 */
    List	   *column_list;	/* TDengine 列结构的列列表 */

    int64		row_nums;		/* 行数 */
    Datum	  **rows;			/* 扫描的所有行 */
    int64		rowidx;			/* 当前行的索引 */
    bool	  **rows_isnull;	/* 值是否为空 */
    bool		for_update;		/* 如果此扫描是更新目标，则为 true */
    bool		is_agg;			/* 扫描是否为聚合操作 */
    List	   *tlist;			/* 目标列表 */

    /* 工作内存上下文 */
    MemoryContext temp_cxt;		/* 用于每行临时数据的上下文 */
    AttrNumber *junk_idx;

    /* 如果是子计划结果关系，用于更新行移动 */
    struct TDengineFdwExecState *aux_fmstate;	/* 如果已创建，为外部插入状态 */

    /* 目标列表中的函数下推支持 */
    bool		is_tlist_func_pushdown;

    /* 无模式信息 */
    schemaless_info slinfo;

    void	   *temp_result;
}			TDengineFdwExecState;


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