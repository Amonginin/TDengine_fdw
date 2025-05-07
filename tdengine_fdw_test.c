#include "postgres.h"

#include "tdengine_fdw.h"

#include <stdio.h>

#include "access/reloptions.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "optimizer/appendinfo.h"

#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/cost.h"
#include "optimizer/clauses.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/paths.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "utils/array.h"
#include "utils/date.h"
#include "utils/hsearch.h"
#include "utils/timestamp.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_aggregate.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "utils/typcache.h"
#include "utils/selfuncs.h"
#include "utils/syscache.h"

/* If no remote estimates, assume a sort costs 20% extra */
#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

extern PGDLLEXPORT void _PG_init(void);

static void tdengine_fdw_exit(int code, Datum arg);

extern Datum tdengine_fdw_handler(PG_FUNCTION_ARGS);
extern Datum tdengine_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(tdengine_fdw_handler);
PG_FUNCTION_INFO_V1(tdengine_fdw_version);

// 用于估计外部表的大小和成本，为查询规划器提供必要的信息。
static void tdengineGetForeignRelSize(PlannerInfo *root,
                                      RelOptInfo *baserel,
                                      Oid foreigntableid);
// 生成访问外部表的不同路径，帮助规划器选择最优查询路径。
static void tdengineGetForeignPaths(PlannerInfo *root,
                                    RelOptInfo *baserel,
                                    Oid foreigntableid);
// 根据选择的最佳路径生成外部扫描计划。
static ForeignScan *tdengineGetForeignPlan(PlannerInfo *root,
                                           RelOptInfo *baserel,
                                           Oid foreigntableid,
                                           ForeignPath *best_path,
                                           List *tlist,
                                           List *scan_clauses,
                                           Plan *outer_plan);
// 获取执行ForeignScan算子所需的信息，并将它们组织并保存在ForeignScanState中
static void tdengineBeginForeignScan(ForeignScanState *node,
                                     int eflags);
// 读取外部数据源的一行数据，并将它组织为PG中的Tuple
static TupleTableSlot *tdengineIterateForeignScan(ForeignScanState *node);
// 将外部数据源的读取位置重置回最初的起始位置
static void tdengineReScanForeignScan(ForeignScanState *node);
// 释放整个ForeignScan算子执行过程中占用的外部资源或FDW中的资源
static void tdengineEndForeignScan(ForeignScanState *node);

static void tdengine_to_pg_type(StringInfo str, char *typname);

static void prepare_query_params(PlanState *node,
                                 List *fdw_exprs,
                                 List *remote_exprs,
                                 Oid foreigntableid,
                                 int numParams,
                                 FmgrInfo **param_flinfo,
                                 List **param_exprs,
                                 const char ***param_values,
                                 Oid **param_types,
                                 TDengineType **param_tdengine_types,
                                 TDengineValue **param_tdengine_values,
                                 TDengineColumnInfo **param_column_info);

static void process_query_params(ExprContext *econtext,
                                 FmgrInfo *param_flinfo,
                                 List *param_exprs,
                                 const char **param_values,
                                 Oid *param_types,
                                 TDengineType *param_tdengine_types,
                                 TDengineValue *param_tdengine_values,
                                 TDengineColumnInfo *param_column_info);

static void create_cursor(ForeignScanState *node);
static void execute_dml_stmt(ForeignScanState *node);
static TupleTableSlot **execute_foreign_insert_modify(EState *estate,
                                                      ResultRelInfo *resultRelInfo,
                                                      TupleTableSlot **slots,
                                                      TupleTableSlot **planSlots,
                                                      int numSlots);
static int tdengine_get_batch_size_option(Relation rel);

/*
 * 此枚举描述了 ForeignPath 的 fdw_private 列表中存储的内容。
 * 存储以下信息：
 *
 * 1) 一个布尔标志，表明远程查询是否进行了最终排序
 * 2) 一个布尔标志，表明远程查询是否包含 LIMIT 子句
 */
enum FdwPathPrivateIndex
{
    /* 有最终排序标志（以布尔节点形式） */
    FdwPathPrivateHasFinalSort,
    /* 有 LIMIT 子句标志（以布尔节点形式） */
    FdwPathPrivateHasLimit,
};

/*
 * Similarly, this enum describes what's kept in the fdw_private list for
 * a ModifyTable node referencing a tdengine_fdw foreign table.  We store:
 *
 * 1) DELETE statement text to be sent to the remote server
 * 2) Integer list of target attribute numbers for INSERT (NIL for a DELETE)
 */
enum FdwModifyPrivateIndex
{
    /* SQL statement to execute remotely (as a String node) */
    FdwModifyPrivateUpdateSql,
    /* Integer list of target attribute numbers */
    FdwModifyPrivateTargetAttnums,
};

enum FdwDirectModifyPrivateIndex
{
    /* SQL statement to execute remotely (as a String node) */
    FdwDirectModifyPrivateUpdateSql,
    /* has-returning flag (as an Boolean node) */
    FdwDirectModifyPrivateHasReturning,
    /* Integer list of attribute numbers retrieved by RETURNING */
    FdwDirectModifyPrivateRetrievedAttrs,
    /* set-processed flag (as an Boolean node) */
    FdwDirectModifyPrivateSetProcessed,
    /* remote conditions */
    FdwDirectModifyRemoteExprs
};

/*
 * Execution state of a foreign scan that modifies a foreign table directly.
 */
typedef struct TDengineFdwDirectModifyState
{
    Relation rel;             /* relcache entry for the foreign table */
    UserMapping *user;        /* user mapping of foreign server */
    AttInMetadata *attinmeta; /* attribute datatype conversion metadata */

    /* extracted fdw_private data */
    char *query;           /* text of UPDATE/DELETE command */
    bool has_returning;    /* is there a RETURNING clause? */
    List *retrieved_attrs; /* attr numbers retrieved by RETURNING */
    bool set_processed;    /* do we set the command es_processed? */

    /* for remote query execution */
    char **params;
    int numParams;                         /* number of parameters passed to query */
    FmgrInfo *param_flinfo;                /* output conversion functions for them */
    List *param_exprs;                     /* executable expressions for param values */
    const char **param_values;             /* textual values of query parameters */
    Oid *param_types;                      /* type of query parameters */
    TDengineType *param_tdengine_types;    /* TDengine type of query parameters */
    TDengineValue *param_tdengine_values;  /* values for TDengine */
    TDengineColumnInfo *param_column_info; /* information of columns */

    tdengine_opt *tdengineFdwOptions; /* TDengine FDW options */

    /* for storing result tuples */
    int num_tuples;       /* # of result tuples */
    int next_tuple;       /* index of next one to return */
    Relation resultRel;   /* relcache entry for the target relation */
    AttrNumber *attnoMap; /* array of attnums of input user columns */
    AttrNumber ctidAttno; /* attnum of input ctid column */
    AttrNumber oidAttno;  /* attnum of input oid column */
    bool hasSystemCols;   /* are there system columns of resultRel? */

    /* working memory context */
    MemoryContext temp_cxt; /* context for per-tuple temporary data */
} TDengineFdwDirectModifyState;

/*
 * PostgreSQL扩展初始化函数
 * 1. 在PostgreSQL加载扩展时自动调用
 * 2. 注册进程退出回调函数，确保资源正确释放
 * 3. 使用on_proc_exit机制保证无论进程如何退出都会执行清理
 *
 * 无显式参数，遵循PostgreSQL扩展初始化函数标准格式
 *
 * 回调机制：
 * - 注册tdengine_fdw_exit作为退出处理函数
 * - 传递NULL作为回调参数
 *
 * 注意事项：
 * 1. 必须在扩展加载时调用
 * 2. 是PostgreSQL扩展的标准入口点
 */
void _PG_init(void)
{
    /* 注册进程退出回调函数 */
    on_proc_exit(&tdengine_fdw_exit, PointerGetDatum(NULL));
}

/*
 * TDengine FDW 退出回调函数
 * 1. 在PostgreSQL进程退出时被调用
 * 2. 负责清理TDengine客户端连接资源
 * 3. 确保不会出现资源泄漏
 *
 * @code 退出状态码
 * @arg 回调参数(未使用)
 */
static void
tdengine_fdw_exit(int code, Datum arg)
{
    /* 清理TDengine C++客户端连接 */
    cleanup_cxx_client_connection();
}

Datum tdengine_fdw_version(PG_FUNCTION_ARGS)
{
    PG_RETURN_INT32(CODE_VERSION);
}

/**
 * =====================注册回调函数======================
 */
Datum tdengine_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    fdwroutine->GetForeignRelSize = tdengineGetForeignRelSize;
    fdwroutine->GetForeignPaths = tdengineGetForeignPaths;
    fdwroutine->GetForeignPlan = tdengineGetForeignPlan;

    fdwroutine->BeginForeignScan = tdengineBeginForeignScan;
    fdwroutine->IterateForeignScan = tdengineIterateForeignScan;
    fdwroutine->ReScanForeignScan = tdengineReScanForeignScan;
    fdwroutine->EndForeignScan = tdengineEndForeignScan;

    PG_RETURN_POINTER(fdwroutine);
}

//========================= GetForeignRelSize ===========================
/*
 * 获取给定外部关系的外部扫描的成本和大小估计
 * 基础关系或外部关系之间的连接。
 *
 * param_join_conds：与外部关系的参数化子句。
 * pathkeys：指定给定路径的预期排序顺序（如果有的话）。
 *
 * 返回p_row、p_width、p_startup_cost和p_total_cost变量。
 */
static void
estimate_path_cost_size(PlannerInfo *root,
                        RelOptInfo *foreignrel,
                        List *param_join_conds,
                        List *pathkeys,
                        double *p_rows, int *p_width,
                        Cost *p_startup_cost, Cost *p_total_cost)
{
    TDengineFdwRelationInfo *fpinfo = (TDengineFdwRelationInfo *)foreignrel->fdw_private;
    double rows;
    double retrieved_rows;
    int width;
    Cost startup_cost;
    Cost total_cost;
    Cost cpu_per_tuple;

    /*
     * 如果表或服务器配置为使用远程估计，
     * 连接到外部服务器并执行EXPLAIN以估计限制+连接子句选择的行数。
     * 否则，使用我们在本地拥有的任何统计信息来估计行，方式类似于普通表。
     */
    if (fpinfo->use_remote_estimate)
    {
        // ereport 是 PostgreSQL 中的一个宏，用于报告错误、警告和其他消息
        // 定义位于 PostgreSQL 的源代码中，具体文件是 src/include/utils/elog.h
        ereport(ERROR, (errmsg("Remote estimation is unsupported")));
    }
    else
    {
        Cost run_cost = 0;
        /*
         * 我们不支持这种模式下的连接条件（因此，不能创建参数化路径）。
         */
        Assert(param_join_conds == NIL);
        /*
         * 对基本对外关系使用set_baserel_size_estimates（）进行的行/宽度估计，
         * 对外关系之间的连接使用set_joinrel_size_estimates（）进行的行/宽度估计。
         */
        rows = foreignrel->rows;
        width = foreignrel->reltarget->width;

        /* 计算检索的行数. */
        // clamp_row_est 是 PostgreSQL 源码中的一个函数，
        // 定义在 src/include/optimizer/pathnode.h 文件里。
        retrieved_rows = clamp_row_est(rows / fpinfo->local_conds_sel);

        /*
         * 如果已经缓存了成本，则直接使用；
         */
        if (fpinfo->rel_startup_cost > 0 && fpinfo->rel_total_cost > 0)
        {
            startup_cost = fpinfo->rel_startup_cost;
            run_cost = fpinfo->rel_total_cost - fpinfo->rel_startup_cost;
        }
        /* 否则，将其视为顺序扫描来计算成本*/
        else
        {
            Assert(foreignrel->reloptkind != RELOPT_JOINREL);
            /* 将检索到的行估计限制为最小(检索行，外部关系->元组). */
            retrieved_rows = Min(retrieved_rows, foreignrel->tuples);

            // 初始化启动成本为 0
            startup_cost = 0;
            // 初始化运行成本为 0
            run_cost = 0;
            // 计算顺序扫描页面的成本，即顺序扫描页面成本乘以页面数量
            run_cost += seq_page_cost * foreignrel->pages;

            // 将基础关系的限制条件的启动成本加到总启动成本中
            startup_cost += foreignrel->baserestrictcost.startup;
            // 计算每行的 CPU 成本，包括基础的元组 CPU 成本和基础关系限制条件的每行成本
            cpu_per_tuple =
                cpu_tuple_cost + foreignrel->baserestrictcost.per_tuple;
            // 计算处理所有元组的 CPU 成本，并加到运行成本中
            run_cost += cpu_per_tuple * foreignrel->tuples;
        }

        /*
         * 当没有远程估计时，我们实际上没有办法准确估计生成排序输出的成本。
         * 这是因为如果远程端选择的查询计划本身就会生成有序的输出，那么排序成本可能为零。
         * 但在大多数情况下，排序操作会产生一定的成本。
         * 因此，我们需要估计一个成本值，这个值应该足够高，以确保在排序顺序在本地没有用处时，我们不会选择排序路径。
         * 同时，这个值又要足够低，以便在排序操作对本地有帮助时，我们会倾向于将 ORDER BY 子句下推到远程服务器。
         */
        if (pathkeys != NIL)
        {
            /*
             * 如果存在路径键（pathkeys），意味着查询需要对结果进行排序。
             * 此时，将启动成本（startup_cost）乘以默认的排序成本乘数（DEFAULT_FDW_SORT_MULTIPLIER）。
             * 这样可以增加启动成本，以反映排序操作可能带来的额外开销。
             */
            startup_cost *= DEFAULT_FDW_SORT_MULTIPLIER;

            /*
             * 同样地，将运行成本（run_cost）乘以默认的排序成本乘数（DEFAULT_FDW_SORT_MULTIPLIER）。
             * 这是为了考虑到排序操作在查询执行过程中可能产生的额外开销。
             */
            run_cost *= DEFAULT_FDW_SORT_MULTIPLIER;
        }

        /*
         * 计算总成本（total_cost），即启动成本和运行成本之和。
         * 在前面的代码中，如果存在排序操作，启动成本和运行成本已经被相应地调整。
         * 这里将调整后的启动成本和运行成本相加，得到最终的总成本。
         */
        total_cost = startup_cost + run_cost;
    }
    // 检查当前扫描是否没有指定排序键（pathkeys）且没有与外部关系的参数化连接条件（param_join_conds）
    // 若满足条件，则进行成本缓存操作
    if (pathkeys == NIL && param_join_conds == NIL)
    {
        // 将当前计算得到的启动成本（startup_cost）赋值给 TDengineFdwRelationInfo 结构体中的 rel_startup_cost 成员
        // 用于后续成本计算的缓存
        fpinfo->rel_startup_cost = startup_cost;
        // 将当前计算得到的总成本（total_cost）赋值给 TDengineFdwRelationInfo 结构体中的 rel_total_cost 成员
        // 用于后续成本计算的缓存
        fpinfo->rel_total_cost = total_cost;
    }
    /*
     * 额外开销
     */
    startup_cost += fpinfo->fdw_startup_cost;
    total_cost += fpinfo->fdw_startup_cost;
    total_cost += fpinfo->fdw_tuple_cost * retrieved_rows;
    total_cost += cpu_tuple_cost * retrieved_rows;
    /* 返回值. */
    *p_rows = rows;
    *p_width = width;
    *p_startup_cost = startup_cost;
    *p_total_cost = total_cost;
}

/*
 * 该函数的作用是提取实际从远程 TDengine 服务器获取的列信息。
 */
static void
tdengine_extract_slcols(TDengineFdwRelationInfo *fpinfo, PlannerInfo *root, RelOptInfo *baserel, List *tlist)
{
    // 存储范围表项信息
    RangeTblEntry *rte;
    // 输入目标列表，若 tlist 不为空则使用 tlist，否则使用 baserel 的目标表达式列表
    List *input_tlist = (tlist) ? tlist : baserel->reltarget->exprs;
    // 指向 ListCell 的指针，用于遍历列表
    ListCell *lc = NULL;

    // 是否为无模式（schemaless），不是则直接返回，
    if (!fpinfo->slinfo.schemaless)
        return;

    // PG 的 planner_rt_fetch 函数根据 baserel 的 relid 从 root 中获取对应的范围表项
    rte = planner_rt_fetch(baserel->relid, root);
    // tdengine_is_select_all 函数判断是否为全列选择，并将结果存储在 fpinfo 的 all_fieldtag 成员中
    fpinfo->all_fieldtag = tdengine_is_select_all(rte, input_tlist, &fpinfo->slinfo);

    /* All actual column is required to be selected */
    // 如果是全列选择，则直接返回，无需进一步提取列信息
    if (fpinfo->all_fieldtag)
        return;

    /* Extract jsonb schemaless variable names from tlist */
    // 初始化 fpinfo 的 slcols 成员为空列表
    fpinfo->slcols = NIL;
    // 调用 tdengine_pull_slvars 函数从输入目标列表中提取 jsonb 无模式变量名，并更新 fpinfo 的 slcols 成员
    fpinfo->slcols = tdengine_pull_slvars((Expr *)input_tlist, baserel->relid,
                                          fpinfo->slcols, false, NULL, &(fpinfo->slinfo));

    /* Append josnb schemaless variable names from local_conds. */
    // 遍历 fpinfo 的 local_conds 列表，其中存储了本地条件信息
    foreach (lc, fpinfo->local_conds)
    {
        // 从列表中取出当前的 RestrictInfo 结构体
        // lfirst 是一个宏定义，用于获取列表（List 类型）中的第一个元素。
        // 这个宏定义在 postgres.h 文件中
        RestrictInfo *ri = (RestrictInfo *)lfirst(lc);

        // 调用 tdengine_pull_slvars 函数从当前本地条件的子句中提取 jsonb 无模式变量名，并追加到 fpinfo 的 slcols 成员中
        fpinfo->slcols = tdengine_pull_slvars(ri->clause, baserel->relid,
                                              fpinfo->slcols, false, NULL, &(fpinfo->slinfo));
    }
}

/*
 * tdengineGetForeignRelSize: 为外部表的扫描创建一个 FdwPlan
 */
static void
tdengineGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    // 用于存储外部表的相关信息，包括连接选项、条件子句等
    TDengineFdwRelationInfo *fpinfo;
    // 存储TDengine连接选项，如服务器地址、端口、认证信息等
    tdengine_opt *options;
    // 用于遍历列表的迭代器
    ListCell *lc;
    // 用于存储执行查询的用户ID
    Oid userid;

    // 从规划器的范围表中获取当前外部表的范围表条目(RTE)
    RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);

    // 输出调试信息，显示当前正在执行的函数名
    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    // 为TDengineFdwRelationInfo结构体分配内存并初始化为0
    fpinfo = (TDengineFdwRelationInfo *)palloc0(sizeof(TDengineFdwRelationInfo));
    // 将分配的结构体指针存储在baserel的fdw_private字段中，供后续使用
    baserel->fdw_private = (void *)fpinfo;

    // 确定执行用户ID：如果RTE指定了检查用户则使用该用户，否则使用当前用户
    userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

    // 从外部表OID和用户ID获取TDengine连接选项
    options = tdengine_get_options(foreigntableid, userid);

    // 获取外部表的无模式(schemaless)信息，存储在fpinfo的slinfo字段中
    // 无模式表不需要预定义严格的表结构
    tdengine_get_schemaless_info(&(fpinfo->slinfo), options->schemaless, foreigntableid);

    // 标记基础外部表总是支持查询下推
    fpinfo->pushdown_safe = true;

    // 从系统目录中获取外部表定义信息
    fpinfo->table = GetForeignTable(foreigntableid);
    // 从系统目录中获取外部服务器定义信息
    fpinfo->server = GetForeignServer(fpinfo->table->serverid);

    /*
     * 分类处理限制条件子句：
     * 1. 可以下推到远程服务器执行的子句(remote_conds)
     * 2. 必须在本地执行的子句(local_conds)
     */
    foreach (lc, baserel->baserestrictinfo)
    {
        RestrictInfo *ri = (RestrictInfo *)lfirst(lc);

        // 检查子句是否可以作为远程表达式下推执行
        if (tdengine_is_foreign_expr(root, baserel, ri->clause, false))
            fpinfo->remote_conds = lappend(fpinfo->remote_conds, ri);
        else
            fpinfo->local_conds = lappend(fpinfo->local_conds, ri);
    }

    /*
     * 识别需要从远程服务器检索的属性：
     * 1. 从目标表达式中提取属性编号
     * 2. 从本地条件子句中提取额外需要的属性编号
     */
    pull_varattnos((Node *)baserel->reltarget->exprs, baserel->relid, &fpinfo->attrs_used);

    foreach (lc, fpinfo->local_conds)
    {
        RestrictInfo *rinfo = (RestrictInfo *)lfirst(lc);
        pull_varattnos((Node *)rinfo->clause, baserel->relid, &fpinfo->attrs_used);
    }

    /*
     * 计算本地条件的选择性：
     * - 用于估算本地过滤条件会过滤掉多少数据
     * - 基于本地统计信息进行估算
     */
    fpinfo->local_conds_sel = clauselist_selectivity(root,
                                                     fpinfo->local_conds,
                                                     baserel->relid,
                                                     JOIN_INNER,
                                                     NULL);

    /*
     * 初始化成本估算缓存为负值：
     * - 用于标记成本估算尚未计算
     * - 后续首次调用estimate_path_cost_size()时会填充实际值
     */
    fpinfo->rel_startup_cost = -1;
    fpinfo->rel_total_cost = -1;

    /*
     * 行数估算逻辑：
     * - 如果配置为使用远程估算(use_remote_estimate)，则连接远程服务器执行EXPLAIN
     * - 否则使用本地统计信息进行估算
     */
    if (fpinfo->use_remote_estimate)
    {
        ereport(ERROR, (errmsg("Remote estimation is unsupported")));
    }
    else
    {
        /*
         * 本地估算处理：
         * - 如果表未分析过(reltuples<0)，使用默认值(10页)估算
         * - 否则使用本地统计信息进行更精确的估算
         */
        if (baserel->tuples < 0)
        {
            baserel->pages = 10;
            baserel->tuples = (10 * BLCKSZ) / (baserel->reltarget->width +
                                               MAXALIGN(SizeofHeapTupleHeader));
        }

        // 使用本地统计信息估算关系大小
        set_baserel_size_estimates(root, baserel);

        // 计算路径成本和大小估算
        estimate_path_cost_size(root, baserel, NIL, NIL,
                                &fpinfo->rows, &fpinfo->width,
                                &fpinfo->startup_cost, &fpinfo->total_cost);
    }

    /*
     * 设置关系名称：
     * - 用于EXPLAIN输出中标识关系
     * - 使用relid作为标识符
     */
    fpinfo->relation_name = psprintf("%u", baserel->relid);
}

//========================== GetForeignPaths ====================
/*
 *      为对外表的扫描创建可能的扫描路径
 */
static void
tdengineGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
    // 启动成本初始化为 10
    Cost startup_cost = 10;
    // 总成本初始化为表的行数加上启动成本
    Cost total_cost = baserel->rows + startup_cost;

    // 输出调试信息，显示当前函数名
    elog(DEBUG1, "tdengine_fdw : %s", __func__);
    /* 估算成本 */
    // 重新设置总成本为表的行数
    total_cost = baserel->rows;

    /* 创建一个 ForeignPath 节点并将其作为唯一可能的路径添加 */
    add_path(baserel, (Path *)
             // 创建一个外部扫描路径
             create_foreignscan_path(root, baserel,
                                     NULL, /* 默认的路径目标 */
                                     baserel->rows,
                                     startup_cost,
                                     total_cost,
                                     NIL, /* 没有路径键 */
                                          // #if (PG_VERSION_NUM >= 120000)
                                     baserel->lateral_relids,
                                     // #else
                                     //                                      NULL,    /* 也没有外部关系 */
                                     // #endif
                                     NULL,   /* 没有额外的计划 */
                                             // #if PG_VERSION_NUM >= 170000
                                             //                                      NIL, /* 没有 fdw_restrictinfo 列表 */
                                             // #endif
                                     NULL)); /* 没有 fdw_private 数据 */
}

//====================== GetForeignPlan ======================
/*
 * 获取一个外部扫描计划节点
 */
static ForeignScan *
tdengineGetForeignPlan(PlannerInfo *root,
                       RelOptInfo *baserel,
                       Oid foreigntableid,
                       ForeignPath *best_path,
                       List *tlist,
                       List *scan_clauses,
                       Plan *outer_plan)
{
    // 将外部关系的私有数据转换为 TDengineFdwRelationInfo
    TDengineFdwRelationInfo *fpinfo = (TDengineFdwRelationInfo *)baserel->fdw_private;
    // 获取扫描关系的ID
    Index scan_relid = baserel->relid;
    // 传递给执行器的私有数据列表
    List *fdw_private = NULL;
    // 本地执行的表达式列表
    List *local_exprs = NULL;
    // 远程执行的表达式列表
    List *remote_exprs = NULL;
    // 参数列表
    List *params_list = NULL;
    // 传递给外部服务器的目标列表
    List *fdw_scan_tlist = NIL;
    // 远程条件列表
    List *remote_conds = NIL;

    StringInfoData sql;
    // 从远程服务器检索的属性列表
    List *retrieved_attrs;
    // 遍历列表的迭代器
    ListCell *lc;
    // 需要重新检查的条件列表
    List *fdw_recheck_quals = NIL;
    // 表示是否为 FOR UPDATE 操作的标志
    int for_update;
    // 表示查询是否有 LIMIT 子句的标志
    bool has_limit = false;

    // 调试信息
    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    // 决定是否在目标列表中支持函数下推
    fpinfo->is_tlist_func_pushdown = tdengine_is_foreign_function_tlist(root, baserel, tlist);

    /*
     * 获取由 tdengineGetForeignUpperPaths() 创建的 FDW 私有数据（如果有的话）。
     */
    if (best_path->fdw_private)
    {
        // #if (PG_VERSION_NUM >= 150000)
        // 从 FDW 私有数据中获取是否有 LIMIT 子句的标志
        has_limit = boolVal(list_nth(best_path->fdw_private, FdwPathPrivateHasLimit));
        // #else
        //         has_limit = intVal(list_nth(best_path->fdw_private, FdwPathPrivateHasLimit));
        // #endif
    }

    /*
     * 构建要发送执行的查询字符串，并识别要作为参数发送的表达式。
     */

    // 初始化 SQL 查询字符串
    initStringInfo(&sql);

    /*
     * 将扫描子句分为可以在远程执行的和不能在远程执行的。
     * 之前由 classifyConditions 确定为安全或不安全的 baserestrictinfo 子句分别存储在
     * fpinfo->remote_conds 和 fpinfo->local_conds 中。
     * 扫描子句列表中的其他内容将是连接子句，我们必须检查其是否可以在远程安全执行。
     *
     * 注意：这里看到的连接子句应该与之前 tdengineGetForeignPaths 检查的完全相同。
     * 也许值得将之前完成的分类工作传递过来，而不是在这里重复进行。
     *
     * 此代码必须与 "extract_actual_clauses(scan_clauses, false)" 匹配，
     * 除了关于远程执行还是本地执行的额外决策。
     * 但是请注意，我们只从 local_exprs 列表中剥离 RestrictInfo 节点，
     * 因为 appendWhereClause 期望的是 RestrictInfos 列表。
     */
    if ((baserel->reloptkind == RELOPT_BASEREL ||
         baserel->reloptkind == RELOPT_OTHER_MEMBER_REL) &&
        fpinfo->is_tlist_func_pushdown == false)
    {
        // 提取实际要从远程 TDengine 获取的列
        tdengine_extract_slcols(fpinfo, root, baserel, tlist);

        // 遍历扫描子句列表
        foreach (lc, scan_clauses)
        {
            // 获取当前的限制信息节点
            RestrictInfo *rinfo = (RestrictInfo *)lfirst(lc);

            // 确保当前节点是 RestrictInfo 类型
            Assert(IsA(rinfo, RestrictInfo));

            // 忽略任何伪常量，它们会在其他地方处理
            if (rinfo->pseudoconstant)
                continue;

            // 如果该条件在远程条件列表中
            if (list_member_ptr(fpinfo->remote_conds, rinfo))
            {
                // 将该条件的子句添加到远程表达式列表中
                remote_exprs = lappend(remote_exprs, rinfo->clause);
            }
            // 如果该条件在本地条件列表中
            else if (list_member_ptr(fpinfo->local_conds, rinfo))
            {
                // 将该条件的子句添加到本地表达式列表中
                local_exprs = lappend(local_exprs, rinfo->clause);
            }
            // 如果该条件可以在远程安全执行
            else if (tdengine_is_foreign_expr(root, baserel, rinfo->clause, false))
            {
                // 将该条件的子句添加到远程表达式列表中
                remote_exprs = lappend(remote_exprs, rinfo->clause);
            }
            else
            {
                // 否则将该条件的子句添加到本地表达式列表中
                local_exprs = lappend(local_exprs, rinfo->clause);
            }

            /*
             * 对于基础关系扫描，我们必须支持提前谓词求值（EPQ）重新检查，
             * 这应该重新检查所有远程条件。
             */
            fdw_recheck_quals = remote_exprs;
        }
    }
    else
    {
        /*
         * 连接关系或上层关系 - 将扫描关系 ID 设置为 0。
         */
        scan_relid = 0;

        /*
         * 对于连接关系，baserestrictinfo 为空，并且我们目前不考虑参数化，
         * 因此连接关系或上层关系也应该没有扫描子句。
         */
        if (fpinfo->is_tlist_func_pushdown == false)
        {
            // 确保扫描子句列表为空
            Assert(!scan_clauses);
        }

        /*
         * 相反，我们从 fdw_private 结构中获取要应用的条件。
         */
        // 提取实际的远程条件子句
        remote_exprs = extract_actual_clauses(fpinfo->remote_conds, false);
        // 提取实际的本地条件子句
        local_exprs = extract_actual_clauses(fpinfo->local_conds, false);

        /*
         * 在这种情况下，我们将 fdw_recheck_quals 留空，因为我们永远不需要应用 EPQ 重新检查子句。
         * 对于连接关系，EPQ 重新检查在其他地方处理 --- 参见 tdengineGetForeignJoinPaths()。
         * 如果我们正在规划上层关系（即远程分组或聚合），则不需要进行 EPQ，
         * 因为不允许使用 SELECT FOR UPDATE，
         * 实际上我们不能将远程子句放入 fdw_recheck_quals 中，因为未聚合的变量在本地不可用。
         */

        /*
         * 构建要从外部服务器获取的列的列表。
         */
        if (fpinfo->is_tlist_func_pushdown == true)
        {
            // 遍历目标列表
            foreach (lc, tlist)
            {
                // 获取当前的目标项
                TargetEntry *tle = lfirst_node(TargetEntry, lc);

                /*
                 * 从 FieldSelect 子句中提取函数，并添加到 fdw_scan_tlist 中，
                 * 以便仅下推函数部分
                 */
                if (fpinfo->is_tlist_func_pushdown == true && IsA((Node *)tle->expr, FieldSelect))
                {
                    // 将提取的函数添加到 fdw_scan_tlist 中
                    fdw_scan_tlist = add_to_flat_tlist(fdw_scan_tlist,
                                                       tdengine_pull_func_clause((Node *)tle->expr));
                }
                else
                {
                    // 否则将目标项添加到 fdw_scan_tlist 中
                    fdw_scan_tlist = lappend(fdw_scan_tlist, tle);
                }
            }

            // 遍历本地条件列表
            foreach (lc, fpinfo->local_conds)
            {
                // 获取当前的限制信息节点
                RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);
                // 存储变量列表
                List *varlist = NIL;

                // 从条件子句中提取无模式变量
                varlist = tdengine_pull_slvars(rinfo->clause, baserel->relid,
                                               varlist, true, NULL, &(fpinfo->slinfo));

                // 如果变量列表为空
                if (varlist == NIL)
                {
                    // 从条件子句中提取变量
                    varlist = pull_var_clause((Node *)rinfo->clause,
                                              PVC_RECURSE_PLACEHOLDERS);
                }

                // 将变量列表添加到 fdw_scan_tlist 中
                fdw_scan_tlist = add_to_flat_tlist(fdw_scan_tlist, varlist);
            }
        }
        else
        {
            // 构建要解析的目标列表
            fdw_scan_tlist = tdengine_build_tlist_to_deparse(baserel);
        }

        /*
         * 确保外部计划生成的元组描述符与我们的扫描元组插槽匹配。
         * 这是安全的，因为所有扫描和连接都支持投影，所以我们永远不需要插入结果节点。
         * 此外，从外部计划的条件中移除本地条件，以免它们被评估两次，
         * 一次由本地计划评估，一次由扫描评估。
         */
        if (outer_plan)
        {
            /*
             * 目前，我们只考虑连接之外的分组和聚合。
             * 涉及聚合或分组的查询不需要 EPQ 机制，因此这里不应该有外部计划。
             */
            Assert(baserel->reloptkind != RELOPT_UPPER_REL);
            // 设置外部计划的目标列表
            outer_plan->targetlist = fdw_scan_tlist;

            // 遍历本地表达式列表
            foreach (lc, local_exprs)
            {
                // 将外部计划转换为连接计划
                Join *join_plan = (Join *)outer_plan;
                // 获取当前的条件子句
                Node *qual = lfirst(lc);

                // 从外部计划的条件中移除当前条件子句
                outer_plan->qual = list_delete(outer_plan->qual, qual);

                /*
                 * 对于内连接，外部扫描计划的本地条件也可能是连接条件的一部分。
                 */
                if (join_plan->jointype == JOIN_INNER)
                {
                    // 从连接计划的连接条件中移除当前条件子句
                    join_plan->joinqual = list_delete(join_plan->joinqual,
                                                      qual);
                }
            }
        }
    }

    /*
     * 构建要发送执行的查询字符串，并识别要作为参数发送的表达式。
     */
    // 重新初始化 SQL 查询字符串
    initStringInfo(&sql);
    // 为关系解析 SELECT 语句
    tdengine_deparse_select_stmt_for_rel(&sql, root, baserel, fdw_scan_tlist,
                                         remote_exprs, best_path->path.pathkeys,
                                         false, &retrieved_attrs, &params_list, has_limit);

    // 记住远程表达式，供 tdenginePlanDirectModify 可能使用
    fpinfo->final_remote_exprs = remote_exprs;

    // 初始化 FOR UPDATE 标志
    for_update = false;
    if (baserel->relid == root->parse->resultRelation &&
        (root->parse->commandType == CMD_UPDATE ||
         root->parse->commandType == CMD_DELETE))
    {
        /* 关系是 UPDATE/DELETE 目标，因此使用 FOR UPDATE */
        for_update = true;
    }

    // 获取远程条件
    if (baserel->reloptkind == RELOPT_UPPER_REL)
    {
        TDengineFdwRelationInfo *ofpinfo;

        ofpinfo = (TDengineFdwRelationInfo *)fpinfo->outerrel->fdw_private;
        remote_conds = ofpinfo->remote_conds;
    }
    else
        remote_conds = remote_exprs;

    /*
     * 构建将提供给执行器的 fdw_private 列表。
     * 列表中的项必须与上面的 enum FdwScanPrivateIndex 匹配。
     */
    // 创建包含 SQL 查询字符串、检索的属性和 FOR UPDATE 标志的列表
    fdw_private = list_make3(makeString(sql.data), retrieved_attrs, makeInteger(for_update));
    // 将 fdw_scan_tlist 添加到 fdw_private 列表中
    fdw_private = lappend(fdw_private, fdw_scan_tlist);
    // 将函数下推标志添加到 fdw_private 列表中
    fdw_private = lappend(fdw_private, makeInteger(fpinfo->is_tlist_func_pushdown));
    // 将无模式标志添加到 fdw_private 列表中
    fdw_private = lappend(fdw_private, makeInteger(fpinfo->slinfo.schemaless));
    // 将远程条件添加到 fdw_private 列表中
    fdw_private = lappend(fdw_private, remote_conds);

    /*
     * 根据目标列表、本地过滤表达式、远程参数表达式和 FDW 私有信息创建 ForeignScan 节点。
     *
     * 注意，远程参数表达式存储在完成的计划节点的 fdw_exprs 字段中；
     * 我们不能将它们保存在私有状态中，因为那样它们将不会受到后续规划器处理的影响。
     */
    return make_foreignscan(tlist,
                            local_exprs,
                            scan_relid,
                            params_list,
                            fdw_private,
                            fdw_scan_tlist,
                            fdw_recheck_quals,
                            outer_plan);
}

//========================== BeginForeignScan =====================
/*
 * tdengineBeginForeignScan - 初始化外部表扫描
 * 功能: 为TDengine外部表扫描初始化执行状态,准备查询参数和连接信息
 * 参数:
 *   @node: ForeignScanState节点,包含扫描状态信息
 *   @eflags: 执行标志位,指示扫描的执行方式
 * 处理流程:
 *   1. 初始化执行状态结构体festate
 *   2. 从计划节点(fsplan)中提取查询相关信息:
 *      - SQL查询字符串
 *      - 需要检索的属性列表
 *      - FOR UPDATE标志
 *      - 目标列表
 *      - 函数下推标志
 *      - 无模式(schemaless)标志
 *      - 远程表达式列表
 *   3. 确定扫描关系ID(rtindex)和范围表条目(rte)
 *   4. 获取连接选项(tdengineFdwOptions)和用户映射(user)
 *   5. 初始化无模式信息(slinfo)
 *   6. 如果有查询参数(numParams>0),准备参数转换信息
 */
static void
tdengineBeginForeignScan(ForeignScanState *node, int eflags)
{
    // 执行状态结构体
    TDengineFdwExecState *festate = NULL;
    // 执行状态
    EState *estate = node->ss.ps.state;
    // 外部扫描计划节点
    ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;
    // 范围表条目
    RangeTblEntry *rte;
    // 参数数量
    int numParams;
    // 扫描关系ID
    int rtindex;
    // 无模式标志
    bool schemaless;
    // 用户ID
    Oid userid;
    // #ifdef CXX_CLIENT
    // 外部表信息(用于C++客户端)
    ForeignTable *ftable;
    // #endif
    // 远程表达式列表
    List *remote_exprs;

    // 调试日志
    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    /* 初始化执行状态 */
    festate = (TDengineFdwExecState *)palloc0(sizeof(TDengineFdwExecState));
    node->fdw_state = (void *)festate;
    festate->rowidx = 0;

    /* 从计划节点中提取信息 */
    festate->query = strVal(list_nth(fsplan->fdw_private, 0));                                 // SQL查询字符串
    festate->retrieved_attrs = list_nth(fsplan->fdw_private, 1);                               // 需要检索的属性
    festate->for_update = intVal(list_nth(fsplan->fdw_private, 2)) ? true : false;             // FOR UPDATE标志
    festate->tlist = (List *)list_nth(fsplan->fdw_private, 3);                                 // 目标列表
    festate->is_tlist_func_pushdown = intVal(list_nth(fsplan->fdw_private, 4)) ? true : false; // 函数下推标志
    schemaless = intVal(list_nth(fsplan->fdw_private, 5)) ? true : false;                      // 无模式标志
    remote_exprs = (List *)list_nth(fsplan->fdw_private, 6);                                   // 远程表达式列表

    festate->cursor_exists = false; // 游标存在标志初始化为false

    /* 确定扫描关系ID */
    if (fsplan->scan.scanrelid > 0)
        rtindex = fsplan->scan.scanrelid;
    else
        rtindex = bms_next_member(fsplan->fs_relids, -1);

    // 获取范围表条目
    rte = exec_rt_fetch(rtindex, estate);

    /* 获取用户ID */
    userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

    /* 获取连接选项和用户映射 */
    festate->tdengineFdwOptions = tdengine_get_options(rte->relid, userid);
    ftable = GetForeignTable(rte->relid);
    festate->user = GetUserMapping(userid, ftable->serverid);

    /* 初始化无模式信息 */
    tdengine_get_schemaless_info(&(festate->slinfo), schemaless, rte->relid);

    /* 准备查询参数 */
    numParams = list_length(fsplan->fdw_exprs);
    festate->numParams = numParams;
    if (numParams > 0)
    {
        prepare_query_params((PlanState *)node,
                             fsplan->fdw_exprs,
                             remote_exprs,
                             rte->relid,
                             numParams,
                             &festate->param_flinfo,
                             &festate->param_exprs,
                             &festate->param_values,
                             &festate->param_types,
                             &festate->param_tdengine_types,
                             &festate->param_tdengine_values,
                             &festate->param_column_info);
    }
}

//======================== IterateForeignScan ==================
/*
 * 逐个迭代从 TDengine 获取行，并将其放入元组槽中
 */
static TupleTableSlot *
tdengineIterateForeignScan(ForeignScanState *node)
{
    // 获取执行状态
    TDengineFdwExecState *festate = (TDengineFdwExecState *)node->fdw_state;
    // 获取元组槽
    TupleTableSlot *tupleSlot = node->ss.ss_ScanTupleSlot;
    // 获取执行状态
    EState *estate = node->ss.ps.state;
    // 获取元组描述符
    TupleDesc tupleDescriptor = tupleSlot->tts_tupleDescriptor;
    // 获取 TDengine 选项
    tdengine_opt *options;
    // 存储查询返回结果
    struct TDengineQuery_return volatile ret;
    // 存储查询结果
    struct TDengineResult volatile *result = NULL;
    // 获取外键扫描计划
    ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;
    // 范围表条目
    RangeTblEntry *rte;
    // 范围表索引
    int rtindex;
    // 是否为聚合操作
    bool is_agg;

    // 打印调试信息
    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    /*
     * 确定以哪个用户身份进行远程访问。这应该与 ExecCheckRTEPerms() 一致。
     * 在连接或聚合操作的情况下，使用编号最小的成员范围表条目作为代表；
     * 从任何一个成员范围表条目得到的结果都是相同的。
     */
    if (fsplan->scan.scanrelid > 0)
    {
        // 如果扫描关系 ID 大于 0，使用该 ID
        rtindex = fsplan->scan.scanrelid;
        // 不是聚合操作
        is_agg = false;
    }
    else
    {
        // 否则，获取最小的范围表 ID
        rtindex = bms_next_member(fsplan->fs_relids, -1);
        // 是聚合操作
        is_agg = true;
    }
    // 获取范围表条目
    rte = rt_fetch(rtindex, estate->es_range_table);

    // 获取 TDengine 选项
    options = festate->tdengineFdwOptions;

    /*
     * 如果这是在 Begin 或 ReScan 之后的第一次调用，我们需要在远程端创建游标。
     * 绑定参数的操作在这个函数中完成。
     */
    if (!festate->cursor_exists)
        // 创建游标
        create_cursor(node);

    // 初始化元组槽的值为 0
    memset(tupleSlot->tts_values, 0, sizeof(Datum) * tupleDescriptor->natts);
    // 初始化元组槽的空值标记为 true
    memset(tupleSlot->tts_isnull, true, sizeof(bool) * tupleDescriptor->natts);
    // 清空元组槽
    ExecClearTuple(tupleSlot);

    if (festate->rowidx == 0)
    {
        // 保存旧的内存上下文
        MemoryContext oldcontext = NULL;

        // 异常处理开始
        PG_TRY();
        {
            // festate->rows 需要比每行更长的上下文
            oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);
            // #ifdef CXX_CLIENT
            // TODO: 使用 C++ 客户端执行查询
            ret = TDengineQuery(festate->query, festate->user, options,
                                // #else
                                //             // 使用普通方式执行查询
                                //             ret = TDengineQuery(festate->query, options->svr_address, options->svr_port,
                                //                                 options->svr_username, options->svr_password,
                                //                                 options->svr_database,
                                // #endif
                                festate->param_tdengine_types,
                                festate->param_tdengine_values,
                                festate->numParams);
            if (ret.r1 != NULL)
            {
                // 复制错误信息
                char *err = pstrdup(ret.r1);
                // 释放原错误信息
                free(ret.r1);
                ret.r1 = err;
                // 打印错误信息
                elog(ERROR, "tdengine_fdw : %s", err);
            }

            result = ret.r0;
            festate->temp_result = (void *)result;

            // 获取结果集的行数
            festate->row_nums = result->nrow;
            // 打印查询信息
            elog(DEBUG1, "tdengine_fdw : query: %s", festate->query);

            // 切换回旧的内存上下文
            MemoryContextSwitchTo(oldcontext);
            // 释放结果集
            TDengineFreeResult((TDengineResult *)result);
        }
        // 异常处理捕获部分
        PG_CATCH();
        {
            if (ret.r1 == NULL)
            {
                // 释放结果集
                TDengineFreeResult((TDengineResult *)result);
            }

            if (oldcontext)
                // 切换回旧的内存上下文
                MemoryContextSwitchTo(oldcontext);

            // 重新抛出异常
            PG_RE_THROW();
        }
        // 异常处理结束
        PG_END_TRY();
    }

    if (festate->rowidx < festate->row_nums)
    {
        // 保存旧的内存上下文
        MemoryContext oldcontext = NULL;

        // 获取结果集
        result = (TDengineResult *)festate->temp_result;
        // 从结果行创建元组
        make_tuple_from_result_row(&(result->rows[festate->rowidx]),
                                   (TDengineResult *)result,
                                   tupleDescriptor,
                                   tupleSlot->tts_values,
                                   tupleSlot->tts_isnull,
                                   rte->relid,
                                   festate,
                                   is_agg);
        // 切换到查询上下文
        oldcontext = MemoryContextSwitchTo(estate->es_query_cxt);

        // 释放结果行
        freeTDengineResultRow(festate, festate->rowidx);

        if (festate->rowidx == (festate->row_nums - 1))
        {
            // 释放结果集
            freeTDengineResult(festate);
        }

        // 切换回旧的内存上下文
        MemoryContextSwitchTo(oldcontext);

        // 存储虚拟元组
        ExecStoreVirtualTuple(tupleSlot);
        // 行索引加 1
        festate->rowidx++;
    }

    // 返回元组槽
    return tupleSlot;
}

//===================== ReScanForeignScan =====================
/*
 * 从扫描的起始位置重新开始扫描。请注意，扫描所依赖的任何参数的值可能已经发生变化，
 * 因此新的扫描不一定会返回与之前完全相同的行。
 */
static void
tdengineReScanForeignScan(ForeignScanState *node)
{

    TDengineFdwExecState *festate = (TDengineFdwExecState *)node->fdw_state;

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    festate->cursor_exists = false;
    festate->rowidx = 0;
}

//===================== EndForeignScan =======================
/*
 * 结束对外部表的扫描，并释放本次扫描所使用的对象
 */
static void
tdengineEndForeignScan(ForeignScanState *node)
{
    TDengineFdwExecState *festate = (TDengineFdwExecState *)node->fdw_state;

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    if (festate != NULL)
    {
        festate->cursor_exists = false;
        festate->rowidx = 0;
    }
}

/*
 * tdengineAddForeignUpdateTargets
 *      为外部表的更新/删除操作添加所需的resjunk列
 *
 * 功能: 为外部表的UPDATE/DELETE操作添加标识列(时间列和标签列)
 * 这些列将被标记为resjunk(查询中不需要但执行需要的列)
 *
 * @root: 规划器信息
 * @rtindex: 范围表索引
 * @target_rte: 目标范围表条目
 * @target_relation: 目标关系(表)
 */
static void
tdengineAddForeignUpdateTargets(
    PlannerInfo *root,
    Index rtindex,
    RangeTblEntry *target_rte,
    Relation target_relation)
{
    // 获取目标关系的OID
    Oid relid = RelationGetRelid(target_relation);
    // 获取目标关系的元组描述符
    TupleDesc tupdesc = target_relation->rd_att;
    int i;

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    /* 遍历外部表的所有列 */
    for (i = 0; i < tupdesc->natts; i++)
    {
        // 获取当前列的属性信息
        Form_pg_attribute att = TupleDescAttr(tupdesc, i);
        // 获取当前列的属性编号
        AttrNumber attrno = att->attnum;
        // 获取当前列的名称
        char *colname = tdengine_get_column_name(relid, attrno);

        /* 如果是时间列或标签列 */
        if (TDENGINE_IS_TIME_COLUMN(colname) || tdengine_is_tag_key(colname, relid))
        {
            Var *var;

            /* 创建一个Var节点表示所需的值 */
            var = makeVar(rtindex,
                          attrno,
                          att->atttypid,
                          att->atttypmod,
                          att->attcollation,
                          0);

            /* 将其注册为该目标关系需要的行标识列 */
            add_row_identity_var(root, var, rtindex, pstrdup(NameStr(att->attname)));
        }
    }
}

/*
 * tdenginePlanForeignModify
 *    规划对外部表的插入/更新/删除操作
 *    功能: 为外部表的修改操作(INSERT/UPDATE/DELETE)生成执行计划
 *    参数:
 *      @root: 规划器信息
 *      @plan: 修改表操作计划
 *      @resultRelation: 结果关系索引
 *      @subplan_index: 子计划索引
 *    返回值: 包含SQL语句和目标属性列表的链表
 */
static List *
tdenginePlanForeignModify(PlannerInfo *root,
                          ModifyTable *plan,
                          Index resultRelation,
                          int subplan_index)
{
    // 获取操作类型(INSERT/UPDATE/DELETE)
    CmdType operation = plan->operation;
    // 获取范围表条目
    RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
    Relation rel;
    StringInfoData sql;      // 用于构建SQL语句的缓冲区
    List *targetAttrs = NIL; // 目标属性列表
    TupleDesc tupdesc;       // 元组描述符

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    initStringInfo(&sql); // 初始化SQL语句缓冲区

    /*
     * 核心代码已经对每个关系加锁，这里可以使用NoLock
     */
    rel = table_open(rte->relid, NoLock); // 打开关系表
    tupdesc = RelationGetDescr(rel);      // 获取关系表的元组描述符

    // 根据操作类型处理
    if (operation == CMD_INSERT)
    {
        // INSERT操作: 收集所有非删除列
        int attnum;
        for (attnum = 1; attnum <= tupdesc->natts; attnum++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);
            if (!attr->attisdropped)
                targetAttrs = lappend_int(targetAttrs, attnum);
        }
    }
    else if (operation == CMD_UPDATE)
        elog(ERROR, "UPDATE is not supported"); // 不支持UPDATE操作
    else if (operation == CMD_DELETE)
    {
        // DELETE操作: 收集时间列和所有标签列
        int i;
        Oid foreignTableId = RelationGetRelid(rel);
        for (i = 0; i < tupdesc->natts; i++)
        {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            AttrNumber attrno = attr->attnum;
            char *colname = tdengine_get_column_name(foreignTableId, attrno);
            // 只添加时间列和标签列
            if (TDENGINE_IS_TIME_COLUMN(colname) || tdengine_is_tag_key(colname, rte->relid))
                if (!attr->attisdropped)
                    targetAttrs = lappend_int(targetAttrs, attrno);
        }
    }
    else
        elog(ERROR, "Not supported"); // 不支持其他操作类型

    // 检查不支持的特性
    if (plan->returningLists)
        elog(ERROR, "RETURNING is not supported"); // 不支持RETURNING子句
    if (plan->onConflictAction != ONCONFLICT_NONE)
        elog(ERROR, "ON CONFLICT is not supported"); // 不支持ON CONFLICT子句

    /*
     * 构建SQL语句
     */
    switch (operation)
    {
    case CMD_INSERT:
    case CMD_UPDATE:
        break; // INSERT和UPDATE操作暂不处理SQL构建
    case CMD_DELETE:
        // 构建DELETE语句
        tdengine_deparse_delete(&sql, root, resultRelation, rel, targetAttrs);
        break;
    default:
        elog(ERROR, "unexpected operation: %d", (int)operation);
        break;
    }

    table_close(rel, NoLock); // 关闭关系表

    // 返回SQL语句和目标属性列表
    return list_make2(makeString(sql.data), targetAttrs);
}

/*
 * tdengineBeginForeignModify - 初始化外部表修改操作
 * 功能: 为INSERT/UPDATE/DELETE操作准备执行状态
 * 参数:
 *   @mtstate: 修改表操作状态
 *   @resultRelInfo: 结果关系信息
 *   @fdw_private: FDW私有数据列表
 *   @subplan_index: 子计划索引
 *   @eflags: 执行标志位
 * 处理流程:
 *   1. 检查是否为EXPLAIN ONLY模式，是则直接返回
 *   2. 初始化执行状态结构体fmstate
 *   3. 获取用户身份和连接选项
 *   4. 设置查询语句和检索属性
 *   5. 为INSERT/DELETE操作准备列信息
 */
static void
tdengineBeginForeignModify(ModifyTableState *mtstate,
                           ResultRelInfo *resultRelInfo,
                           List *fdw_private,
                           int subplan_index,
                           int eflags)
{
    // 执行状态结构体
    TDengineFdwExecState *fmstate = NULL;
    // 执行状态
    EState *estate = mtstate->ps.state;
    // 目标关系
    Relation rel = resultRelInfo->ri_RelationDesc;
    // 参数数量
    AttrNumber n_params = 0;
    // 类型输出函数OID
    Oid typefnoid = InvalidOid;
    // 用户ID
    Oid userid;
    // 是否为变长类型标志
    bool isvarlena = false;
    // 列表迭代器
    ListCell *lc = NULL;
    // 外部表OID
    Oid foreignTableId = InvalidOid;
    // 子计划
    Plan *subplan;
    int i;
    // 范围表条目
    RangeTblEntry *rte;
    // 外部表信息
    ForeignTable *ftable;

    // 调试日志
    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    /* 如果是EXPLAIN ONLY模式，直接返回 */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    // 获取外部表OID
    foreignTableId = RelationGetRelid(rel);
    // 获取子计划
    subplan = outerPlanState(mtstate)->plan;

    // 初始化执行状态结构体
    fmstate = (TDengineFdwExecState *)palloc0(sizeof(TDengineFdwExecState));
    fmstate->rowidx = 0;

    /* 获取用户身份 */
    rte = exec_rt_fetch(resultRelInfo->ri_RangeTableIndex,
                        mtstate->ps.state);
    userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();

    /* 获取连接选项和用户映射 */
    fmstate->tdengineFdwOptions = tdengine_get_options(foreignTableId, userid);
    ftable = GetForeignTable(foreignTableId);
    fmstate->user = GetUserMapping(userid, ftable->serverid);

    // 设置查询语句和检索属性
    fmstate->rel = rel;
    fmstate->query = strVal(list_nth(fdw_private, FdwModifyPrivateUpdateSql));
    fmstate->retrieved_attrs = (List *)list_nth(fdw_private, FdwModifyPrivateTargetAttnums);

    /* 为INSERT/DELETE操作准备列信息 */
    if (mtstate->operation == CMD_INSERT || mtstate->operation == CMD_DELETE)
    {
        fmstate->column_list = NIL;

        if (fmstate->retrieved_attrs)
        {
            foreach (lc, fmstate->retrieved_attrs)
            {
                int attnum = lfirst_int(lc);
                struct TDengineColumnInfo *col = (TDengineColumnInfo *)palloc0(sizeof(TDengineColumnInfo));

                /* 获取列名并设置列类型 */
                col->column_name = tdengine_get_column_name(foreignTableId, attnum);
                if (TDENGINE_IS_TIME_COLUMN(col->column_name))
                    col->column_type = TDENGINE_TIME_KEY;
                else if (tdengine_is_tag_key(col->column_name, foreignTableId))
                    col->column_type = TDENGINE_TAG_KEY;
                else
                    col->column_type = TDENGINE_FIELD_KEY;

                /* 将列信息添加到列列表中 */
                fmstate->column_list = lappend(fmstate->column_list, col);
            }
        }
        // 设置批量大小选项
        fmstate->batch_size = tdengine_get_batch_size_option(rel);
    }

    /* 计算参数总数(检索属性数+1) */
    n_params = list_length(fmstate->retrieved_attrs) + 1;

    /* 分配并初始化各种参数信息的内存空间 */
    fmstate->p_flinfo = (FmgrInfo *)palloc0(sizeof(FmgrInfo) * n_params);                              // 参数输出函数信息
    fmstate->p_nums = 0;                                                                               // 参数计数器初始化为0
    fmstate->param_flinfo = (FmgrInfo *)palloc0(sizeof(FmgrInfo) * n_params);                          // 参数转换函数信息
    fmstate->param_types = (Oid *)palloc0(sizeof(Oid) * n_params);                                     // 参数类型OID
    fmstate->param_tdengine_types = (TDengineType *)palloc0(sizeof(TDengineType) * n_params);          // TDengine参数类型
    fmstate->param_tdengine_values = (TDengineValue *)palloc0(sizeof(TDengineValue) * n_params);       // TDengine参数值
    fmstate->param_column_info = (TDengineColumnInfo *)palloc0(sizeof(TDengineColumnInfo) * n_params); // 列信息

    /* 创建临时内存上下文用于每行数据处理 */
    fmstate->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
                                              "tdengine_fdw temporary data",
                                              ALLOCSET_SMALL_SIZES);

    /* 设置可传输参数 */
    foreach (lc, fmstate->retrieved_attrs)
    {
        int attnum = lfirst_int(lc);                                               // 获取属性编号
        Form_pg_attribute attr = TupleDescAttr(RelationGetDescr(rel), attnum - 1); // 获取属性信息

        Assert(!attr->attisdropped); // 确保属性未被删除

        /* 获取类型输出函数信息并初始化 */
        getTypeOutputInfo(attr->atttypid, &typefnoid, &isvarlena);
        fmgr_info(typefnoid, &fmstate->p_flinfo[fmstate->p_nums]);
        fmstate->p_nums++; // 递增参数计数器
    }
    Assert(fmstate->p_nums <= n_params); // 验证参数数量不超过总数

    /* 分配并初始化junk属性索引数组 */
    fmstate->junk_idx = palloc0(RelationGetDescr(rel)->natts * sizeof(AttrNumber));

    /* 遍历表的所有列 */
    for (i = 0; i < RelationGetDescr(rel)->natts; i++)
    {
        /*
         * 为主键列获取resjunk属性编号并存储
         * 在子计划的目标列表中查找对应属性
         */
        fmstate->junk_idx[i] =
            ExecFindJunkAttributeInTlist(subplan->targetlist,
                                         get_attname(foreignTableId, i + 1
                                                     // #if (PG_VERSION_NUM >= 110000)
                                                     ,
                                                     false
                                                     // #endif
                                                     ));
    }

    /* 初始化辅助状态 */
    fmstate->aux_fmstate = NULL;

    /* 将执行状态设置到结果关系信息中 */
    resultRelInfo->ri_FdwState = fmstate;
}

/*
 * tdengineExecForeignInsert - 执行外部表单行插入操作
 * 功能: 向TDengine外部表插入一行数据
 * 参数:
 *   @estate: 执行状态
 *   @resultRelInfo: 结果关系信息
 *   @slot: 包含待插入数据的元组槽
 *   @planSlot: 计划元组槽
 * 返回值: 插入成功返回元组槽，失败返回NULL
 */
static TupleTableSlot *
tdengineExecForeignInsert(EState *estate,
                          ResultRelInfo *resultRelInfo,
                          TupleTableSlot *slot,
                          TupleTableSlot *planSlot)
{
    // 获取执行状态
    TDengineFdwExecState *fmstate = (TDengineFdwExecState *)resultRelInfo->ri_FdwState;
    // 用于存储返回的元组槽
    TupleTableSlot **rslot;
    // 插入的元组数量(单行插入固定为1)
    int numSlots = 1;

    // 记录调试日志
    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    /*
     * 如果存在辅助执行状态(aux_fmstate)，则临时替换当前执行状态
     * 用于处理特殊插入场景
     */
    if (fmstate->aux_fmstate)
        resultRelInfo->ri_FdwState = fmstate->aux_fmstate;

    // 执行实际的插入操作
    rslot = execute_foreign_insert_modify(estate, resultRelInfo, &slot,
                                          &planSlot, numSlots);

    /* 恢复原始执行状态 */
    if (fmstate->aux_fmstate)
        resultRelInfo->ri_FdwState = fmstate;

    // 返回插入结果(成功返回元组槽，失败返回NULL)
    return rslot ? *rslot : NULL;
}

// #if (PG_VERSION_NUM >= 140000)
/*
 * tdengineExecForeignBatchInsert - 批量插入多行数据到外部表
 * 功能: 执行批量插入操作，将多行数据一次性插入到TDengine外部表中
 *
 * 参数:
 *   @estate: 执行状态，包含查询执行环境信息
 *   @resultRelInfo: 结果关系信息，描述目标表的结构和状态
 *   @slots: 元组槽数组，包含待插入的多行数据
 *   @planSlots: 计划元组槽数组(当前未使用，保留参数)
 *   @numSlots: 指向插入行数的指针，表示要处理的元组数量
 *
 * 返回值: 返回处理后的元组槽数组指针
 *
 * 处理流程:
 *   1. 获取执行状态(fmstate)和返回结果槽指针(rslot)
 *   2. 记录调试日志
 *   3. 检查是否存在辅助执行状态(aux_fmstate):
 *      a. 存在则临时替换当前执行状态
 *   4. 调用execute_foreign_insert_modify执行实际插入操作
 *   5. 恢复原始执行状态(如果之前替换过)
 *   6. 返回处理结果
 *
 * 注意事项:
 *   - 主要用于提高批量插入性能
 *   - 支持临时切换执行状态处理特殊插入场景
 *   - 实际插入逻辑在execute_foreign_insert_modify中实现
 */
static TupleTableSlot **
tdengineExecForeignBatchInsert(EState *estate,
                               ResultRelInfo *resultRelInfo,
                               TupleTableSlot **slots,
                               TupleTableSlot **planSlots,
                               int *numSlots)
{
    // 获取执行状态
    TDengineFdwExecState *fmstate = (TDengineFdwExecState *)resultRelInfo->ri_FdwState;
    // 用于存储返回结果
    TupleTableSlot **rslot;

    // 记录调试日志
    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    /*
     * 如果存在辅助执行状态(aux_fmstate)，临时替换当前执行状态
     * 用于处理特殊插入场景
     */
    if (fmstate->aux_fmstate)
        resultRelInfo->ri_FdwState = fmstate->aux_fmstate;

    // 执行实际的批量插入操作
    rslot = execute_foreign_insert_modify(estate, resultRelInfo, slots,
                                          planSlots, *numSlots);

    /* 恢复原始执行状态 */
    if (fmstate->aux_fmstate)
        resultRelInfo->ri_FdwState = fmstate;

    // 返回处理结果
    return rslot;
}


/*
 * tdengineGetForeignModifyBatchSize - 获取批量修改操作的批处理大小
 * 功能: 确定可以批量插入到外部表中的最大元组数量
 *
 * 参数:
 *   @resultRelInfo: 结果关系信息，包含表结构和状态信息
 *
 * 返回值:
 *   int - 返回允许的批量大小，如果批量操作不被允许则返回1
 *
 * 处理流程:
 *   1. 获取执行状态(fmstate)并记录调试日志
 *   2. 验证函数是否首次调用(ri_BatchSize应为0)
 *   3. 检查是否在UPDATE操作的目标关系上执行插入(应被拒绝)
 *   4. 确定批量大小:
 *      a. 如果有执行状态(fmstate)，使用其中存储的批量大小
 *      b. 否则直接从服务器/表选项中获取
 *   5. 检查禁用批量操作的条件:
 *      a. 存在RETURNING子句
 *      b. 存在BEFORE/AFTER ROW INSERT触发器
 *      c. 存在WITH CHECK OPTION约束
 *      d. 如果满足任一条件则返回1(禁用批量)
 *   6. 确保批量参数不超过65535限制
 *   7. 返回计算得到的批量大小
 *
 * 注意事项:
 *   - 批量操作可提高性能但受多种限制
 *   - 触发器存在时会禁用批量以保证正确性
 *   - 参数总数限制为65535(uint16最大值)
 */
static int
tdengineGetForeignModifyBatchSize(ResultRelInfo *resultRelInfo)
{
    int batch_size;
    TDengineFdwExecState *fmstate = (TDengineFdwExecState *)resultRelInfo->ri_FdwState;

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    /* 验证函数是否首次调用 */
    Assert(resultRelInfo->ri_BatchSize == 0);

    /* 
     * 检查是否在UPDATE操作的目标关系上执行插入
     * 这种情况应被postgresBeginForeignInsert()拒绝
     */
    Assert(fmstate == NULL || fmstate->aux_fmstate == NULL);

    /* 
     * 确定批量大小:
     * - 有执行状态: 使用其中存储的值
     * - 无执行状态(如EXPLAIN): 直接从选项获取
     */
    if (fmstate)
        batch_size = fmstate->batch_size;
    else
        batch_size = tdengine_get_batch_size_option(resultRelInfo->ri_RelationDesc);

    /*
     * 检查禁用批量操作的条件:
     * 1. 存在RETURNING子句
     * 2. 存在WITH CHECK OPTION约束
     * 3. 存在BEFORE/AFTER ROW INSERT触发器
     */
    if (resultRelInfo->ri_projectReturning != NULL ||
        resultRelInfo->ri_WithCheckOptions != NIL ||
        (resultRelInfo->ri_TrigDesc &&
         (resultRelInfo->ri_TrigDesc->trig_insert_before_row ||
          resultRelInfo->ri_TrigDesc->trig_insert_after_row)))
        return 1;  // 满足任一条件则禁用批量

    /* 
     * 确保批量参数不超过65535限制
     * 计算最大可能的批量大小
     */
    if (fmstate && fmstate->p_nums > 0)
        batch_size = Min(batch_size, 65535 / fmstate->p_nums);

    return batch_size;
}

/*
 * bindJunkColumnValue - 绑定junk列值到TDengine参数
 * 功能: 将计划节点中的junk列(特殊用途列)值绑定到TDengine查询参数
 *
 * 参数:
 *   @fmstate: TDengine执行状态结构体
 *   @slot: 包含实际数据的元组槽
 *   @planSlot: 包含junk列的计划元组槽
 *   @foreignTableId: 外部表OID
 *   @bindnum: 参数绑定起始位置
 *
 * 处理流程:
 *   1. 遍历元组槽的所有属性
 *   2. 检查是否为有效的junk列(通过junk_idx数组)
 *   3. 从planSlot获取junk列值
 *   4. 处理NULL值情况
 *   5. 非NULL值时绑定到TDengine参数
 */
static void
bindJunkColumnValue(TDengineFdwExecState *fmstate,
                    TupleTableSlot *slot,
                    TupleTableSlot *planSlot,
                    Oid foreignTableId,
                    int bindnum)
{
    int i;          // 列索引计数器
    Datum value;    // 列值

    /* 遍历元组槽的所有属性 */
    for (i = 0; i < slot->tts_tupleDescriptor->natts; i++)
    {
        // 获取当前列类型
        Oid type = TupleDescAttr(slot->tts_tupleDescriptor, i)->atttypid;
        bool is_null = false;  // NULL值标志

        /* 检查是否为有效的junk列 */
        if (fmstate->junk_idx[i] == InvalidAttrNumber)
            continue;  // 非junk列则跳过

        /* 从planSlot获取junk列值 */
        value = ExecGetJunkAttribute(planSlot, fmstate->junk_idx[i], &is_null);

        /* 处理NULL值 */
        if (is_null)
        {
            // 设置NULL类型标记
            fmstate->param_tdengine_types[bindnum] = TDENGINE_NULL;
            // 设置NULL值(0)
            fmstate->param_tdengine_values[bindnum].i = 0;
        }
        else
        {
            /* 获取列信息并绑定到TDengine参数 */
            struct TDengineColumnInfo *col = list_nth(fmstate->column_list, (int)bindnum);
            // 设置列类型
            fmstate->param_column_info[bindnum].column_type = col->column_type;
            // 绑定SQL变量到TDengine参数
            tdengine_bind_sql_var(type, bindnum, value, fmstate->param_column_info,
                                 fmstate->param_tdengine_types, fmstate->param_tdengine_values);
        }
        bindnum++;  // 递增绑定位置
    }
}


// #endif

/*
 * tdengineExecForeignDelete - 执行外部表删除操作
 * 功能: 从TDengine外部表中删除一行数据
 *
 * 参数:
 *   @estate: 执行状态，包含查询执行环境信息
 *   @resultRelInfo: 结果关系信息，描述目标表的结构和状态
 *   @slot: 包含待删除数据的元组槽
 *   @planSlot: 计划元组槽(当前未使用，保留参数)
 *
 * 返回值: 返回处理后的元组槽
 *
 * 处理流程:
 *   1. 获取执行状态和表信息
 *   2. 绑定junk列值(用于标识要删除的行)
 *   3. 执行远程删除查询:
 *      a. 使用C++客户端或普通方式连接TDengine
 *      b. 传递参数类型和值
 *   4. 处理查询结果:
 *      a. 错误处理: 复制错误信息并抛出异常
 *      b. 释放查询结果资源
 *   5. 返回元组槽
 *
 * 注意事项:
 *   - 使用volatile修饰ret防止编译器优化
 *   - 错误信息需要复制后再释放原始指针
 *   - 无论操作成功与否都需释放查询结果
 */
static TupleTableSlot *
tdengineExecForeignDelete(EState *estate,
                          ResultRelInfo *resultRelInfo,
                          TupleTableSlot *slot,
                          TupleTableSlot *planSlot)
{
    // 获取执行状态
    TDengineFdwExecState *fmstate = (TDengineFdwExecState *)resultRelInfo->ri_FdwState;
    // 获取表关系
    Relation rel = resultRelInfo->ri_RelationDesc;
    // 获取外部表OID
    Oid foreignTableId = RelationGetRelid(rel);
    // 存储查询返回结果(volatile防止优化)
    struct TDengineQuery_return volatile ret;

    // 记录调试日志
    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    // 绑定junk列值(用于标识要删除的行)
    bindJunkColumnValue(fmstate, slot, planSlot, foreignTableId, 0);
    
    /* 执行查询 */
    // #ifdef CXX_CLIENT
    ret = TDengineQuery(fmstate->query, fmstate->user, fmstate->tdengineFdwOptions,
                        // #else
                        // 普通连接方式(注释掉的备选方案)
                        // #endif
                        fmstate->param_tdengine_types, 
                        fmstate->param_tdengine_values, 
                        fmstate->p_nums);

    // 错误处理
    if (ret.r1 != NULL)
    {
        // 复制错误信息
        char *err = pstrdup(ret.r1);
        // 释放原始错误信息
        free(ret.r1);
        ret.r1 = err;
        // 抛出错误
        elog(ERROR, "tdengine_fdw : %s", err);
    }

    // 释放查询结果
    TDengineFreeResult((TDengineResult *)&ret.r0);
    
    /* 返回元组槽 */
    return slot;
}


/*
 * tdengineEndForeignModify - 结束对外部表的修改操作
 * 功能: 清理INSERT/UPDATE/DELETE操作后的执行状态
 * 
 * 参数:
 *   @estate: 执行状态，包含查询执行环境信息
 *   @resultRelInfo: 结果关系信息，描述目标表的结构和状态
 *
 * 处理流程:
 *   1. 记录调试日志
 *   2. 检查执行状态是否存在
 *   3. 重置游标状态(cursor_exists = false)
 *   4. 重置行索引(rowidx = 0)
 *
 * 注意事项:
 *   - 在修改操作完成后调用
 *   - 确保后续操作不会使用已失效的状态
 *   - 不释放内存，仅重置状态标志
 */
static void
tdengineEndForeignModify(EState *estate,
                         ResultRelInfo *resultRelInfo)
{
    // 获取执行状态
    TDengineFdwExecState *fmstate = (TDengineFdwExecState *)resultRelInfo->ri_FdwState;

    // 记录调试日志
    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    // 检查并重置执行状态
    if (fmstate != NULL)
    {
        fmstate->cursor_exists = false;  // 重置游标状态
        fmstate->rowidx = 0;            // 重置行索引
    }
}


// #if (PG_VERSION_NUM >= 110000)
/*
 * tdengineBeginForeignInsert - 开始分区表插入操作(不支持)
 * 功能: 占位函数，用于处理分区表插入操作的开始阶段
 * 参数:
 *   @mtstate: 修改表操作状态
 *   @resultRelInfo: 结果关系信息
 * 说明:
 *   - 当前TDengine FDW不支持分区表插入操作
 *   - 调用此函数会直接抛出错误
 */
static void
tdengineBeginForeignInsert(ModifyTableState *mtstate,
                           ResultRelInfo *resultRelInfo)
{
    elog(ERROR, "Not support partition insert");
}

/*
 * tdengineEndForeignInsert - 结束分区表插入操作(不支持)
 * 功能: 占位函数，用于处理分区表插入操作的结束阶段
 * 参数:
 *   @estate: 执行状态
 *   @resultRelInfo: 结果关系信息
 * 说明:
 *   - 当前TDengine FDW不支持分区表插入操作
 *   - 调用此函数会直接抛出错误
 */
static void
tdengineEndForeignInsert(EState *estate,
                         ResultRelInfo *resultRelInfo)
{
    elog(ERROR, "Not support partition insert");
}

// #endif

/*
 * tdengineBeginDirectModify - 准备直接修改外部表
 * 功能: 初始化直接修改外部表所需的执行状态和参数
 *
 * 参数:
 *   @node: ForeignScanState节点，包含执行计划信息
 *   @eflags: 执行标志位，用于控制执行行为
 *
 * 处理流程:
 *   1. 获取执行计划(fsplan)和执行状态(estate)
 *   2. 记录调试日志
 *   3. 检查是否为EXPLAIN ONLY模式，是则直接返回
 *   4. 分配并初始化直接修改状态结构(dmstate)
 *   5. 确定远程访问用户身份:
 *      a. 新版本(>=16.0)使用fsplan->checkAsUser或当前用户
 *      b. 旧版本直接使用当前用户
 *   6. 获取范围表条目(rte)和关系描述符(rel)
 *   7. 获取连接选项(tdengineFdwOptions)和用户映射(user)
 *   8. 处理外连接相关字段:
 *      a. 如果是外连接(scanrelid=0)，保存结果关系信息
 *      b. 设置dmstate->rel为NULL以使用fdw_scan_tlist
 *   9. 初始化状态变量(num_tuples=-1表示未设置)
 *   10. 从计划节点获取私有信息:
 *      a. SQL语句(query)
 *      b. 是否有RETURNING子句(has_returning)
 *      c. 检索属性列表(retrieved_attrs)
 */
static void
tdengineBeginDirectModify(ForeignScanState *node, int eflags)
{
    // 获取执行计划节点
    ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;
    // 获取执行状态
    EState *estate = node->ss.ps.state;
    // 直接修改状态结构
    TDengineFdwDirectModifyState *dmstate;
    // 范围表索引
    Index rtindex;
    // 用户ID
    Oid userid;
    // 范围表条目
    RangeTblEntry *rte;
    // 参数数量
    int numParams;
    // #ifdef CXX_CLIENT
    // 外部表信息(用于C++客户端)
    ForeignTable *ftable;
    // #endif
    // 远程表达式列表
    List *remote_exprs;

    // 记录调试日志
    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    /* EXPLAIN ONLY模式直接返回 */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    /* 分配并初始化直接修改状态结构 */
    dmstate = (TDengineFdwDirectModifyState *)palloc0(sizeof(TDengineFdwDirectModifyState));
    node->fdw_state = (void *)dmstate;

    /* 确定远程访问用户身份 */
// #if (PG_VERSION_NUM >= 160000)
//     userid = OidIsValid(fsplan->checkAsUser) ? fsplan->checkAsUser : GetUserId();
//     rtindex = node->resultRelInfo->ri_RangeTableIndex;
// #else
    userid = GetUserId();
// #if (PG_VERSION_NUM < 140000)
//     rtindex = estate->es_result_relation_info->ri_RangeTableIndex;
// #else
    rtindex = node->resultRelInfo->ri_RangeTableIndex;
// #endif
// #endif

    /* 获取范围表条目和关系描述符 */
    rte = exec_rt_fetch(rtindex, estate);
    if (fsplan->scan.scanrelid == 0)
        dmstate->rel = ExecOpenScanRelation(estate, rtindex, eflags);
    else
        dmstate->rel = node->ss.ss_currentRelation;

    /* 获取连接选项和用户映射 */
    dmstate->tdengineFdwOptions = tdengine_get_options(rte->relid, userid);
    // #ifdef CXX_CLIENT
    // if (!dmstate->tdengineFdwOptions->svr_version)
    // 	dmstate->tdengineFdwOptions->svr_version = tdengine_get_version_option(dmstate->tdengineFdwOptions);
    /* 获取用户映射 */
    ftable = GetForeignTable(RelationGetRelid(dmstate->rel));
    dmstate->user = GetUserMapping(userid, ftable->serverid);
    // #endif

    /* 处理外连接相关字段 */
    if (fsplan->scan.scanrelid == 0)
    {
        /* 保存外部表信息 */
        dmstate->resultRel = dmstate->rel;

        /*
         * 设置dmstate->rel为NULL以指示:
         * get_returning_data()和make_tuple_from_result_row()应使用
         * fdw_scan_tlist而非目标关系的元组描述符
         */
        dmstate->rel = NULL;
    }

    /* 初始化状态变量 */
    dmstate->num_tuples = -1; /* -1表示尚未设置 */

    /* 从计划节点获取私有信息 */
    dmstate->query = strVal(list_nth(fsplan->fdw_private,
                                     FdwDirectModifyPrivateUpdateSql));
// #if (PG_VERSION_NUM >= 150000)
    dmstate->has_returning = boolVal(list_nth(fsplan->fdw_private,
                                              FdwDirectModifyPrivateHasReturning));
// #else
//     dmstate->has_returning = intVal(list_nth(fsplan->fdw_private,
//                                              FdwDirectModifyPrivateHasReturning));
// #endif
    dmstate->retrieved_attrs = (List *)list_nth(fsplan->fdw_private,
                                                FdwDirectModifyPrivateRetrievedAttrs);
// #if (PG_VERSION_NUM >= 150000)
    dmstate->set_processed = boolVal(list_nth(fsplan->fdw_private,
                                              FdwDirectModifyPrivateSetProcessed));
// #else
//     dmstate->set_processed = intVal(list_nth(fsplan->fdw_private,
//                                              FdwDirectModifyPrivateSetProcessed));
// #endif

        // 从计划节点获取远程表达式列表
    remote_exprs = (List *)list_nth(fsplan->fdw_private,
                                    FdwDirectModifyRemoteExprs);

    /*
     * 准备远程查询参数处理:
     * 1. 计算参数总数
     * 2. 存储到执行状态
     * 3. 如果有参数则调用prepare_query_params准备参数
     */
    numParams = list_length(fsplan->fdw_exprs);  // 获取参数数量
    dmstate->numParams = numParams;              // 存储参数数量到执行状态
    
    /* 如果有参数需要处理 */
    if (numParams > 0)
        prepare_query_params((PlanState *)node,   // 计划状态节点
                             fsplan->fdw_exprs,   // FDW表达式列表
                             remote_exprs,        // 远程表达式列表
                             rte->relid,          // 外部表OID
                             numParams,           // 参数数量
                             &dmstate->param_flinfo,      // 参数输出函数信息
                             &dmstate->param_exprs,       // 参数表达式列表
                             &dmstate->param_values,      // 参数文本值
                             &dmstate->param_types,       // 参数类型OID
                             &dmstate->param_tdengine_types,  // TDengine参数类型
                             &dmstate->param_tdengine_values, // TDengine参数值
                             &dmstate->param_column_info);    // 列信息
}


/*
 * tdengineIterateDirectModify - 执行直接外部表修改操作
 * 功能: 处理直接修改外部表的迭代操作，包括执行DML语句和更新统计信息
 *
 * 参数:
 *   @node: ForeignScanState节点，包含执行状态和计划信息
 *
 * 返回值:
 *   TupleTableSlot* - 返回清空的元组槽
 *
 * 处理流程:
 *   1. 获取执行状态(dmstate)、执行环境(estate)和元组槽(slot)
 *   2. 记录调试日志
 *   3. 检查是否为首次调用(num_tuples == -1)，是则执行DML语句
 *   4. 验证不支持RETURNING子句(Assert)
 *   5. 更新命令处理计数(es_processed)
 *   6. 更新EXPLAIN ANALYZE的元组计数
 *   7. 返回清空的元组槽
 *
 * 注意事项:
 *   - 首次调用时会执行实际的DML语句
 *   - 不支持RETURNING子句
 *   - 会更新执行统计信息
 *   - 总是返回清空的元组槽
 */
static TupleTableSlot *
tdengineIterateDirectModify(ForeignScanState *node)
{
    // 获取直接修改状态
    TDengineFdwDirectModifyState *dmstate = (TDengineFdwDirectModifyState *)node->fdw_state;
    // 获取执行状态
    EState *estate = node->ss.ps.state;
    // 获取扫描元组槽
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    // 获取性能统计信息
    Instrumentation *instr = node->ss.ps.instrument;

    // 记录调试日志
    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    /* 首次调用时执行DML语句 */
    if (dmstate->num_tuples == -1)
        execute_dml_stmt(node);

    // 验证不支持RETURNING子句
    Assert(!dmstate->has_returning);

    /* 更新命令处理计数 */
    if (dmstate->set_processed)
        estate->es_processed += dmstate->num_tuples;

    /* 更新EXPLAIN ANALYZE统计信息 */
    if (instr)
        instr->tuplecount += dmstate->num_tuples;

    // 返回清空的元组槽
    return ExecClearTuple(slot);
}


/*
 * tdengineEndDirectModify - 结束直接修改外部表的操作
 * 功能: 完成对外部表的直接修改操作(DELETE/UPDATE)后的清理工作
 * 
 * 参数:
 *   @node: ForeignScanState节点，包含执行状态信息
 *
 * 处理流程:
 *   1. 记录调试日志，输出当前函数名
 *   2. 当前版本仅记录日志，无实际清理操作
 *
 * 注意事项:
 *   - 当前实现为空函数，仅用于调试日志
 *   - 未来版本可能需要在此释放资源
 *   - 与tdengineBeginDirectModify配对使用
 */
static void
tdengineEndDirectModify(ForeignScanState *node)
{
    // 记录调试日志，输出当前函数名
    elog(DEBUG1, "tdengine_fdw : %s", __func__);
}


/*
 * 设置数据传输模式
 * 功能: 强制设置GUC参数以确保数据值以远程服务器明确的形式输出
 *
 * 返回值:
 *   int - 必须传递给reset_transmission_modes()的嵌套级别
 *
 * 处理流程:
 *   1. 创建新的GUC嵌套级别
 *   2. 检查并设置日期格式为ISO标准(如果当前不是)
 *   3. 检查并设置间隔样式为Postgres风格(如果当前不是)
 *   4. 检查并设置浮点精度至少为3位(如果当前小于3)
 *   5. 强制设置搜索路径为pg_catalog以确保regproc等常量正确打印
 *
 * 注意事项:
 *   - 这些设置与pg_dump使用的值保持一致
 *   - 使用函数SET选项使设置仅持续到调用reset_transmission_modes()
 *   - 如果中间发生错误，guc.c会自动撤销这些设置
 *   - 每次行处理都需要调用，因为不能保留设置以免影响用户可见的计算
 */
int tdengine_set_transmission_modes(void)
{
    // 创建新的GUC嵌套级别
    int nestlevel = NewGUCNestLevel();

    /* 设置日期格式为ISO标准 */
    if (DateStyle != USE_ISO_DATES)
        (void)set_config_option("datestyle", "ISO",
                                PGC_USERSET, PGC_S_SESSION,
                                GUC_ACTION_SAVE, true, 0, false);

    /* 设置间隔样式为Postgres风格 */
    if (IntervalStyle != INTSTYLE_POSTGRES)
        (void)set_config_option("intervalstyle", "postgres",
                                PGC_USERSET, PGC_S_SESSION,
                                GUC_ACTION_SAVE, true, 0, false);

    /* 设置浮点精度至少为3位 */
    if (extra_float_digits < 3)
        (void)set_config_option("extra_float_digits", "3",
                                PGC_USERSET, PGC_S_SESSION,
                                GUC_ACTION_SAVE, true, 0, false);

    /* 强制设置搜索路径为pg_catalog */
    (void)set_config_option("search_path", "pg_catalog",
                            PGC_USERSET, PGC_S_SESSION,
                            GUC_ACTION_SAVE, true, 0, false);

    return nestlevel;
}

/*
 * 重置数据传输模式
 * 功能: 撤销tdengine_set_transmission_modes()设置的GUC参数
 *
 * 参数:
 *   @nestlevel: 由tdengine_set_transmission_modes()返回的嵌套级别
 *
 * 处理流程:
 *   1. 调用AtEOXact_GUC函数回滚到指定嵌套级别的GUC设置
 *   2. 参数true表示在事务结束时执行重置
 *
 * 注意事项:
 *   - 必须与tdengine_set_transmission_modes()配对使用
 *   - 通常在查询处理完成后调用
 *   - 如果中间发生错误会自动调用
 */
void tdengine_reset_transmission_modes(int nestlevel)
{
    AtEOXact_GUC(true, nestlevel);
}

/*
 * 准备远程查询中使用的参数
 * 功能: 为远程查询中使用的参数准备输出转换和相关信息
 *
 * 参数:
 *   @node: 计划状态节点
 *   @fdw_exprs: FDW表达式列表
 *   @remote_exprs: 远程表达式列表
 *   @foreigntableid: 外部表OID
 *   @numParams: 参数数量
 *   @param_flinfo: 输出参数，存储参数的类型输出函数信息
 *   @param_exprs: 输出参数，存储参数表达式列表
 *   @param_values: 输出参数，存储参数的文本形式值
 *   @param_types: 输出参数，存储参数的类型OID
 *   @param_tdengine_types: 输出参数，存储参数的TDengine类型
 *   @param_tdengine_values: 输出参数，存储参数的TDengine值
 *   @param_column_info: 输出参数，存储参数相关的列信息
 *
 * 处理流程:
 *   1. 验证参数数量必须大于0
 *   2. 分配各种参数信息的内存空间
 *   3. 遍历每个FDW表达式:
 *      a. 获取参数表达式类型
 *      b. 获取类型输出函数信息
 *      c. 如果是时间类型参数:
 *         i. 检查参数是否在远程表达式中
 *         ii. 提取相关列信息
 *         iii. 根据列名确定列类型(时间键/标签键/字段键)
 *   4. 初始化参数表达式列表
 *   5. 分配参数值缓冲区
 */
static void
prepare_query_params(PlanState *node,
                     List *fdw_exprs,
                     List *remote_exprs,
                     Oid foreigntableid,
                     int numParams,
                     FmgrInfo **param_flinfo,
                     List **param_exprs,
                     const char ***param_values,
                     Oid **param_types,
                     TDengineType **param_tdengine_types,
                     TDengineValue **param_tdengine_values,
                     TDengineColumnInfo **param_column_info)
{
    int i;
    ListCell *lc;

    /* 验证参数数量必须大于0 */
    Assert(numParams > 0);

    /* 分配各种参数信息的内存空间 */
    *param_flinfo = (FmgrInfo *)palloc0(sizeof(FmgrInfo) * numParams);
    *param_types = (Oid *)palloc0(sizeof(Oid) * numParams);
    *param_tdengine_types = (TDengineType *)palloc0(sizeof(TDengineType) * numParams);
    *param_tdengine_values = (TDengineValue *)palloc0(sizeof(TDengineValue) * numParams);
    *param_column_info = (TDengineColumnInfo *)palloc0(sizeof(TDengineColumnInfo) * numParams);

    /* 遍历每个FDW表达式 */
    i = 0;
    foreach (lc, fdw_exprs)
    {
        Node *param_expr = (Node *)lfirst(lc);
        Oid typefnoid;
        bool isvarlena;

        /* 获取参数表达式类型 */
        (*param_types)[i] = exprType(param_expr);
        /* 获取类型输出函数信息 */
        getTypeOutputInfo(exprType(param_expr), &typefnoid, &isvarlena);
        fmgr_info(typefnoid, &(*param_flinfo)[i]);

        /* 如果是时间类型参数 */
        if (TDENGINE_IS_TIME_TYPE((*param_types)[i]))
        {
            ListCell *expr_cell;

            /* 检查参数是否在远程表达式中 */
            foreach (expr_cell, remote_exprs)
            {
                Node *qual = (Node *)lfirst(expr_cell);

                if (tdengine_param_belong_to_qual(qual, param_expr))
                {
                    Var *col;
                    char *column_name;
                    List *column_list = pull_var_clause(qual, PVC_RECURSE_PLACEHOLDERS);

                    /* 提取相关列信息 */
                    col = linitial(column_list);
                    column_name = tdengine_get_column_name(foreigntableid, col->varattno);

                    /* 根据列名确定列类型 */
                    if (TDENGINE_IS_TIME_COLUMN(column_name))
                        (*param_column_info)[i].column_type = TDENGINE_TIME_KEY;
                    else if (tdengine_is_tag_key(column_name, foreigntableid))
                        (*param_column_info)[i].column_type = TDENGINE_TAG_KEY;
                    else
                        (*param_column_info)[i].column_type = TDENGINE_FIELD_KEY;
                }
            }
        }
        i++;
    }

    /* 初始化参数表达式列表 */
    *param_exprs = (List *)ExecInitExprList(fdw_exprs, node);
    /* 分配参数值缓冲区 */
    *param_values = (const char **)palloc0(numParams * sizeof(char *));
}

/*
 * 检查参数是否属于条件表达式
 * 功能: 递归检查参数节点是否出现在条件表达式树中
 *
 * 参数:
 *   @qual: 条件表达式树节点
 *   @param: 要检查的参数节点
 *
 * 处理流程:
 *   1. 检查条件表达式是否为空，为空直接返回false
 *   2. 检查当前节点是否与参数节点相等
 *   3. 递归检查表达式树的所有子节点
 */
static bool tdengine_param_belong_to_qual(Node *qual, Node *param)
{
    /* 空条件直接返回false */
    if (qual == NULL)
        return false;

    /* 当前节点与参数匹配则返回true */
    if (equal(qual, param))
        return true;

    /* 递归检查表达式树的所有子节点 */
    return expression_tree_walker(qual, tdengine_param_belong_to_qual, param);
}

/*
 * Construct array of query parameter values and bind parameters
 *
 */

static void
process_query_params(ExprContext *econtext,
                     FmgrInfo *param_flinfo,
                     List *param_exprs,
                     const char **param_values,
                     Oid *param_types,
                     TDengineType *param_tdengine_types,
                     TDengineValue *param_tdengine_values,
                     TDengineColumnInfo *param_column_info)
{
    int nestlevel;
    int i;
    ListCell *lc;

    nestlevel = tdengine_set_transmission_modes();

    i = 0;
    foreach (lc, param_exprs)
    {
        ExprState *expr_state = (ExprState *)lfirst(lc);
        Datum expr_value;
        bool isNull;

        /* Evaluate the parameter expression */
        expr_value = ExecEvalExpr(expr_state, econtext, &isNull);

        /*
         * Get string sentation of each parameter value by invoking
         * type-specific output function, unless the value is null.
         */
        if (isNull)
        {
            elog(ERROR, "tdengine_fdw : cannot bind NULL due to TDengine does not support to filter NULL value");
        }
        else
        {
            /* Bind parameters */
            tdengine_bind_sql_var(param_types[i], i, expr_value, param_column_info,
                                  param_tdengine_types, param_tdengine_values);
            param_values[i] = OutputFunctionCall(&param_flinfo[i], expr_value);
        }
        i++;
    }
    tdengine_reset_transmission_modes(nestlevel);
}

/*
 * create_cursor - 为外部扫描创建游标并处理查询参数
 * 功能: 为给定的ForeignScanState节点创建游标，处理查询参数并准备执行
 *
 * 参数:
 *   @node: ForeignScanState节点，包含执行状态和计划信息
 *
 * 处理流程:
 *   1. 获取执行状态(festate)和表达式上下文(econtext)
 *   2. 检查是否有查询参数需要处理(numParams > 0)
 *   3. 如果有参数:
 *      a. 切换到每元组内存上下文(ecxt_per_tuple_memory)避免内存泄漏
 *      b. 分配参数存储空间
 *      c. 调用process_query_params处理参数转换和绑定
 *      d. 切换回原始内存上下文
 *   4. 标记游标已创建(cursor_exists = true)
 *
 * 注意事项:
 *   - 使用每元组内存上下文处理参数以避免重复扫描时的内存泄漏
 *   - 参数处理包括类型转换和值绑定
 *   - 游标创建后需要后续操作来实际执行查询
 */
static void
create_cursor(ForeignScanState *node)
{
    // 获取执行状态
    TDengineFdwExecState *festate = (TDengineFdwExecState *)node->fdw_state;
    // 获取表达式上下文
    ExprContext *econtext = node->ss.ps.ps_ExprContext;
    // 获取参数数量
    int numParams = festate->numParams;
    // 获取参数值数组
    const char **values = festate->param_values;

    /* 如果有查询参数需要处理 */
    if (numParams > 0)
    {
        MemoryContext oldcontext;

        /* 切换到每元组内存上下文 */
        oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
        // 分配参数存储空间
        festate->params = palloc(numParams);
        // 处理查询参数(类型转换和绑定)
        process_query_params(econtext,
                             festate->param_flinfo,
                             festate->param_exprs,
                             values,
                             festate->param_types,
                             festate->param_tdengine_types,
                             festate->param_tdengine_values,
                             festate->param_column_info);

        /* 切换回原始内存上下文 */
        MemoryContextSwitchTo(oldcontext);
    }

    /* 标记游标已创建 */
    festate->cursor_exists = true;
}

/*
 * execute_dml_stmt - 执行直接UPDATE/DELETE语句
 * 功能: 处理直接修改外部表的DML语句执行，包括参数准备和结果处理
 *
 * 参数:
 *   @node: ForeignScanState节点，包含执行状态和计划信息
 *
 * 处理流程:
 *   1. 获取执行状态(dmstate)、表达式上下文(econtext)和参数信息
 *   2. 如果有参数(numParams > 0):
 *      a. 切换到每元组内存上下文处理参数
 *      b. 分配参数存储空间
 *      c. 调用process_query_params处理参数转换和绑定
 *      d. 切换回原始内存上下文
 *   3. 执行远程查询:
 *      a. 使用C++客户端或普通方式连接TDengine
 *      b. 传递参数类型和值
 *   4. 处理查询结果:
 *      a. 错误处理: 复制错误信息并抛出异常
 *      b. 释放查询结果资源
 *   5. 设置处理元组数为0(TDengine DELETE不返回行)
 *
 * 注意事项:
 *   - 使用volatile修饰ret防止编译器优化
 *   - 错误信息需要复制后再释放原始指针
 *   - 无论操作成功与否都需释放查询结果
 *   - TDengine的DELETE操作不返回受影响行数，默认设为0
 */
static void
execute_dml_stmt(ForeignScanState *node)
{
    // 获取直接修改状态
    TDengineFdwDirectModifyState *dmstate = (TDengineFdwDirectModifyState *)node->fdw_state;
    // 获取表达式上下文
    ExprContext *econtext = node->ss.ps.ps_ExprContext;
    // 获取参数数量
    int numParams = dmstate->numParams;
    // 获取参数值数组
    const char **values = dmstate->param_values;
    // 存储查询返回结果(volatile防止优化)
    struct TDengineQuery_return volatile ret;

    /* 处理查询参数 */
    if (numParams > 0)
    {
        MemoryContext oldcontext;

        // 切换到每元组内存上下文
        oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
        // 分配参数存储空间
        dmstate->params = palloc(numParams);
        // 处理查询参数(类型转换和绑定)
        process_query_params(econtext,
                            dmstate->param_flinfo,
                            dmstate->param_exprs,
                            values,
                            dmstate->param_types,
                            dmstate->param_tdengine_types,
                            dmstate->param_tdengine_values,
                            dmstate->param_column_info);

        // 切换回原始内存上下文
        MemoryContextSwitchTo(oldcontext);
    }

    /* 执行查询 */
    // #ifdef CXX_CLIENT
    ret = TDengineQuery(dmstate->query, dmstate->user, dmstate->tdengineFdwOptions,
    // #else
    // 普通连接方式(注释掉的备选方案)
    // #endif
                        dmstate->param_tdengine_types, 
                        dmstate->param_tdengine_values, 
                        dmstate->numParams);

    // 错误处理
    if (ret.r1 != NULL)
    {
        // 复制错误信息
        char *err = pstrdup(ret.r1);
        // 释放原始错误信息
        free(ret.r1);
        ret.r1 = err;
        // 抛出错误
        elog(ERROR, "tdengine_fdw : %s", err);
    }

    // 释放查询结果
    TDengineFreeResult((TDengineResult *)&ret.r0);

    /* 
     * TDengine的DELETE操作不返回受影响行数
     * 因此默认设置为0 
     */
    dmstate->num_tuples = 0;
}


/*
 * execute_foreign_insert_modify - 执行外部表插入/修改操作
 * 功能: 处理批量插入操作，准备参数并调用TDengine接口执行插入
 *
 * 参数:
 *   @estate: 执行状态
 *   @resultRelInfo: 结果关系信息
 *   @slots: 包含待插入数据的元组槽数组
 *   @planSlots: 计划元组槽数组(未使用)
 *   @numSlots: 要处理的元组数量
 *
 * 返回值: 返回处理后的元组槽数组
 *
 * 处理流程:
 *   1. 获取执行状态和表信息
 *   2. 切换到临时内存上下文处理参数
 *   3. 重新分配参数存储空间以适应批量操作
 *   4. 遍历每个元组槽，绑定参数值:
 *      a. 处理非空约束检查
 *      b. 特殊处理时间列(time和time_text)
 *      c. 绑定普通列值
 *   5. 调用TDengine接口执行批量插入
 *   6. 清理临时内存并返回结果
 */
static TupleTableSlot **
execute_foreign_insert_modify(EState *estate,
                              ResultRelInfo *resultRelInfo,
                              TupleTableSlot **slots,
                              TupleTableSlot **planSlots,
                              int numSlots)
{
    // 获取执行状态
    TDengineFdwExecState *fmstate = (TDengineFdwExecState *)resultRelInfo->ri_FdwState;
    uint32_t bindnum = 0; // 参数绑定计数器
    char *ret;            // 操作结果
    int i;                // 循环计数器
    // 获取表信息和描述符
    Relation rel = resultRelInfo->ri_RelationDesc;
    TupleDesc tupdesc = RelationGetDescr(rel);
    char *tablename = tdengine_get_table_name(rel); // 获取表名
    bool time_had_value = false;                    // 时间列是否有值标志
    int bind_num_time_column = 0;                   // 时间列绑定位置
    MemoryContext oldcontext;                       // 旧内存上下文

    // 切换到临时内存上下文处理参数
    oldcontext = MemoryContextSwitchTo(fmstate->temp_cxt);

    // 重新分配参数存储空间以适应批量操作
    fmstate->param_tdengine_types = (TDengineType *)repalloc(fmstate->param_tdengine_types, sizeof(TDengineType) * fmstate->p_nums * numSlots);
    fmstate->param_tdengine_values = (TDengineValue *)repalloc(fmstate->param_tdengine_values, sizeof(TDengineValue) * fmstate->p_nums * numSlots);
    fmstate->param_column_info = (TDengineColumnInfo *)repalloc(fmstate->param_column_info, sizeof(TDengineColumnInfo) * fmstate->p_nums * numSlots);

    /* 从元组槽获取参数并绑定 */
    if (slots != NULL && fmstate->retrieved_attrs != NIL)
    {
        int nestlevel;
        ListCell *lc;

        // 设置数据传输模式
        nestlevel = tdengine_set_transmission_modes();

        // 遍历每个元组槽
        for (i = 0; i < numSlots; i++)
        {
            /* 绑定值到参数 */
            foreach (lc, fmstate->retrieved_attrs)
            {
                int attnum = lfirst_int(lc) - 1;                                           // 属性编号
                Oid type = TupleDescAttr(slots[i]->tts_tupleDescriptor, attnum)->atttypid; // 属性类型
                Datum value;                                                               // 属性值
                bool is_null;                                                              // 是否为空标志
                // 获取列信息
                struct TDengineColumnInfo *col = list_nth(fmstate->column_list, (int)bindnum % fmstate->p_nums);

                // 设置列名和类型
                fmstate->param_column_info[bindnum].column_name = col->column_name;
                fmstate->param_column_info[bindnum].column_type = col->column_type;
                // 获取属性值
                value = slot_getattr(slots[i], attnum + 1, &is_null);

                /* 检查值是否为空 */
                if (is_null)
                {
                    // 检查非空约束
                    if (TupleDescAttr(tupdesc, attnum)->attnotnull)
                        elog(ERROR, "tdengine_fdw : null value in column \"%s\" of relation \"%s\" violates not-null constraint",
                             col->column_name, tablename);
                    // 设置空值标记
                    fmstate->param_tdengine_types[bindnum] = TDENGINE_NULL;
                    fmstate->param_tdengine_values[bindnum].i = 0;
                }
                else
                {
                    // 特殊处理时间列
                    if (TDENGINE_IS_TIME_COLUMN(col->column_name))
                    {
                        /* 时间列处理逻辑 */
                        if (!time_had_value)
                        {
                            // 第一次遇到时间列，绑定值
                            tdengine_bind_sql_var(type, bindnum, value, fmstate->param_column_info,
                                                  fmstate->param_tdengine_types, fmstate->param_tdengine_values);
                            bind_num_time_column = bindnum;
                            time_had_value = true;
                        }
                        else
                        {
                            // 重复时间列警告
                            elog(WARNING, "Inserting value has both \'time_text\' and \'time\' columns specified. The \'time\' will be ignored.");
                            if (strcmp(col->column_name, TDENGINE_TIME_TEXT_COLUMN) == 0)
                            {
                                // 绑定time_text列值
                                tdengine_bind_sql_var(type, bind_num_time_column, value, fmstate->param_column_info,
                                                      fmstate->param_tdengine_types, fmstate->param_tdengine_values);
                            }
                            // 忽略重复时间列
                            fmstate->param_tdengine_types[bindnum] = TDENGINE_NULL;
                            fmstate->param_tdengine_values[bindnum].i = 0;
                        }
                    }
                    else
                    {
                        // 绑定普通列值
                        tdengine_bind_sql_var(type, bindnum, value, fmstate->param_column_info,
                                              fmstate->param_tdengine_types, fmstate->param_tdengine_values);
                    }
                }
                bindnum++; // 递增绑定计数器
            }
        }
        // 重置数据传输模式
        tdengine_reset_transmission_modes(nestlevel);
    }

    // 验证绑定参数数量
    Assert(bindnum == fmstate->p_nums * numSlots);

    /* 执行插入操作 */
    ret = TDengineInsert(tablename, fmstate->user, fmstate->tdengineFdwOptions,
                         fmstate->param_column_info, fmstate->param_tdengine_types, fmstate->param_tdengine_values, fmstate->p_nums, numSlots);
    // 检查插入结果
    if (ret != NULL)
        elog(ERROR, "tdengine_fdw : %s", ret);

    // 恢复原始内存上下文并清理临时内存
    MemoryContextSwitchTo(oldcontext);
    MemoryContextReset(fmstate->temp_cxt);

    return slots;
}

// #if (PG_VERSION_NUM >= 140000)
/*
 * tdengine_get_batch_size_option - 获取外部表的批量操作大小
 * 功能: 从表或服务器选项中获取批量操作大小配置
 * 参数:
 *   @rel: 关系描述符，包含表信息
 * 返回值: 配置的批量操作大小(默认返回1表示不启用批量操作)
 *
 * 处理流程:
 *   1. 初始化默认批量大小为1(不启用批量操作)
 *   2. 获取表和服务器的选项配置
 *   3. 合并表选项和服务器选项(表选项优先)
 *   4. 遍历选项查找batch_size配置
 *   5. 解析并返回配置的批量大小
 */
static int
tdengine_get_batch_size_option(Relation rel)
{
    // 获取表OID
    Oid foreigntableid = RelationGetRelid(rel);
    // 初始化选项列表
    List *options = NIL;
    // 列表迭代器
    ListCell *lc;
    // 外部表信息
    ForeignTable *table;
    // 外部服务器信息
    ForeignServer *server;

    /* 默认批量大小为1(不启用批量操作) */
    int batch_size = 1;

    /*
     * 加载表和服务器选项:
     * 表选项优先于服务器选项
     */
    table = GetForeignTable(foreigntableid);
    server = GetForeignServer(table->serverid);

    /* 合并表选项和服务器选项 */
    options = list_concat(options, table->options);
    options = list_concat(options, server->options);

    /* 遍历选项查找batch_size配置 */
    foreach (lc, options)
    {
        // 获取当前选项定义
        DefElem *def = (DefElem *)lfirst(lc);

        /* 检查是否为batch_size选项 */
        if (strcmp(def->defname, "batch_size") == 0)
        {
            /* 解析选项值为整数 */
            (void)parse_int(defGetString(def), &batch_size, 0, NULL);
            break;
        }
    }

    return batch_size;
}
// #endif