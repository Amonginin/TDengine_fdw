#include "postgres.h"

#include "tdengine_fdw.h"

#include <stdio.h>

#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "nodes/makefuncs.h"
#include "storage/ipc.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/numeric.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/hsearch.h"
#include "utils/syscache.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/timestamp.h"
#include "utils/formatting.h"
#include "utils/memutils.h"
#include "utils/guc.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "access/reloptions.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "optimizer/cost.h"
#include "optimizer/paths.h"
#include "optimizer/prep.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/plancat.h"
#include "optimizer/planmain.h"
#include "parser/parsetree.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "postmaster/syslogger.h"
#include "storage/fd.h"
#include "catalog/pg_type.h"

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
    foreach(lc, fpinfo->local_conds)
    {
        // 从列表中取出当前的 RestrictInfo 结构体
        // lfirst 是一个宏定义，用于获取列表（List 类型）中的第一个元素。
        // 这个宏定义在 postgres.h 文件中
        RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

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
    // 用于存储外部表的相关信息
    TDengineFdwRelationInfo *fpinfo;
    // 用于存储 TDengine 的选项信息
    tdengine_opt *options;
    // 用于遍历列表
    ListCell   *lc;
    // 用于存储用户 ID
    Oid         userid;

    // 从规划器的范围表中获取当前外部表的范围表条目
    RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);

    // 输出调试信息，显示当前正在执行的函数名
    elog(DEBUG1, "tdengine_fdw : %s", __func__);
    // 为 TDengineFdwRelationInfo 结构体分配内存并初始化为 0
    fpinfo = (TDengineFdwRelationInfo *) palloc0(sizeof(TDengineFdwRelationInfo));
    // 将分配的结构体指针存储在 baserel 的 fdw_private 字段中
    baserel->fdw_private = (void *) fpinfo;

    // 如果范围表条目中的 checkAsUser 有效，则使用该用户 ID，否则获取当前用户的 ID
    userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();


    // 从外部表和用户 ID 获取 TDengine 的选项信息
    options = tdengine_get_options(foreigntableid, userid);

    // 获取外部表的无模式信息，并存储在 fpinfo 的 slinfo 字段中
    tdengine_get_schemaless_info(&(fpinfo->slinfo), options->schemaless, foreigntableid);

    // 基础外部表总是需要下推
    fpinfo->pushdown_safe = true;
    // 从系统目录中查找外部表的信息，并存储在 fpinfo 的 table 字段中
    fpinfo->table = GetForeignTable(foreigntableid);
    // 从系统目录中查找外部服务器的信息，并存储在 fpinfo 的 server 字段中
    fpinfo->server = GetForeignServer(fpinfo->table->serverid);

    /*
     * 识别哪些 baserestrictinfo 子句可以发送到远程服务器，哪些不能。
     */
    // 遍历 baserel 中的 baserestrictinfo 列表
    foreach(lc, baserel->baserestrictinfo)
    {
        // 获取当前列表项，并将其转换为 RestrictInfo 结构体指针
        RestrictInfo *ri = (RestrictInfo *) lfirst(lc);

        // 检查当前子句是否可以作为远程表达式发送到远程服务器
        if (tdengine_is_foreign_expr(root, baserel, ri->clause, false))
            // 如果可以，则将该子句添加到 fpinfo 的 remote_conds 列表中
            fpinfo->remote_conds = lappend(fpinfo->remote_conds, ri);
        else
            // 如果不可以，则将该子句添加到 fpinfo 的 local_conds 列表中
            fpinfo->local_conds = lappend(fpinfo->local_conds, ri);
    }

    /*
     * 识别哪些属性需要从远程服务器检索。
     */
    // 从目标表达式中提取需要从远程服务器检索的属性编号，并存储在 fpinfo 的 attrs_used 字段中
    pull_varattnos((Node *) baserel->reltarget->exprs, baserel->relid, &fpinfo->attrs_used);

    // 遍历 fpinfo 的 local_conds 列表
    foreach(lc, fpinfo->local_conds)
    {
        // 获取当前列表项，并将其转换为 RestrictInfo 结构体指针
        RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

        // 从本地条件子句中提取需要从远程服务器检索的属性编号，并存储在 fpinfo 的 attrs_used 字段中
        pull_varattnos((Node *) rinfo->clause, baserel->relid, &fpinfo->attrs_used);
    }

    /*
     * 计算本地条件的选择性和成本，这样我们就不必为每个路径再次计算。
     * 对于这些条件，我们所能做的最好的事情就是根据本地统计信息估计选择性。
     */
    // 计算本地条件的选择性，并存储在 fpinfo 的 local_conds_sel 字段中
    fpinfo->local_conds_sel = clauselist_selectivity(root,
                                                     fpinfo->local_conds,
                                                     baserel->relid,
                                                     JOIN_INNER,
                                                     NULL);

    /*
     * 将缓存的关系成本设置为负值，以便我们可以在调用 estimate_path_cost_size() 函数时
     * （通常是第一次调用）检测到何时设置了合理的成本。
     */
    // 将缓存的启动成本设置为 -1
    fpinfo->rel_startup_cost = -1;
    // 将缓存的总成本设置为 -1
    fpinfo->rel_total_cost = -1;

    /*
     * 如果表或服务器配置为使用远程估计，则连接到外部服务器并执行 EXPLAIN 以估计
     * 由限制子句选择的行数以及平均行宽度。否则，使用我们本地的统计信息进行估计，
     * 方式与普通表类似。
     */
    // 检查是否使用远程估计
    if (fpinfo->use_remote_estimate)
    {
        // 如果使用远程估计，但当前不支持，则抛出错误信息
        ereport(ERROR, (errmsg("Remote estimation is unsupported")));
    }
    else
    {
        /*
         * 如果不允许询问远程服务器，可以使用类似于 plancat.c 中处理空关系的方法：
         * 使用最小大小估计为 10 页，并除以基于列数据类型的宽度估计，
         * 以获得相应的元组数。
         */

        /*
         * 如果外部表从未进行过 ANALYZE 操作，则其 reltuples 将小于 0，表示 "未知"
         */
        // 检查外部表的元组数是否小于 0
        if (baserel->tuples < 0)

        {
            // 将外部表的页数设置为 10
            baserel->pages = 10;
            // 根据页大小和元组头部大小计算外部表的元组数
            baserel->tuples =
                (10 * BLCKSZ) / (baserel->reltarget->width +
                                 MAXALIGN(SizeofHeapTupleHeader));
        }

        // 使用本地统计信息尽可能准确地估计 baserel 的大小
        set_baserel_size_estimates(root, baserel);

        // 填充基本的成本估计，以便后续使用
        estimate_path_cost_size(root, baserel, NIL, NIL,
                                &fpinfo->rows, &fpinfo->width,
                                &fpinfo->startup_cost, &fpinfo->total_cost);
    }

    /*
     * 在构造 fpinfo 时设置关系的名称。它将用于在 EXPLAIN 输出中构建描述连接关系的字符串。
     * 无法知道是否指定了 VERBOSE 选项，因此总是对外部表名进行模式限定。
     */
    // 将 baserel 的 relid 转换为字符串，并存储在 fpinfo 的 relation_name 字段中
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
    Cost        startup_cost = 10;
    // 总成本初始化为表的行数加上启动成本
    Cost        total_cost = baserel->rows + startup_cost;

    // 输出调试信息，显示当前函数名
    elog(DEBUG1, "tdengine_fdw : %s", __func__);
    /* 估算成本 */
    // 重新设置总成本为表的行数
    total_cost = baserel->rows;

    /* 创建一个 ForeignPath 节点并将其作为唯一可能的路径添加 */
    add_path(baserel, (Path *)
             // 创建一个外部扫描路径
             create_foreignscan_path(root, baserel,
                                     NULL,    /* 默认的路径目标 */
                                     baserel->rows,
                                     startup_cost,
                                     total_cost,
                                     NIL,    /* 没有路径键 */
// #if (PG_VERSION_NUM >= 120000)
                                     baserel->lateral_relids,
// #else
//                                      NULL,    /* 也没有外部关系 */
// #endif
                                     NULL,    /* 没有额外的计划 */
// #if PG_VERSION_NUM >= 170000
//                                      NIL, /* 没有 fdw_restrictinfo 列表 */
// #endif
                                     NULL));    /* 没有 fdw_private 数据 */
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
    TDengineFdwRelationInfo *fpinfo = (TDengineFdwRelationInfo *) baserel->fdw_private;
    // 获取扫描关系的ID
    Index		scan_relid = baserel->relid;
    // 传递给执行器的私有数据列表
    List	   *fdw_private = NULL;
    // 本地执行的表达式列表
    List	   *local_exprs = NULL;
    // 远程执行的表达式列表
    List	   *remote_exprs = NULL;
    // 参数列表
    List	   *params_list = NULL;
    // 传递给外部服务器的目标列表
    List	   *fdw_scan_tlist = NIL;
    // 远程条件列表
    List	   *remote_conds = NIL;

    StringInfoData sql;
    // 从远程服务器检索的属性列表
    List	   *retrieved_attrs;
    // 遍历列表的迭代器
    ListCell   *lc;
    // 需要重新检查的条件列表
    List	   *fdw_recheck_quals = NIL;
    // 表示是否为 FOR UPDATE 操作的标志
    int			for_update;
    // 表示查询是否有 LIMIT 子句的标志
    bool		has_limit = false;

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
        foreach(lc, scan_clauses)
        {
            // 获取当前的限制信息节点
            RestrictInfo *rinfo = (RestrictInfo *) lfirst(lc);

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
            foreach(lc, tlist)
            {
                // 获取当前的目标项
                TargetEntry *tle = lfirst_node(TargetEntry, lc);

                /*
                 * 从 FieldSelect 子句中提取函数，并添加到 fdw_scan_tlist 中，
                 * 以便仅下推函数部分
                 */
                if (fpinfo->is_tlist_func_pushdown == true && IsA((Node *) tle->expr, FieldSelect))
                {
                    // 将提取的函数添加到 fdw_scan_tlist 中
                    fdw_scan_tlist = add_to_flat_tlist(fdw_scan_tlist,
                                                       tdengine_pull_func_clause((Node *) tle->expr));
                }
                else
                {
                    // 否则将目标项添加到 fdw_scan_tlist 中
                    fdw_scan_tlist = lappend(fdw_scan_tlist, tle);
                }
            }

            // 遍历本地条件列表
            foreach(lc, fpinfo->local_conds)
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
                    varlist = pull_var_clause((Node *) rinfo->clause,
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
            foreach(lc, local_exprs)
            {
                // 将外部计划转换为连接计划
                Join	   *join_plan = (Join *) outer_plan;
                // 获取当前的条件子句
                Node	   *qual = lfirst(lc);

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

        ofpinfo = (TDengineFdwRelationInfo *) fpinfo->outerrel->fdw_private;
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
 * 初始化对数据库的访问
 */
static void
tdengineBeginForeignScan(ForeignScanState *node, int eflags)
{
    TDengineFdwExecState *festate = NULL;
    EState *estate = node->ss.ps.state;
    ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
    RangeTblEntry *rte;
    int numParams;
    int rtindex;
    bool schemaless;
    Oid userid;
// #ifdef CXX_CLIENT
    ForeignTable *ftable;
// #endif
    List *remote_exprs;

    elog(DEBUG1, "tdengine_fdw : %s", __func__);

    /*
     * 将私有状态保存在 node->fdw_state 中。
     */
    festate = (TDengineFdwExecState *) palloc0(sizeof(TDengineFdwExecState));
    node->fdw_state = (void *) festate;
    festate->rowidx = 0;

    /* 保存我们已经有的状态信息 */
    festate->query = strVal(list_nth(fsplan->fdw_private, 0));
    festate->retrieved_attrs = list_nth(fsplan->fdw_private, 1);
    festate->for_update = intVal(list_nth(fsplan->fdw_private, 2)) ? true : false;
    festate->tlist = (List *) list_nth(fsplan->fdw_private, 3);
    festate->is_tlist_func_pushdown = intVal(list_nth(fsplan->fdw_private, 4)) ? true : false;
    schemaless = intVal(list_nth(fsplan->fdw_private, 5)) ? true : false;
    remote_exprs = (List *) list_nth(fsplan->fdw_private, 6);

    festate->cursor_exists = false;

    if (fsplan->scan.scanrelid > 0)
        rtindex = fsplan->scan.scanrelid;
    else
// #if PG_VERSION_NUM < 160000
        rtindex = bms_next_member(fsplan->fs_relids, -1);
// #else
//         rtindex = bms_next_member(fsplan->fs_base_relids, -1);
// #endif
    rte = exec_rt_fetch(rtindex, estate);

// #if PG_VERSION_NUM >= 160000
//     /*
//      * 确定以哪个用户身份进行远程访问。这应该与 ExecCheckPermissions() 的操作一致。
//      */
//     userid = OidIsValid(fsplan->checkAsUser) ? fsplan->checkAsUser : GetUserId();
// #else
    userid = rte->checkAsUser ? rte->checkAsUser : GetUserId();
// #endif

    /* 获取选项 */
    festate->tdengineFdwOptions = tdengine_get_options(rte->relid, userid);
    if (!festate->tdengineFdwOptions->svr_version)
        festate->tdengineFdwOptions->svr_version = tdengine_get_version_option(festate->tdengineFdwOptions);
    /* 获取用户映射 */
    ftable = GetForeignTable(rte->relid);
    festate->user = GetUserMapping(userid, ftable->serverid);

    tdengine_get_schemaless_info(&(festate->slinfo), schemaless, rte->relid);

    /* 为远程查询中使用的参数的输出转换做准备。 */
    numParams = list_length(fsplan->fdw_exprs);
    festate->numParams = numParams;
    if (numParams > 0)
    {
        prepare_query_params((PlanState *) node,
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
    TDengineFdwExecState *festate = (TDengineFdwExecState *) node->fdw_state;
    // 获取元组槽
    TupleTableSlot *tupleSlot = node->ss.ss_ScanTupleSlot;
    // 获取执行状态
    EState	   *estate = node->ss.ps.state;
    // 获取元组描述符
    TupleDesc	tupleDescriptor = tupleSlot->tts_tupleDescriptor;
    // 获取 TDengine 选项
    tdengine_opt *options;
    // 存储查询返回结果
    struct TDengineQuery_return volatile ret;
    // 存储查询结果
    struct TDengineResult volatile *result = NULL;
    // 获取外键扫描计划
    ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
    // 范围表条目
    RangeTblEntry *rte;
    // 范围表索引
    int			rtindex;
    // 是否为聚合操作
    bool		is_agg;

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
            // 使用 C++ 客户端执行查询
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
                char	   *err = pstrdup(ret.r1);
                // 释放原错误信息
                free(ret.r1);
                ret.r1 = err;
                // 打印错误信息
                elog(ERROR, "tdengine_fdw : %s", err);
            }

            result = ret.r0;
            festate->temp_result = (void *) result;

            // 获取结果集的行数
            festate->row_nums = result->nrow;
            // 打印查询信息
            elog(DEBUG1, "tdengine_fdw : query: %s", festate->query);

            // 切换回旧的内存上下文
            MemoryContextSwitchTo(oldcontext);
            // 释放结果集
            TDengineFreeResult((TDengineResult *) result);
        }
        // 异常处理捕获部分
        PG_CATCH();
        {
            if (ret.r1 == NULL)
            {
                // 释放结果集
                TDengineFreeResult((TDengineResult *) result);
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

        if (festate->rowidx == (festate->row_nums-1))
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

    TDengineFdwExecState *festate = (TDengineFdwExecState *) node->fdw_state;

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
	TDengineFdwExecState *festate = (TDengineFdwExecState *) node->fdw_state;

	elog(DEBUG1, "tdengine_fdw : %s", __func__);

	if (festate != NULL)
	{
		festate->cursor_exists = false;
		festate->rowidx = 0;
	}
}













