#include "slvars.c"
#include "deparse.c"
#include "tdengine_fdw.h"

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
 * Library load-time initialization, sets on_proc_exit() callback for
 * backend shutdown.
 */
void _PG_init(void)
{
    on_proc_exit(&tdengine_fdw_exit, PointerGetDatum(NULL));
}
/*
 * tdengine_fdw_exit: Exit callback function.
 */
static void
tdengine_fdw_exit(int code, Datum arg)
{
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
         * 如果不允许咨询远程服务器，我们能做的不多，但我们可以使用类似于 plancat.c 中
         * 处理空关系的方法：使用最小大小估计为 10 页，并除以基于列数据类型的宽度估计，
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
     * 我们无法知道是否指定了 VERBOSE 选项，因此总是对外部表名进行模式限定。
     */
    // 将 baserel 的 relid 转换为字符串，并存储在 fpinfo 的 relation_name 字段中
    fpinfo->relation_name = psprintf("%u", baserel->relid);
}