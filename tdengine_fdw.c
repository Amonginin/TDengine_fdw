#include "tdengine_fdw.h"

/**
 * 外部定义的 PG 初始化函数，模块加载时会被调用
 * 执行一些初始化操作，例如设置回调函数、注册全局变量等
 * PGDLLEXPORT 是 PostgreSQL 特定的宏，
 * 用于指定函数的导出属性，确保该函数可以被动态加载器正确识别和加载
 */
extern PGDLLEXPORT void _PG_init(void);

/**
 * 这个函数可能用于加载 tdengine 相关的库，
 * 例如初始化 tdengine 的客户端连接、加载必要的动态链接库等。
 */
bool tdengine_load_library(void);
static void tdengine_fdw_exit(int code, Datum arg);

/**
 * PostgreSQL 提供的一个宏，
 * 用于确保模块与当前运行的 PostgreSQL 服务器版本兼容。
 * 这个宏会在模块加载时进行版本检查，如果版本不兼容，模块将无法加载。
 */
PG_MODULE_MAGIC;

/* 启动一个外部查询所需的默认 CPU 成本。 */
#define DEFAULT_FDW_STARTUP_COST 100.0

/**
 * 处理一行数据所需的默认 CPU 成本
 * 在cpu_tuple_cost基础之上额外的开销
 */
#if PG_VERSION_NUM >= 170000
#define DEFAULT_FDW_TUPLE_COST 0.2
#else
#define DEFAULT_FDW_TUPLE_COST 0.01
#endif

/**
 * 没有远程估计时，假设排序操作会额外增加 20% 的成本。
 */
#define DEFAULT_FDW_SORT_MULTIPLIER 1.2

/**
 * 用于判断一个列是否为关键列。
 * 它接受一个参数A，通过比较A的defname是否为"key"，
 * 以及A的arg所指向的值是否为"true"来确定该列是否为关键列。
 */
#define IS_KEY_COLUMN(A) ((strcmp(A->defname, "key") == 0) && \
                          (strcmp(((Value *)(A->arg))->val.str, "true") == 0))

/**
 * 表示创建（SPD_CMD_CREATE）和删除（SPD_CMD_DROP）操作。
 * 在处理相关命令时，可以使用这两个常量来区分不同的操作类型。
 */
#define SPD_CMD_CREATE 0
#define SPD_CMD_DROP 1

/**
 * 接口注册函数和选项验证函数
 * Datum 是 PostgreSQL 中用于表示任意数据类型的通用数据类型。
 * PG_FUNCTION_ARGS 是 PostgreSQL 中定义的一个宏，用于表示函数的参数列表。
 */
extern Datum tdengine_fdw_handler(PG_FUNCTION_ARGS);
extern Datum tdengine_fdw_validator(PG_FUNCTION_ARGS);
/**
 * PG_FUNCTION_INFO_V1 宏用于向 PostgreSQL 的系统提供函数的元信息，
 * 使得 PostgreSQL 能够正确识别和调用这些函数。
 */
PG_FUNCTION_INFO_V1(tdengine_fdw_handler);
PG_FUNCTION_INFO_V1(tdengine_fdw_version);

/**
 * =========================函数声明========================
 */

//1. 扫描相关函数（必填）
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

//2. 修改相关函数：处理对外部表的修改操作，包括插入、删除和更新
static void tdengineAddForeignUpdateTargets(
#if (PG_VERSION_NUM < 140000)
    Query *parsetree,
#else
    PlannerInfo *root,
    Index rtindex,
#endif
    RangeTblEntry *target_rte,
    Relation target_relation);

static List *tdenginePlanForeignModify(PlannerInfo *root,
                                       ModifyTable *plan,
                                       Index resultRelation,
                                       int subplan_index);

static void tdengineBeginForeignModify(ModifyTableState *mtstate,
                                       ResultRelInfo *resultRelInfo,
                                       List *fdw_private,
                                       int subplan_index,
                                       int eflags);

static TupleTableSlot *tdengineExecForeignInsert(EState *estate,
                                                 ResultRelInfo *resultRelInfo,
                                                 TupleTableSlot *slot,
                                                 TupleTableSlot *planSlot);
#if (PG_VERSION_NUM >= 140000)
static TupleTableSlot **tdengineExecForeignBatchInsert(EState *estate,
                                                       ResultRelInfo *resultRelInfo,
                                                       TupleTableSlot **slots,
                                                       TupleTableSlot **planSlots,
                                                       int *numSlots);
static int tdengineGetForeignModifyBatchSize(ResultRelInfo *resultRelInfo);
#endif
static TupleTableSlot *tdengineExecForeignDelete(EState *estate,
                                                 ResultRelInfo *rinfo,
                                                 TupleTableSlot *slot,
                                                 TupleTableSlot *planSlot);

static void tdengineEndForeignModify(EState *estate,
                                     ResultRelInfo *resultRelInfo);

#if (PG_VERSION_NUM >= 110000)
static void tdengineEndForeignInsert(EState *estate,
                                     ResultRelInfo *resultRelInfo);
static void tdengineBeginForeignInsert(ModifyTableState *mtstate,
                                       ResultRelInfo *resultRelInfo);
#endif

static bool tdenginePlanDirectModify(PlannerInfo *root,
                                     ModifyTable *plan,
                                     Index resultRelation,
                                     int subplan_index);

static void tdengineBeginDirectModify(ForeignScanState *node, int eflags);

static TupleTableSlot *tdengineIterateDirectModify(ForeignScanState *node);

static void tdengineEndDirectModify(ForeignScanState *node);


//3. 解释相关函数：解释外部扫描和修改操作的执行计划
static void tdengineExplainForeignScan(ForeignScanState *node,
                                       struct ExplainState *es);

static void tdengineExplainForeignModify(ModifyTableState *mtstate,
                                         ResultRelInfo *rinfo,
                                         List *fdw_private,
                                         int subplan_index,
                                         struct ExplainState *es);

static void tdengineExplainDirectModify(ForeignScanState *node,
                                        struct ExplainState *es);

//4. 分析和导入相关函数                                    
static bool tdengineAnalyzeForeignTable(Relation relation,
                                        AcquireSampleRowsFunc *func,
                                        BlockNumber *totalpages);

static List *tdengineImportForeignSchema(ImportForeignSchemaStmt *stmt,
                                         Oid serverOid);

static void tdengineGetForeignUpperPaths(PlannerInfo *root,
                                         UpperRelationKind stage,
                                         RelOptInfo *input_rel,
                                         RelOptInfo *output_rel
#if (PG_VERSION_NUM >= 110000)
                                         ,
                                         void *extra
#endif
);

//5. 其他辅助函数：处理数据类型转换、查询参数准备、游标创建、DML 语句执行等操作，
//以及检查是否包含特定的正则表达式函数、获取批量大小选项等
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
                                 InfluxDBType **param_tdengine_types,
                                 InfluxDBValue **param_tdengine_values,
                                 InfluxDBColumnInfo **param_column_info);

static void process_query_params(ExprContext *econtext,
                                 FmgrInfo *param_flinfo,
                                 List *param_exprs,
                                 const char **param_values,
                                 Oid *param_types,
                                 InfluxDBType *param_tdengine_types,
                                 InfluxDBValue *param_tdengine_values,
                                 InfluxDBColumnInfo *param_column_info);

static void create_cursor(ForeignScanState *node);
static void execute_dml_stmt(ForeignScanState *node);
static TupleTableSlot **execute_foreign_insert_modify(EState *estate,
                                                      ResultRelInfo *resultRelInfo,
                                                      TupleTableSlot **slots,
                                                      TupleTableSlot **planSlots,
                                                      int numSlots);
static bool foreign_grouping_ok(PlannerInfo *root, RelOptInfo *grouped_rel);
static void add_foreign_grouping_paths(PlannerInfo *root,
                                       RelOptInfo *input_rel,
                                       RelOptInfo *grouped_rel);
static bool tdengine_contain_regex_star_functions_walker(Node *node, void *context);
static bool tdengine_contain_regex_star_functions(Node *clause);
#if (PG_VERSION_NUM >= 140000)
static int tdengine_get_batch_size_option(Relation rel);
#endif
static void tdengine_extract_slcols(InfluxDBFdwRelationInfo *fpinfo, PlannerInfo *root, RelOptInfo *baserel, List *tlist);
static bool tdengine_is_existed_measurement(Oid serverOid, char *tbl_name, tdengine_opt *options);
static bool tdengine_param_belong_to_qual(Node *qual, Node *param);

#ifdef CXX_CLIENT
#define free(x) pfree(x)
#endif













/*
 *库加载时初始化，设置on_proc_exit（）回调用来后端
 */
void _PG_init(void)
{
    on_proc_exit(&tdengine_fdw_exit, PointerGetDatum(NULL));
}
/*
 * “退出”回调函数
 */
static void
tdengine_fdw_exit(int code, Datum arg)
{
#ifdef CXX_CLIENT
    cleanup_cxx_client_connection();
#endif
}