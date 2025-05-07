#include "postgres.h"
#include "tdengine_fdw.h"

#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/sysattr.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "optimizer/tlist.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#define QUOTE '"'

// TODO: TDengine支持的函数列表
/* List of stable function with star argument of TDengine */
static const char *TDengineStableStarFunction[] = {
	"tdengine_count_all",
	"tdengine_mode_all",
	"tdengine_max_all",
	"tdengine_min_all",
	"tdengine_sum_all",
	"integral_all",
	"mean_all",
	"median_all",
	"spread_all",
	"stddev_all",
	"first_all",
	"last_all",
	"percentile_all",
	"sample_all",
	"abs_all",
	"acos_all",
	"asin_all",
	"atan_all",
	"atan2_all",
	"ceil_all",
	"cos_all",
	"cumulative_sum_all",
	"derivative_all",
	"difference_all",
	"elapsed_all",
	"exp_all",
	"floor_all",
	"ln_all",
	"log_all",
	"log2_all",
	"log10_all",
	"moving_average_all",
	"non_negative_derivative_all",
	"non_negative_difference_all",
	"pow_all",
	"round_all",
	"sin_all",
	"sqrt_all",
	"tan_all",
	"chande_momentum_oscillator_all",
	"exponential_moving_average_all",
	"double_exponential_moving_average_all",
	"kaufmans_efficiency_ratio_all",
	"kaufmans_adaptive_moving_average_all",
	"triple_exponential_moving_average_all",
	"triple_exponential_derivative_all",
	"relative_strength_index_all",
	NULL};

/* List of unique function without star argument of TDengine */
static const char *TDengineUniqueFunction[] = {
	"bottom",
	"percentile",
	"top",
	"cumulative_sum",
	"derivative",
	"difference",
	"elapsed",
	"log2",
	"log10", /* Use for PostgreSQL old version */
	"moving_average",
	"non_negative_derivative",
	"non_negative_difference",
	"holt_winters",
	"holt_winters_with_fit",
	"chande_momentum_oscillator",
	"exponential_moving_average",
	"double_exponential_moving_average",
	"kaufmans_efficiency_ratio",
	"kaufmans_adaptive_moving_average",
	"triple_exponential_moving_average",
	"triple_exponential_derivative",
	"relative_strength_index",
	"tdengine_count",
	"integral",
	"spread",
	"first",
	"last",
	"sample",
	"tdengine_time",
	"tdengine_fill_numeric",
	"tdengine_fill_option",
	NULL};

/* List of supported builtin function of TDengine */
static const char *TDengineSupportedBuiltinFunction[] = {
	"now",
	"sqrt",
	"abs",
	"acos",
	"asin",
	"atan",
	"atan2",
	"ceil",
	"cos",
	"exp",
	"floor",
	"ln",
	"log",
	"log10",
	"pow",
	"round",
	"sin",
	"tan",
	NULL};

/*
 * foreign_glob_cxt: 表达式树遍历的全局上下文结构
 *
 * 成员说明:
 *   @root: 全局计划器状态
 *   @foreignrel: 当前正在规划的外部关系
 *   @relids: 底层扫描中基础关系的relids（是一个bitmap）
 *   @relid: 关系OID
 *   @mixing_aggref_status: 标记表达式是否同时包含聚合和非聚合元素
 *   @for_tlist: 是否为目标列表表达式求值
 *   @is_inner_func: 是否存在于内部表达式中
 *
 * 使用场景:
 *   用于tdengine_foreign_expr_walker遍历表达式树时的全局状态保持
 */
typedef struct foreign_glob_cxt
{
	PlannerInfo *root;
	RelOptInfo *foreignrel;
	Relids relids;
	Oid relid;
	unsigned int mixing_aggref_status;
	bool for_tlist;
	bool is_inner_func;
} foreign_glob_cxt;

/*
 * FDWCollateState: 排序规则状态枚举
 *
 * 枚举值说明:
 *   FDW_COLLATE_NONE - 表达式属于不可排序类型
 *   FDW_COLLATE_SAFE - 排序规则来自外部变量
 *   FDW_COLLATE_UNSAFE - 排序规则来自其他来源
 *
 * 使用场景:
 *   用于标记表达式中排序规则的来源安全性
 */
typedef enum
{
	FDW_COLLATE_NONE,
	FDW_COLLATE_SAFE,
	FDW_COLLATE_UNSAFE,
} FDWCollateState;

/*
 * foreign_loc_cxt: 表达式树遍历的局部上下文结构
 *
 * 成员说明:
 *   @collation: 当前排序规则OID(如果有)
 *   @state: 当前排序规则选择状态
 *   @can_skip_cast: 外部函数是否可以跳过float8/numeric转换
 *   @can_pushdown_stable: 查询是否包含带星号或正则的stable函数
 *   @can_pushdown_volatile: 查询是否包含volatile函数
 *   @tdengine_fill_enable: 是否在tdengine_time()内解析子表达式 TODO:
 *   @have_otherfunc_tdengine_time_tlist: 目标列表中是否有除tdengine_time()外的其他函数 TODO:
 *   @has_time_key: 是否与时间键列比较
 *   @has_sub_or_add_operator: 表达式是否包含'+'或'-'运算符
 *   @is_comparison: 是否包含比较操作
 *
 * 使用场景:
 *   用于tdengine_foreign_expr_walker遍历表达式树时的局部状态保持
 */
typedef struct foreign_loc_cxt
{
	Oid collation;
	FDWCollateState state;
	bool can_skip_cast;
	bool can_pushdown_stable;
	bool can_pushdown_volatile;
	bool tdengine_fill_enable;			   // TODO:
	bool have_otherfunc_tdengine_time_tlist; // TODO:
	bool has_time_key;
	bool has_sub_or_add_operator;
	bool is_comparison;
} foreign_loc_cxt;

/*
 * PatternMatchingOperator - 模式匹配操作符类型枚举
 *
 * 枚举值说明:
 *   UNKNOWN_OPERATOR: 未知操作符类型(默认值)
 *   LIKE_OPERATOR: 区分大小写的LIKE操作符
 *   NOT_LIKE_OPERATOR: 区分大小写的NOT LIKE操作符
 *   ILIKE_OPERATOR: 不区分大小写的LIKE操作符
 *   NOT_ILIKE_OPERATOR: 不区分大小写的NOT LIKE操作符
 *   REGEX_MATCH_CASE_SENSITIVE_OPERATOR: 区分大小写的正则表达式匹配操作符
 *   REGEX_NOT_MATCH_CASE_SENSITIVE_OPERATOR: 区分大小写的正则表达式不匹配操作符
 *   REGEX_MATCH_CASE_INSENSITIVE_OPERATOR: 不区分大小写的正则表达式匹配操作符
 *   REGEX_NOT_MATCH_CASE_INSENSITIVE_OPERATOR: 不区分大小写的正则表达式不匹配操作符
 *
 * 使用场景:
 *   用于标识和分类SQL中的各种模式匹配操作符类型，
 *   主要在SQL语句解析和反解析过程中使用。
 */
typedef enum
{
	UNKNOWN_OPERATOR = 0,
	LIKE_OPERATOR,		/* LIKE case senstive */
	NOT_LIKE_OPERATOR,	/* NOT LIKE case sensitive */
	ILIKE_OPERATOR,		/* LIKE case insensitive */
	NOT_ILIKE_OPERATOR, /* NOT LIKE case insensitive */
	REGEX_MATCH_CASE_SENSITIVE_OPERATOR,
	REGEX_NOT_MATCH_CASE_SENSITIVE_OPERATOR,
	REGEX_MATCH_CASE_INSENSITIVE_OPERATOR,
	REGEX_NOT_MATCH_CASE_INSENSITIVE_OPERATOR
} PatternMatchingOperator;

/*
 * deparse_expr_cxt - 表达式反解析上下文结构体
 *
 * 功能说明:
 *   用于存储表达式反解析过程中的各种状态信息，
 *   主要用于将PostgreSQL表达式转换为TDengine兼容的SQL语句
 *
 * 成员说明:
 *   @root: 全局规划器状态(PlannerInfo结构指针)
 *   @foreignrel: 当前正在处理的外部关系信息(RelOptInfo结构指针)
 *   @scanrel: 底层扫描关系信息，当foreignrel表示连接或基础关系时与foreignrel相同
 *   @buf: 输出缓冲区，用于构建最终的SQL语句
 *   @params_list: 将成为远程参数的表达式列表
 *   @op_type: 模式匹配操作符类型(PatternMatchingOperator枚举)
 *   @is_tlist: 标记是否在目标列表表达式反解析过程中
 *   @can_skip_cast: 标记外部函数是否可以跳过float8/numeric类型转换
 *   @can_delete_directly: 标记DELETE语句是否可以直接下推执行
 *   @has_bool_cmp: 标记外部是否有布尔比较目标
 *   @tdengine_fill_expr: 存储fill()函数表达式(FuncExpr结构指针)
 *   @convert_to_timestamp:
 *     标记在与时间键列比较时，如果其数据类型是带时区的时间戳(timestamp with time zone)，
 *     是否需要转换为不带时区的时间戳(timestamp without time zone)
 */
typedef struct deparse_expr_cxt
{
	PlannerInfo *root;				 /* global planner state */
	RelOptInfo *foreignrel;			 /* the foreign relation we are planning for */
	RelOptInfo *scanrel;			 /* the underlying scan relation. Same as
									  * foreignrel, when that represents a join or
									  * a base relation. */
	StringInfo buf;					 /* output buffer to append to */
	List **params_list;				 /* exprs that will become remote Params */
	PatternMatchingOperator op_type; /* Type of operator for pattern matching */
	bool is_tlist;					 /* deparse during target list exprs */
	bool can_skip_cast;				 /* outer function can skip float8/numeric cast */
	bool can_delete_directly;		 /* DELETE statement can pushdown
									  * directly */
	bool has_bool_cmp;				 /* outer has bool comparison target */
	FuncExpr *tdengine_fill_expr;		 /* Store the fill() function */

	/*
	 * For comparison with time key column, if its data type is timestamp with time zone,
	 * need to convert to timestamp without time zone.
	 */
	bool convert_to_timestamp;
} deparse_expr_cxt;

typedef struct pull_func_clause_context
{
	List *funclist;
} pull_func_clause_context;

// /*
//  * Functions to determine whether an expression can be evaluated safely on
//  * remote server.
//  */
// static bool tdengine_foreign_expr_walker(Node *node,
// 										 foreign_glob_cxt *glob_cxt,
// 										 foreign_loc_cxt *outer_cxt);

// /*
//  * Functions to construct string representation of a node tree.
//  */

static void tdengine_deparse_expr(Expr *node, deparse_expr_cxt *context);
static void tdengine_deparse_var(Var *node, deparse_expr_cxt *context);
static void tdengine_deparse_const(Const *node, deparse_expr_cxt *context, int showtype);
static void tdengine_deparse_param(Param *node, deparse_expr_cxt *context);
static void tdengine_deparse_func_expr(FuncExpr *node, deparse_expr_cxt *context);
static void tdengine_deparse_fill_option(StringInfo buf, const char *val);
static void tdengine_deparse_op_expr(OpExpr *node, deparse_expr_cxt *context);
static void tdengine_deparse_operator_name(StringInfo buf, Form_pg_operator opform, PatternMatchingOperator *op_type);
static void tdengine_deparse_scalar_array_op_expr(ScalarArrayOpExpr *node,
												  deparse_expr_cxt *context);
static void tdengine_deparse_relabel_type(RelabelType *node, deparse_expr_cxt *context);
static void tdengine_deparse_bool_expr(BoolExpr *node, deparse_expr_cxt *context);
static void tdengine_deparse_null_test(NullTest *node, deparse_expr_cxt *context);
static void tdengine_deparse_array_expr(ArrayExpr *node, deparse_expr_cxt *context);
static void tdengine_deparse_coerce_via_io(CoerceViaIO *cio, deparse_expr_cxt *context);

static void tdengine_print_remote_param(int paramindex, Oid paramtype, int32 paramtypmod,
										deparse_expr_cxt *context);
static void tdengine_print_remote_placeholder(Oid paramtype, int32 paramtypmod,
											  deparse_expr_cxt *context);

static void tdengine_deparse_relation(StringInfo buf, Relation rel);
static void tdengine_deparse_target_list(StringInfo buf, PlannerInfo *root, Index rtindex, Relation rel,
										 Bitmapset *attrs_used, List **retrieved_attrs);
static void tdengine_deparse_target_list_schemaless(StringInfo buf, Relation rel, Oid reloid,
													Bitmapset *attrs_used,
													List **retrieved_attrs,
													bool all_fieldtag, List *slcols);
static void tdengine_deparse_slvar(Node *node, Var *var, Const *cnst, deparse_expr_cxt *context);
static void tdengine_deparse_column_ref(StringInfo buf, int varno, int varattno, Oid vartype, PlannerInfo *root, bool convert, bool *can_delete_directly);

static void tdengine_deparse_select(List *tlist, List **retrieved_attrs, deparse_expr_cxt *context);
static void tdengine_deparse_from_expr_for_rel(StringInfo buf, PlannerInfo *root,
											   RelOptInfo *foreignrel,
											   bool use_alias, List **params_list);
static void tdengine_deparse_from_expr(List *quals, deparse_expr_cxt *context);
static void tdengine_deparse_aggref(Aggref *node, deparse_expr_cxt *context);
static void tdengine_append_conditions(List *exprs, deparse_expr_cxt *context);
static void tdengine_append_group_by_clause(List *tlist, deparse_expr_cxt *context);
static void tdengine_append_order_by_clause(List *pathkeys, deparse_expr_cxt *context);
static Node *tdengine_deparse_sort_group_clause(Index ref, List *tlist,
												deparse_expr_cxt *context);

static void tdengine_deparse_explicit_target_list(List *tlist, List **retrieved_attrs,
												  deparse_expr_cxt *context);
static Expr *tdengine_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel);

static bool tdengine_contain_time_column(List *exprs, schemaless_info *pslinfo);
static bool tdengine_contain_time_key_column(Oid relid, List *exprs);
static bool tdengine_contain_time_expr(List *exprs);
static bool tdengine_contain_time_function(List *exprs);
static bool tdengine_contain_time_param(List *exprs);
static bool tdengine_contain_time_const(List *exprs);

static void tdengine_append_field_key(TupleDesc tupdesc, StringInfo buf, Index rtindex, PlannerInfo *root, bool first);
static void tdengine_append_limit_clause(deparse_expr_cxt *context);
static bool tdengine_is_string_type(Node *node, schemaless_info *pslinfo);
static char *tdengine_quote_identifier(const char *s, char q);
static bool tdengine_contain_functions_walker(Node *node, void *context);

bool tdengine_is_grouping_target(TargetEntry *tle, Query *query);
bool tdengine_is_builtin(Oid objectId);
bool tdengine_is_regex_argument(Const *node, char **extval);
char *tdengine_replace_function(char *in);
bool tdengine_is_star_func(Oid funcid, char *in);
static bool tdengine_is_unique_func(Oid funcid, char *in);
static bool tdengine_is_supported_builtin_func(Oid funcid, char *in);
static bool exist_in_function_list(char *funcname, const char **funclist);

static void add_backslash(StringInfo buf, const char *ptr, const char *regex_special);
static bool tdengine_last_percent_sign_check(const char *val);
static void tdengine_deparse_string_like_pattern(StringInfo buf, const char *val, PatternMatchingOperator op_type);
static void tdengine_deparse_string_regex_pattern(StringInfo buf, const char *val, PatternMatchingOperator op_type);

/*
 * Local variables.
 */
static char *cur_opname = NULL;

/*
 * 反解析关系名称到SQL语句
 * 功能: 将PostgreSQL关系对象转换为TDengine兼容的表名引用格式
 *
 * 参数:
 *   @buf: 输出字符串缓冲区
 *   @rel: 关系对象(表或视图)
 *
 * 处理流程:
 *   1. 通过tdengine_get_table_name获取表名(考虑FDW选项table_name)
 *   2. 使用tdengine_quote_identifier为表名添加引号
 *   3. 将带引号的表名追加到输出缓冲区
 *
 * 注意事项:
 *   - 会优先使用FDW选项中的table_name(如果设置)
 *   - 输出的表名会被适当引号包围
 *   - 适用于生成SELECT/INSERT/UPDATE等语句中的表名部分
 */
static void
tdengine_deparse_relation(StringInfo buf, Relation rel)
{
	/* 获取表名(考虑FDW选项) */
	char *relname = tdengine_get_table_name(rel);

	/* 添加带引号的表名到输出缓冲区 */
	appendStringInfo(buf, "%s", tdengine_quote_identifier(relname, QUOTE));
}

/*
 * tdengine_quote_identifier - 为标识符添加引号
 *
 * 参数:
 *   @s: 需要加引号的原始字符串
 *   @q: 引号字符(如单引号'或双引号")
 *
 * 返回值:
 *   char* - 新分配的内存，包含加引号后的字符串
 *
 * 功能说明:
 *   1. 为SQL标识符添加引号，处理标识符中的引号转义
 *   2. 分配足够内存容纳转义后的字符串
 *   3. 在字符串前后添加指定引号字符
 *   4. 对字符串中的引号字符进行转义(重复一次)
 *
 * 内存管理:
 *   使用palloc分配内存，调用者需负责释放
 */
static char *
tdengine_quote_identifier(const char *s, char q)
{
	/* 分配足够内存: 原始长度*2(考虑转义) + 3(两个引号和结束符) */
	char *result = palloc(strlen(s) * 2 + 3);
	char *r = result;

	/* 添加起始引号 */
	*r++ = q;

	/* 处理字符串内容 */
	while (*s)
	{
		/* 转义引号字符 */
		if (*s == q)
			*r++ = *s;
		/* 复制字符 */
		*r++ = *s;
		s++;
	}

	/* 添加结束引号和终止符 */
	*r++ = q;
	*r++ = '\0';

	return result;
}

/*
 * pull_func_clause_walker - 递归遍历语法树并收集函数表达式节点
 *
 * 参数:
 *   @node: 当前遍历的语法树节点
 *   @context: 上下文结构，用于存储收集到的函数表达式
 *
 * 返回值:
 *   bool - 是否继续遍历子节点(false表示停止遍历当前分支)
 *
 * 功能:
 *   1. 检查当前节点是否为函数表达式(FuncExpr)
 *   2. 如果是函数表达式，则将其添加到上下文的函数列表中
 *   3. 递归遍历所有子节点
 */
static bool
tdengine_pull_func_clause_walker(Node *node, pull_func_clause_context *context)
{
	/* 空节点直接返回false，不继续遍历 */
	if (node == NULL)
		return false;

	/* 检查节点是否为函数表达式类型 */
	if (IsA(node, FuncExpr))
	{
		/* 将函数表达式节点添加到上下文的链表中 */
		context->funclist = lappend(context->funclist, node);
		/* 返回false表示不需要继续遍历当前节点的子节点 */
		return false;
	}

	/* 使用PostgreSQL的expression_tree_walker递归遍历所有子节点 */
	return expression_tree_walker(node, tdengine_pull_func_clause_walker,
								  (void *)context);
}

/*
 * pull_func_clause - 从语法树中提取所有函数表达式
 *
 * 参数:
 *   @node: 语法树的根节点
 *
 * 返回值:
 *   List* - 包含所有找到的函数表达式的链表
 *
 * 功能:
 *   1. 初始化上下文结构
 *   2. 调用walker函数开始遍历语法树
 *   3. 返回收集到的函数表达式列表
 */
List *
tdengine_pull_func_clause(Node *node)
{
	/* 初始化上下文结构 */
	pull_func_clause_context context;
	/* 初始化空链表 */
	context.funclist = NIL;

	/* 调用walker函数开始遍历语法树 */
	tdengine_pull_func_clause_walker(node, &context);

	/* 返回收集到的函数表达式链表 */
	return context.funclist;
}

/*
 * 判断给定表达式是否可以在外部服务器上安全执行
 *
 * 参数:
 *   @root: 规划器信息
 *   @baserel: 基础关系信息
 *   @expr: 要检查的表达式
 *   @for_tlist: 是否为目标列表表达式
 */
bool tdengine_is_foreign_expr(PlannerInfo *root,
							  RelOptInfo *baserel,
							  Expr *expr,
							  bool for_tlist)
{
	foreign_glob_cxt glob_cxt; // 全局上下文
	foreign_loc_cxt loc_cxt;   // 局部上下文
	TDengineFdwRelationInfo *fpinfo = (TDengineFdwRelationInfo *)(baserel->fdw_private);

	/*
	 * 初始化全局上下文:
	 *   - 设置规划器信息、外部表信息
	 *   - 初始化聚合函数状态标记
	 *   - 设置目标列表标志
	 *   - 标记当前不在嵌套函数中
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;
	glob_cxt.relid = fpinfo->table->relid;
	glob_cxt.mixing_aggref_status = TDENGINE_TARGETS_MIXING_AGGREF_SAFE;
	glob_cxt.for_tlist = for_tlist;
	glob_cxt.is_inner_func = false;

	/*
	 * 设置关系ID集合(relids):
	 *   - 对于上层关系，使用其底层扫描关系的关系ID集合
	 *   - 对于其他关系，使用自身的关系ID集合
	 */
	if (baserel->reloptkind == RELOPT_UPPER_REL)
		glob_cxt.relids = fpinfo->outerrel->relids;
	else
		glob_cxt.relids = baserel->relids;

	/*
	 * 初始化局部上下文:
	 *   - 排序规则初始为无效
	 *   - 排序规则状态初始为无
	 *   - 默认不允许跳过类型转换
	 *   - 初始化时间相关标志
	 */
	loc_cxt.collation = InvalidOid;
	loc_cxt.state = FDW_COLLATE_NONE;
	loc_cxt.can_skip_cast = false;
	loc_cxt.tdengine_fill_enable = false;
	loc_cxt.has_time_key = false;
	loc_cxt.has_sub_or_add_operator = false;
	loc_cxt.is_comparison = false;

	/* 递归遍历表达式树进行检查 */
	if (!tdengine_foreign_expr_walker((Node *)expr, &glob_cxt, &loc_cxt))
		return false;

	/*
	 * 检查排序规则:
	 * 如果表达式有不安全的排序规则(非来自外部变量)，则不能下推
	 */
	if (loc_cxt.state == FDW_COLLATE_UNSAFE)
		return false;

	/* 表达式可以安全地在远程服务器上执行 */
	return true;
}

/*
 * is_valid_type: 检查给定的OID是否为TDengine支持的有效数据类型
 *
 * 参数:
 *   @type: 要检查的数据类型OID
 *
 * 功能说明:
 *   1. 检查输入的类型OID是否在TDengine支持的数据类型列表中
 *      虽然所有类型OID都定义在PostgreSQL头文件中，
 *      但函数只选择了TDengine支持的那些类型子集进行检查
 *   2. 支持的类型包括:
 *      - 整数类型(INT2/INT4/INT8/OID)
 *      - 浮点类型(FLOAT4/FLOAT8/NUMERIC)
 *      - 字符串类型(VARCHAR/TEXT)
 *      - 时间类型(TIME/TIMESTAMP/TIMESTAMPTZ)
 *
 * 使用场景:
 *   在表达式下推检查时，用于验证参数或变量的数据类型是否可以被TDengine处理
 */
static bool
is_valid_type(Oid type)
{
	switch (type)
	{
	case INT2OID:
	case INT4OID:
	case INT8OID:
	case OIDOID:
	case FLOAT4OID:
	case FLOAT8OID:
	case NUMERICOID:
	case VARCHAROID:
	case TEXTOID:
	case TIMEOID:
	case TIMESTAMPOID:
	case TIMESTAMPTZOID:
		return true;
	}
	return false;
}

/*
 * tdengine_foreign_expr_walker: 递归检查表达式节点是否可下推到TDengine执行
 *
 * 参数:
 *   @node: 要检查的表达式节点
 *   @glob_cxt: 全局上下文信息(包含规划器状态、外部表信息等)
 *   @outer_cxt: 外层表达式上下文信息(包含排序规则状态等)
 *
 * 功能说明:
 *   1. 递归遍历表达式树，检查各种节点类型是否支持远程执行
 *   2. 维护表达式下推状态信息(排序规则、类型安全等)
 *   3. 处理不同类型节点的特殊限制条件
 *   4. 更新全局和局部上下文信息
 *
 * 处理流程:
 *   1. 初始化内层上下文信息
 *   2. 根据节点类型进行不同处理
 *   3. 递归检查子节点
 *   4. 合并子节点的状态信息
 *   5. 返回最终检查结果
 */
static bool
tdengine_foreign_expr_walker(Node *node,
							 foreign_glob_cxt *glob_cxt,
							 foreign_loc_cxt *outer_cxt)
{
	bool check_type = true;				/* 是否需要检查类型安全性 */
	foreign_loc_cxt inner_cxt;			/* 内层表达式上下文 */
	Oid collation;						/* 当前节点的排序规则OID */
	FDWCollateState state;				/* 排序规则状态 */
	HeapTuple tuple;					/* 系统缓存元组 */
	Form_pg_operator form;				/* 操作符信息结构体 */
	char *cur_opname;					/* 当前操作符名称 */
	static bool is_time_column = false; /* 标记是否时间列(静态变量保持状态) */

	/* 获取FDW关系信息 */
	TDengineFdwRelationInfo *fpinfo =
		(TDengineFdwRelationInfo *)(glob_cxt->foreignrel->fdw_private);

	/* 空节点直接返回true */
	if (node == NULL)
		return true;

	/* 初始化内层上下文 */
	inner_cxt.collation = InvalidOid;
	inner_cxt.state = FDW_COLLATE_NONE;
	inner_cxt.can_skip_cast = false;
	inner_cxt.can_pushdown_stable = false;
	inner_cxt.can_pushdown_volatile = false;
	inner_cxt.tdengine_fill_enable = false;
	inner_cxt.has_time_key = false;
	inner_cxt.has_sub_or_add_operator = false;
	inner_cxt.is_comparison = false;

	/* 根据节点类型进行不同处理 */
	switch (nodeTag(node))
	{
	case T_Var:
		/* 处理变量节点 */
		{
			Var *var = (Var *)node;

			/*
			 * 如果变量属于外部表，则认为其排序规则是安全的；
			 * 如果属于其他表，则按参数处理方式处理其排序规则
			 */
			if (bms_is_member(var->varno, glob_cxt->relids) &&
				var->varlevelsup == 0)
			{
				/* 变量属于外部表 */

				/* 检查变量属性号是否有效 */
				if (var->varattno < 0)
					return false;

				/* 检查是否为时间类型列 */
				if (TDENGINE_IS_TIME_TYPE(var->vartype))
				{
					is_time_column = true;

					/*
					 * 不支持下推时间列与时间键的比较运算(包含加减间隔的情况)
					 * 例如: time_key = time_column +/- interval
					 */
					if (outer_cxt->is_comparison &&
						outer_cxt->has_sub_or_add_operator &&
						outer_cxt->has_time_key)
						return false;
				}

				/* 标记当前目标是字段/标签列 */
				glob_cxt->mixing_aggref_status |= TDENGINE_TARGETS_MARK_COLUMN;

				/* 检查排序规则 */
				collation = var->varcollid;
				state = OidIsValid(collation) ? FDW_COLLATE_SAFE : FDW_COLLATE_NONE;
			}
			else
			{
				/* 变量属于其他表的情况 */
				collation = var->varcollid;
				if (collation == InvalidOid ||
					collation == DEFAULT_COLLATION_OID)
				{
					/*
					 * 无排序规则或与外部变量排序规则兼容时，
					 * 设置为NONE状态
					 */
					state = FDW_COLLATE_NONE;
				}
				else
				{
					/*
					 * 不立即返回失败，因为变量可能出现在
					 * 不关心排序规则的上下文中
					 */
					state = FDW_COLLATE_UNSAFE;
				}
			}
		}
		break;

	/* 处理常量节点(Const) */
	case T_Const:
	{
		char *type_name;
		Const *c = (Const *)node;

		/* 处理INTERVAL类型常量 */
		if (c->consttype == INTERVALOID)
		{
			Interval *interval = DatumGetIntervalP(c->constvalue);
#if (PG_VERSION_NUM >= 150000)
			struct pg_itm tm;
			interval2itm(*interval, &tm);
#else
			struct pg_tm tm;
			fsec_t fsec;

			interval2tm(*interval, &tm, &fsec);
#endif

			/*
			 *   TDengine不支持包含月份或年份的时间间隔计算，
			 *   因此需要过滤掉这类INTERVAL常量
			 */
			if (tm.tm_mon != 0 || tm.tm_year != 0)
			{
				return false;
			}
		}

		/*
		 * 处理常量类型名称检查
		 * 功能: 检查常量类型是否为特殊类型"tdengine_fill_enum"
		 *      如果是则跳过内置类型检查
		 */
		type_name = tdengine_get_data_type_name(c->consttype);
		if (strcmp(type_name, "tdengine_fill_enum") == 0)
			check_type = false;

		/*
		 * 检查常量排序规则
		 * 规则:
		 *   1. 如果常量使用非默认排序规则，表示可能是非内置类型或包含CollateExpr
		 *   2. 这类表达式不能下推到远程执行
		 * 返回值:
		 *   false - 排序规则不安全，不能下推
		 */
		if (c->constcollid != InvalidOid &&
			c->constcollid != DEFAULT_COLLATION_OID)
			return false;

		/* 默认情况下认为常量不设置排序规则 */
		collation = InvalidOid;
		state = FDW_COLLATE_NONE;
	}
	break;

	/*
	 * 处理参数节点(T_Param)
	 * 功能: 检查参数类型是否有效且符合下推条件
	 */
	case T_Param:
	{
		Param *p = (Param *)node;

		/* 检查参数类型是否有效 */
		if (!is_valid_type(p->paramtype))
			return false;

		/* 处理时间类型参数的特殊限制 */
		if (TDENGINE_IS_TIME_TYPE(p->paramtype))
		{
			/*
			 * 不支持下推时间参数与时间键的比较运算(包含加减间隔的情况)
			 * 示例: time_key = Param +/- interval
			 */
			if (outer_cxt->is_comparison &&
				outer_cxt->has_sub_or_add_operator &&
				outer_cxt->has_time_key)
				return false;
		}

		/*
		 * 参数排序规则处理规则:
		 * 1. 无排序规则或默认排序规则 - 标记为NONE状态
		 * 2. 其他排序规则 - 标记为UNSAFE状态
		 */
		collation = p->paramcollid;
		if (collation == InvalidOid ||
			collation == DEFAULT_COLLATION_OID)
			state = FDW_COLLATE_NONE;
		else
			state = FDW_COLLATE_UNSAFE;
	}
	break;
		/*
		 *   1. 仅支持基础关系(RELOPT_BASEREL)或其他成员关系(RELOPT_OTHER_MEMBER_REL)
		 *   2. 用于支持星号(*)和正则表达式函数的字段访问
		 */
	case T_FieldSelect:
	{
		if (!(glob_cxt->foreignrel->reloptkind == RELOPT_BASEREL ||
			  glob_cxt->foreignrel->reloptkind == RELOPT_OTHER_MEMBER_REL))
			return false;

		collation = InvalidOid;
		state = FDW_COLLATE_NONE;
		check_type = false;
	}
	break;

	/*
	 * 处理函数表达式节点(T_FuncExpr)
	 * 功能: 检查函数表达式是否可下推到远程执行
	 */
	case T_FuncExpr:
	{
		FuncExpr *fe = (FuncExpr *)node;
		char *opername = NULL;
		bool is_cast_func = false;
		bool is_star_func = false;
		bool can_pushdown_func = false;
		bool is_regex = false;

		/* 从系统缓存获取函数名称 */
		tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(fe->funcid));
		if (!HeapTupleIsValid(tuple))
		{
			elog(ERROR, "cache lookup failed for function %u", fe->funcid);
		}
		opername = pstrdup(((Form_pg_proc)GETSTRUCT(tuple))->proname.data);
		ReleaseSysCache(tuple);

		/* 处理时间类型函数的特殊限制 */
		if (TDENGINE_IS_TIME_TYPE(fe->funcresulttype))
		{
			if (outer_cxt->is_comparison)
			{
				/* 仅支持now()函数的时间比较 */
				if (strcmp(opername, "now") != 0)
				{
					return false;
				}
				/* 仅支持now()与时间键的比较 */
				else if (!outer_cxt->has_time_key)
				{
					return false;
				}
			}
		}

		/* 检查是否为类型转换函数(float8/numeric) */
		if (strcmp(opername, "float8") == 0 || strcmp(opername, "numeric") == 0)
		{
			is_cast_func = true;
		}

		/* 下推到 TDengine */
		if (tdengine_is_star_func(fe->funcid, opername))
		{
			is_star_func = true;
			outer_cxt->can_pushdown_stable = true;
		}

		if (tdengine_is_unique_func(fe->funcid, opername) ||
			tdengine_is_supported_builtin_func(fe->funcid, opername))
		{
			can_pushdown_func = true;
			inner_cxt.can_skip_cast = true;
			outer_cxt->can_pushdown_volatile = true;
		}

		if (!(is_star_func || can_pushdown_func || is_cast_func))
			return false;

		// TODO: fill()函数相关
		/* fill() must be inside tdengine_time() */
		if (strcmp(opername, "tdengine_fill_numeric") == 0 ||
			strcmp(opername, "tdengine_fill_option") == 0)
		{
			if (outer_cxt->tdengine_fill_enable == false)
				elog(ERROR, "tdengine_fdw: syntax error tdengine_fill_numeric() or tdengine_fill_option() must be embedded inside tdengine_time() function\n");
		}

		/*
		 * 类型转换函数处理逻辑:
		 *   1. 如果是类型转换函数(float8/numeric)且外层不允许跳过转换检查，则拒绝下推
		 *   2. 对于非类型转换函数:
		 *      - 如果不在目标列表(for_tlist=false)且是嵌套函数(is_inner_func=true)，则拒绝下推
		 *      - 否则标记当前处于嵌套函数环境中
		 */
		if (is_cast_func)
		{
			/* 类型转换函数必须在外层允许跳过转换检查时才可下推 */
			if (outer_cxt->can_skip_cast == false)
				return false;
		}
		else
		{
			/*
			 * 非目标列表中的嵌套函数不能下推执行
			 * 防止在非SELECT列表位置执行嵌套函数
			 */
			if (!glob_cxt->for_tlist && glob_cxt->is_inner_func)
				return false;

			/* 标记当前处理的是嵌套函数 */
			glob_cxt->is_inner_func = true;
		}

		/*
		 * Allow tdengine_fill_numeric/tdengine_fill_option() inside
		 * tdengine_time() function
		 */
		if (strcmp(opername, "tdengine_time") == 0)
		{
			inner_cxt.tdengine_fill_enable = true;
		}
		else
		{
			/* There is another function than tdengine_time in tlist */
			outer_cxt->have_otherfunc_tdengine_time_tlist = true;
		}

		/*
		 * Recurse to input subexpressions.
		 */
		if (!tdengine_foreign_expr_walker((Node *)fe->args,
										  glob_cxt, &inner_cxt))
			return false;

		/*
		 * Force to restore the state after deparse subexpression if
		 * it has been change above
		 */
		inner_cxt.tdengine_fill_enable = false;

		if (!is_cast_func)
			glob_cxt->is_inner_func = false;

		if (list_length(fe->args) > 0)
		{
			ListCell *funclc;
			Node *firstArg;

			funclc = list_head(fe->args);
			firstArg = (Node *)lfirst(funclc);

			if (IsA(firstArg, Const))
			{
				Const *arg = (Const *)firstArg;
				char *extval;

				if (arg->consttype == TEXTOID)
					is_regex = tdengine_is_regex_argument(arg, &extval);
			}
		}

		if (is_regex)
		{
			collation = InvalidOid;
			state = FDW_COLLATE_NONE;
			check_type = false;
			outer_cxt->can_pushdown_stable = true;
		}
		else
		{
			/*
			 * If function's input collation is not derived from a
			 * foreign Var, it can't be sent to remote.
			 */
			if (fe->inputcollid == InvalidOid)
				/* OK, inputs are all noncollatable */;
			else if (inner_cxt.state != FDW_COLLATE_SAFE ||
					 fe->inputcollid != inner_cxt.collation)
				return false;

			/*
			 * Detect whether node is introducing a collation not
			 * derived from a foreign Var.  (If so, we just mark it
			 * unsafe for now rather than immediately returning false,
			 * since the parent node might not care.)
			 */
			collation = fe->funccollid;
			if (collation == InvalidOid)
				state = FDW_COLLATE_NONE;
			else if (inner_cxt.state == FDW_COLLATE_SAFE &&
					 collation == inner_cxt.collation)
				state = FDW_COLLATE_SAFE;
			else if (collation == DEFAULT_COLLATION_OID)
				state = FDW_COLLATE_NONE;
			else
				state = FDW_COLLATE_UNSAFE;
		}
	}
	break;
	case T_OpExpr:
	{
		OpExpr *oe = (OpExpr *)node;
		bool is_slvar = false;
		bool is_param = false;
		bool has_time_key = false;
		bool has_time_column = false;
		bool has_time_tags_or_fields_column = false;

		if (tdengine_is_slvar_fetch(node, &(fpinfo->slinfo)))
			is_slvar = true;

		if (tdengine_is_param_fetch(node, &(fpinfo->slinfo)))
			is_param = true;

		/* trans
		 * 同理，只有内置操作符才能下推到远程执行。
		 * (如果操作符是内置的，那么其底层函数也必然是内置的)
		 */
		if (!tdengine_is_builtin(oe->opno) && !is_slvar && !is_param)
			return false;

		tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(oe->opno));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for operator %u", oe->opno);
		form = (Form_pg_operator)GETSTRUCT(tuple);

		/* opname is not a SQL identifier, so we should not quote it. */
		cur_opname = pstrdup(NameStr(form->oprname));
		ReleaseSysCache(tuple);

		if (strcmp(cur_opname, "=") == 0 ||
			strcmp(cur_opname, ">") == 0 ||
			strcmp(cur_opname, "<") == 0 ||
			strcmp(cur_opname, ">=") == 0 ||
			strcmp(cur_opname, "<=") == 0 ||
			strcmp(cur_opname, "!=") == 0 ||
			strcmp(cur_opname, "<>") == 0)
		{
			inner_cxt.is_comparison = true;
		}

		/* trans
		 * 不支持下推时间间隔(interval)与时间间隔之间的比较运算
		 * 例如:
		 *	(时间-时间) vs 时间间隔常量
		 *	(时间-时间) vs (时间-时间)
		 */

		if (inner_cxt.is_comparison &&
			exprType((Node *)linitial(oe->args)) == INTERVALOID &&
			exprType((Node *)lsecond(oe->args)) == INTERVALOID)
		{
			return false;
		}

		has_time_key = tdengine_contain_time_key_column(glob_cxt->relid, oe->args);

		/* trans
		 * 不支持下推时间表达式与非时间键列的比较运算
		 * 例如:
		 *	时间+/-间隔 vs 标签/字段列
		 *	时间+/-间隔 vs 时间+/-间隔
		 *	时间+/-间隔 vs 函数
		 */

		if (inner_cxt.is_comparison &&
			!has_time_key &&
			tdengine_contain_time_expr(oe->args))
		{
			return false;
		}

		/*
		 * Does not pushdown comparsion using !=, <> with time key column.
		 */
		if (strcmp(cur_opname, "!=") == 0 ||
			strcmp(cur_opname, "<>") == 0)
		{
			if (has_time_key)
				return false;
		}

		has_time_column = tdengine_contain_time_column(oe->args, &(fpinfo->slinfo));

		has_time_tags_or_fields_column = (has_time_column && !has_time_key);

		/* Does not pushdown time comparison between tags/fields vs function */
		if (inner_cxt.is_comparison &&
			has_time_tags_or_fields_column &&
			tdengine_contain_time_function(oe->args))
		{
			return false;
		}

		if (strcmp(cur_opname, ">") == 0 ||
			strcmp(cur_opname, "<") == 0 ||
			strcmp(cur_opname, ">=") == 0 ||
			strcmp(cur_opname, "<=") == 0 ||
			strcmp(cur_opname, "=") == 0)
		{
			List *first = list_make1(linitial(oe->args));
			List *second = list_make1(lsecond(oe->args));
			bool has_both_time_colum = tdengine_contain_time_column(first, &(fpinfo->slinfo)) &&
									   tdengine_contain_time_column(second, &(fpinfo->slinfo));

			/*
			 * Does not pushdown time comparsion using <, >, <=, >=, = between time key and time column
			 * For example:
			 *	time key vs time key
			 *	time key vs tags/fields column
			 */
			if (has_time_key && has_both_time_colum)
			{
				return false;
			}

			/* Handle the operators <, >, <=, >= */
			if (strcmp(cur_opname, "=") != 0)
			{
				bool has_first_time_key = tdengine_contain_time_key_column(glob_cxt->relid, first);
				bool has_second_time_key = tdengine_contain_time_key_column(glob_cxt->relid, second);
				bool has_both_tags_or_fields_column = (has_both_time_colum && !has_first_time_key && !has_second_time_key);

				/* Does not pushdown comparison between tags/fields column and time tags/field column */
				if (has_both_tags_or_fields_column)
					return false;

				/*
				 * Does not pushdown comparison between tags/fields column and time constant or time param
				 * For example:
				 *	tags/fields vs '2010-10-10 10:10:10'
				 */
				if (has_time_tags_or_fields_column &&
					(tdengine_contain_time_const(oe->args) ||
					 tdengine_contain_time_param(oe->args)))
				{
					return false;
				}

				/*
				 * Cannot pushdown to TDengine if there is string comparison
				 * with: "<, >, <=, >=" operators.
				 */
				if (tdengine_is_string_type((Node *)linitial(oe->args), &(fpinfo->slinfo)))
				{
					return false;
				}
			}
		}

		/*
		 * Does not support pushdown time comparison between time key column and time column +/- interval or
		 * param +/- interval or function +/- interval except now() +/- interval.
		 * Set flag here and recursive check in each node T_Var, T_Param, T_FuncExpr.
		 */
		if (strcmp(cur_opname, "+") == 0 ||
			strcmp(cur_opname, "-") == 0)
		{
			inner_cxt.has_time_key = outer_cxt->has_time_key;
			inner_cxt.is_comparison = outer_cxt->is_comparison;
			inner_cxt.has_sub_or_add_operator = true;
		}
		else
		{
			inner_cxt.has_time_key = has_time_key;
		}

		if (is_slvar || is_param)
		{
			collation = oe->inputcollid;
			check_type = false;

			state = FDW_COLLATE_SAFE;

			break;
		}

		/*
		 * Recurse to input subexpressions.
		 */
		if (!tdengine_foreign_expr_walker((Node *)oe->args,
										  glob_cxt, &inner_cxt))
			return false;

		/*
		 * Mixing aggregate and non-aggregate error occurs when SELECT
		 * statement includes both of aggregate function and
		 * standalone field key or tag key. It is unsafe to pushdown
		 * if target operation expression has mixing aggregate and
		 * non-aggregate, such as: (1+col1+sum(col2)),
		 * (sum(col1)*col2)
		 */
		if ((glob_cxt->mixing_aggref_status & TDENGINE_TARGETS_MIXING_AGGREF_UNSAFE) ==
			TDENGINE_TARGETS_MIXING_AGGREF_UNSAFE)
		{
			return false;
		}

		/*
		 * If operator's input collation is not derived from a foreign
		 * Var, it can't be sent to remote.
		 */
		if (oe->inputcollid == InvalidOid)
			/* OK, inputs are all noncollatable */;
		else if (inner_cxt.state != FDW_COLLATE_SAFE ||
				 oe->inputcollid != inner_cxt.collation)
			return false;

		/* Result-collation handling is same as for functions */
		collation = oe->opcollid;
		if (collation == InvalidOid)
			state = FDW_COLLATE_NONE;
		else if (inner_cxt.state == FDW_COLLATE_SAFE &&
				 collation == inner_cxt.collation)
			state = FDW_COLLATE_SAFE;
		else
			state = FDW_COLLATE_UNSAFE;
	}
	break;
		/*
		 * 处理标量数组操作表达式节点(T_ScalarArrayOpExpr)
		 * 功能: 检查标量数组操作表达式是否可下推到远程执行
		 *
		 * 处理流程:
		 *   1. 从系统缓存获取操作符信息
		 *   2. 检查字符串类型比较操作是否合法(不支持<,>,<=,>=操作符)
		 *   3. 检查是否为内置操作符
		 *   4. 检查是否包含时间列(不支持时间列操作)
		 *   5. 递归检查子表达式
		 *   6. 检查输入排序规则是否合法
		 *
		 * 注意事项:
		 *   - 标量数组操作表达式通常用于IN/ANY/SOME等操作
		 *   - 严格限制时间列和字符串比较操作的下推
		 */
	case T_ScalarArrayOpExpr:
	{
		ScalarArrayOpExpr *oe = (ScalarArrayOpExpr *)node;

		/* 从系统缓存获取操作符信息 */
		tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(oe->opno));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for operator %u", oe->opno);
		form = (Form_pg_operator)GETSTRUCT(tuple);

		cur_opname = pstrdup(NameStr(form->oprname));
		ReleaseSysCache(tuple);

		/* 检查字符串类型比较操作是否合法 */
		if (tdengine_is_string_type((Node *)linitial(oe->args), &(fpinfo->slinfo)))
		{
			if (strcmp(cur_opname, "<") == 0 ||
				strcmp(cur_opname, ">") == 0 ||
				strcmp(cur_opname, "<=") == 0 ||
				strcmp(cur_opname, ">=") == 0)
			{
				return false;
			}
		}

		/* 检查是否为内置操作符 */
		if (!tdengine_is_builtin(oe->opno))
			return false;

		/* 检查是否包含时间列 */
		if (tdengine_contain_time_column(oe->args, &(fpinfo->slinfo)))
		{
			return false;
		}

		/* 递归检查子表达式 */
		if (!tdengine_foreign_expr_walker((Node *)oe->args,
										  glob_cxt, &inner_cxt))
			return false;

		/* 检查输入排序规则是否合法 */
		if (oe->inputcollid == InvalidOid)
			/* OK, inputs are all noncollatable */;
		else if (inner_cxt.state != FDW_COLLATE_SAFE ||
				 oe->inputcollid != inner_cxt.collation)
			return false;

		/* 输出总是布尔类型且无排序规则 */
		collation = InvalidOid;
		state = FDW_COLLATE_NONE;
	}
	break;
		/*
		 * 处理类型重标记节点(T_RelabelType)
		 * 功能: 检查类型转换表达式是否可下推到远程执行
		 *
		 * 处理流程:
		 *   1. 递归检查子表达式(类型转换的参数)
		 *   2. 获取类型转换后的排序规则ID
		 *   3. 检查排序规则是否合法:
		 *      - 无排序规则(InvalidOid): 标记为FDW_COLLATE_NONE
		 *      - 排序规则与子表达式一致: 标记为FDW_COLLATE_SAFE
		 *      - 其他情况: 标记为FDW_COLLATE_UNSAFE
		 *
		 * 注意事项:
		 *   - 类型重标记节点表示显式或隐式的类型转换
		 *   - 严格检查排序规则来源，防止引入不兼容的排序规则
		 */
	case T_RelabelType:
	{
		RelabelType *r = (RelabelType *)node;

		/* 递归检查子表达式 */
		if (!tdengine_foreign_expr_walker((Node *)r->arg,
										  glob_cxt, &inner_cxt))
			return false;

		/* 获取并检查类型转换后的排序规则 */
		collation = r->resultcollid;
		if (collation == InvalidOid)
			state = FDW_COLLATE_NONE;
		else if (inner_cxt.state == FDW_COLLATE_SAFE &&
				 collation == inner_cxt.collation)
			state = FDW_COLLATE_SAFE;
		else
			state = FDW_COLLATE_UNSAFE;
	}
	break;
		/*
		 * 处理布尔表达式节点(T_BoolExpr)
		 * 功能: 检查布尔表达式是否可下推到远程执行
		 *
		 * 处理流程:
		 *   1. 检查布尔操作类型:
		 *      - NOT操作符直接拒绝下推(TDengine不支持)
		 *   2. 递归检查子表达式
		 *   3. 特殊处理OR表达式:
		 *      - 包含时间列的OR表达式拒绝下推
		 *   4. 设置输出排序规则状态(布尔类型无排序规则)
		 *
		 * 注意事项:
		 *   - 布尔表达式包括AND/OR/NOT逻辑操作
		 *   - TDengine对布尔表达式的支持有限制
		 *   - 输出总是布尔类型且无排序规则
		 */
	case T_BoolExpr:
	{
		BoolExpr *b = (BoolExpr *)node;

		is_time_column = false;

		/* TDengine不支持NOT操作符 */
		if (b->boolop == NOT_EXPR)
		{
			return false;
		}

		/* 递归检查子表达式 */
		if (!tdengine_foreign_expr_walker((Node *)b->args,
										  glob_cxt, &inner_cxt))
			return false;

		/* 特殊处理OR表达式: 包含时间列则拒绝下推 */
		if (b->boolop == OR_EXPR && is_time_column)
		{
			is_time_column = false;
			return false;
		}

		/* 布尔类型无排序规则 */
		collation = InvalidOid;
		state = FDW_COLLATE_NONE;
	}
	break;
		/*
		 * 处理列表节点(T_List)
		 * 功能: 检查表达式列表是否可下推到远程执行
		 *
		 * 处理流程:
		 *   1. 继承外层上下文的标志位:
		 *      - 跳过类型转换标志(can_skip_cast)
		 *      - 填充函数启用标志(tdengine_fill_enable)
		 *      - 时间键标志(has_time_key)
		 *      - 加减操作符标志(has_sub_or_add_operator)
		 *      - 比较操作标志(is_comparison)
		 *   2. 递归检查列表中的每个子表达式
		 *   3. 从子表达式继承排序规则状态:
		 *      - 排序规则ID(collation)
		 *      - 排序规则安全状态(state)
		 *   4. 跳过对列表本身的类型检查
		 *
		 * 注意事项:
		 *   - 列表节点通常包含多个子表达式
		 *   - 排序规则状态由子表达式决定
		 *   - 不直接检查列表类型，只检查其元素
		 */
	case T_List:
	{
		List *l = (List *)node;
		ListCell *lc;

		/* 继承外层上下文标志 */
		inner_cxt.can_skip_cast = outer_cxt->can_skip_cast;
		inner_cxt.tdengine_fill_enable = outer_cxt->tdengine_fill_enable;
		inner_cxt.has_time_key = outer_cxt->has_time_key;
		inner_cxt.has_sub_or_add_operator = outer_cxt->has_sub_or_add_operator;
		inner_cxt.is_comparison = outer_cxt->is_comparison;

		/* 递归检查每个子表达式 */
		foreach (lc, l)
		{
			if (!tdengine_foreign_expr_walker((Node *)lfirst(lc),
											  glob_cxt, &inner_cxt))
				return false;
		}

		/* 从子表达式继承排序规则状态 */
		collation = inner_cxt.collation;
		state = inner_cxt.state;

		/* 不检查列表本身的类型 */
		check_type = false;
	}
	break;
	case T_Aggref:
	{
		Aggref *agg = (Aggref *)node;
		ListCell *lc;
		char *opername = NULL;
		bool old_val;
		int index_const = -1;
		int index;
		bool is_regex = false;
		bool is_star_func = false;
		bool is_not_star_func = false;
		Oid agg_inputcollid = agg->inputcollid;

		/* get function name and schema */
		opername = get_func_name(agg->aggfnoid);

		// TODO:
		/* these function can be passed to TDengine */
		if ((strcmp(opername, "sum") == 0 ||
			 strcmp(opername, "max") == 0 ||
			 strcmp(opername, "min") == 0 ||
			 strcmp(opername, "count") == 0 ||
			 strcmp(opername, "tdengine_distinct") == 0 || // TODO: 
			 strcmp(opername, "spread") == 0 ||
			 strcmp(opername, "sample") == 0 ||
			 strcmp(opername, "first") == 0 ||
			 strcmp(opername, "last") == 0 ||
			 strcmp(opername, "integral") == 0 ||
			 strcmp(opername, "mean") == 0 ||
			 strcmp(opername, "median") == 0 ||
			 strcmp(opername, "tdengine_count") == 0 || // TODO: 
			 strcmp(opername, "tdengine_mode") == 0 ||
			 strcmp(opername, "stddev") == 0 ||
			 strcmp(opername, "tdengine_sum") == 0 || // TODO: 
			 strcmp(opername, "tdengine_max") == 0 || // TODO: 
			 strcmp(opername, "tdengine_min") == 0))	// TODO: 
		{
			is_not_star_func = true;
		}

		is_star_func = tdengine_is_star_func(agg->aggfnoid, opername);

		if (!(is_star_func || is_not_star_func))
			return false;

		/* Some aggregate tdengine functions have a const argument. */
		if (strcmp(opername, "sample") == 0 ||
			strcmp(opername, "integral") == 0)
			index_const = 1;

		/*
		 * Only sum(), count() and spread() are aggregate functions,
		 * max(), min() and last() are selector functions（选择器函数）
		 */
		if (strcmp(opername, "sum") == 0 ||
			strcmp(opername, "spread") == 0 ||
			strcmp(opername, "count") == 0)
		{
			/* Mark target as aggregate function */
			glob_cxt->mixing_aggref_status |= TDENGINE_TARGETS_MARK_AGGREF;
		}

		/* Not safe to pushdown when not in grouping context */
		if (glob_cxt->foreignrel->reloptkind != RELOPT_UPPER_REL)
			return false;

		/* Only non-split aggregates are pushable. */
		// 只有简单的、未分割的聚合函数(AGGSPLIT_SIMPLE模式)才能被下推到远程执行。
		// 分割的聚合函数(如并行处理中的部分聚合)不被支持。
		if (agg->aggsplit != AGGSPLIT_SIMPLE)
			return false;

		/*
		 * Save value of is_time_column before we check time argument
		 * aggregate.
		 */
		old_val = is_time_column;
		is_time_column = false;

		/*
		 * Recurse to input args. aggdirectargs, aggorder and
		 * aggdistinct are all present in args, so no need to check
		 * their shippability explicitly.
		 */
		index = -1;
		foreach (lc, agg->args)
		{
			Node *n = (Node *)lfirst(lc);
			OpExpr *oe = (OpExpr *)NULL;
			Oid resulttype = InvalidOid;
			bool is_slvar = false;

			index++;

			/* If TargetEntry, extract the expression from it */
			if (IsA(n, TargetEntry))
			{
				TargetEntry *tle = (TargetEntry *)n;

				n = (Node *)tle->expr;

				if (IsA(n, Var) ||
					((index == index_const) && IsA(n, Const)))
					/* arguments checking is OK */;
				else if (IsA(n, Const))
				{
					Const *arg = (Const *)n;
					char *extval;

					if (arg->consttype == TEXTOID)
					{
						is_regex = tdengine_is_regex_argument(arg, &extval);
						if (is_regex)
							/* arguments checking is OK */;
						else
							return false;
					}
					else
						return false;
				}
				else if (fpinfo->slinfo.schemaless &&
						 (IsA(n, CoerceViaIO) || IsA(n, OpExpr)))
				{
					if (IsA(n, OpExpr))
					{
						oe = (OpExpr *)n;
						resulttype = oe->opresulttype;
					}
					else
					{
						/* CoerceViaIO */
						CoerceViaIO *cio = (CoerceViaIO *)n;
						oe = (OpExpr *)cio->arg;
						resulttype = cio->resulttype;
					}

					if (tdengine_is_slvar_fetch((Node *)oe, &(fpinfo->slinfo)))
						is_slvar = true;
					else
						return false;
				}
				else if (is_star_func)
					/* arguments checking is OK */;
				else
					return false;
			}

			/* Check if arg is Var */
			if (IsA(n, Var) || is_slvar)
			{
				Var *var;
				char *colname;

				if (is_slvar)
				{
					Const *cnst;

					var = linitial_node(Var, oe->args);
					cnst = lsecond_node(Const, oe->args);
					colname = TextDatumGetCString(cnst->constvalue);
					agg_inputcollid = var->varcollid;
				}
				else
				{
					var = (Var *)n;

					colname = tdengine_get_column_name(glob_cxt->relid, var->varattno);
					resulttype = var->vartype;
				}

				/* Not push down if arg is tag key */
				if (tdengine_is_tag_key(colname, glob_cxt->relid))
					return false;

				/*
				 * Not push down max(), min() if arg type is text
				 * column
				 */
				if ((strcmp(opername, "max") == 0 || strcmp(opername, "min") == 0) && (resulttype == TEXTOID || resulttype == InvalidOid))
					return false;
			}

			if (!tdengine_foreign_expr_walker(n, glob_cxt, &inner_cxt))
				return false;

			/*
			 * Does not pushdown time column argument within aggregate
			 * function except time related functions, because these
			 * functions are converted from func(time, value) to
			 * func(value) when deparsing.
			 */
			if (is_time_column && !(strcmp(opername, "last") == 0 || strcmp(opername, "first") == 0))
			{
				is_time_column = false;
				return false;
			}
		}

		/*
		 * If there is no time column argument within aggregate
		 * function, restore value of is_time_column.
		 */
		is_time_column = old_val;

		if (agg->aggorder || agg->aggfilter)
		{
			return false;
		}

		/*
		 * tdengine_fdw only supports push-down DISTINCT within
		 * aggregate for count()
		 */
		if (agg->aggdistinct && (strcmp(opername, "count") != 0))
			return false;

		if (is_regex)
			check_type = false;
		else
		{
			/*
			 * If aggregate's input collation is not derived from a
			 * foreign Var, it can't be sent to remote.
			 */
			if (agg_inputcollid == InvalidOid)
				/* OK, inputs are all noncollatable */;
			else if (inner_cxt.state != FDW_COLLATE_SAFE ||
					 agg_inputcollid != inner_cxt.collation)
				return false;
		}

		/*
		 * Detect whether node is introducing a collation not derived
		 * from a foreign Var.  (If so, we just mark it unsafe for now
		 * rather than immediately returning false, since the parent
		 * node might not care.)
		 */
		collation = agg->aggcollid;
		if (collation == InvalidOid)
			state = FDW_COLLATE_NONE;
		else if (inner_cxt.state == FDW_COLLATE_SAFE &&
				 collation == inner_cxt.collation)
			state = FDW_COLLATE_SAFE;
		else if (collation == DEFAULT_COLLATION_OID)
			state = FDW_COLLATE_NONE;
		else
			state = FDW_COLLATE_UNSAFE;
	}
	break;
		/*
		 * 处理类型转换节点(T_CoerceViaIO)
		 * 功能: 检查类型转换表达式是否可下推到远程执行
		 *
		 * 处理流程:
		 *   1. 检查无模式变量与时间类型的比较操作:
		 *      - 避免下推时间键与标签/字段的时间表达式比较(如: time key = (fields->>'c2')::timestamp + interval '1d')
		 *   2. 检查参数是否为无模式变量或参数获取表达式:
		 *      - 是: 递归检查子表达式
		 *      - 否: 拒绝下推
		 *   3. 设置输出排序规则状态(类型转换无排序规则)
		 *
		 * 注意事项:
		 *   - 主要用于处理显式的::类型转换操作
		 *   - 严格限制时间类型与无模式变量的比较操作下推
		 *   - 输出总是无排序规则状态
		 */
	case T_CoerceViaIO:
	{
		CoerceViaIO *cio = (CoerceViaIO *)node;
		Node *arg = (Node *)cio->arg;

		/* 检查时间键与无模式变量的时间类型比较 */
		if (tdengine_is_slvar_fetch(arg, &(fpinfo->slinfo)))
		{
			if (TDENGINE_IS_TIME_TYPE(cio->resulttype))
			{
				/* 如果是比较操作且包含时间键和加减操作符则拒绝下推 */
				if (outer_cxt->is_comparison &&
					outer_cxt->has_sub_or_add_operator &&
					outer_cxt->has_time_key)
				{
					return false;
				}
			}
		}

		/* 只允许无模式变量或参数获取表达式下推 */
		if (tdengine_is_slvar_fetch(arg, &(fpinfo->slinfo)) ||
			tdengine_is_param_fetch(arg, &(fpinfo->slinfo)))
		{
			/* 递归检查子表达式 */
			if (!tdengine_foreign_expr_walker(arg, glob_cxt, &inner_cxt))
				return false;
		}
		else
		{
			return false;
		}

		/* 类型转换无排序规则 */
		collation = InvalidOid;
		state = FDW_COLLATE_NONE;
	}
	break;
		/*
		 * 处理空值测试节点(T_NullTest)
		 * 功能: 检查IS NULL/IS NOT NULL表达式是否可下推到远程执行
		 *
		 * 处理流程:
		 *   1. 获取无模式变量列名(tdengine_get_slvar)
		 *   2. 检查列名是否存在且是否为标签键(tag key):
		 *      - 不是标签键则拒绝下推
		 *   3. 设置输出排序规则状态(布尔类型无排序规则)
		 *
		 * 注意事项:
		 *   - 仅支持对标签键(tag key)的空值测试
		 *   - 输出总是布尔类型且无排序规则
		 *   - 主要用于处理TDengine标签列的IS NULL/IS NOT NULL条件
		 */
	case T_NullTest:
	{
		NullTest *nt = (NullTest *)node;
		char *colname;

		/* 获取无模式变量列名 */
		colname = tdengine_get_slvar(nt->arg, &(fpinfo->slinfo));

		/* 检查是否为标签键 */
		if (colname == NULL || !tdengine_is_tag_key(colname, glob_cxt->relid))
			return false;

		/* 布尔类型无排序规则 */
		collation = InvalidOid;
		state = FDW_COLLATE_NONE;
	}
	break;
		/*
		 * 处理数组表达式节点(T_ArrayExpr)
		 * 功能: 检查数组表达式是否可下推到远程执行
		 *
		 * 处理流程:
		 *   1. 递归检查数组元素子表达式
		 *   2. 获取数组的排序规则ID(array_collid)
		 *   3. 检查排序规则状态:
		 *      - 无排序规则(InvalidOid): 标记为FDW_COLLATE_NONE
		 *      - 排序规则与子表达式一致: 标记为FDW_COLLATE_SAFE
		 *      - 默认排序规则: 标记为FDW_COLLATE_NONE
		 *      - 其他情况: 标记为FDW_COLLATE_UNSAFE
		 *
		 * 注意事项:
		 *   - 数组表达式必须从输入变量继承排序规则
		 *   - 与函数表达式使用相同的排序规则检查逻辑
		 *   - 默认排序规则被视为无排序规则
		 */
	case T_ArrayExpr:
	{
		ArrayExpr *a = (ArrayExpr *)node;

		/* 递归检查数组元素 */
		if (!tdengine_foreign_expr_walker((Node *)a->elements,
										  glob_cxt, &inner_cxt))
			return false;

		/* 检查数组排序规则 */
		collation = a->array_collid;
		if (collation == InvalidOid)
			state = FDW_COLLATE_NONE;
		else if (inner_cxt.state == FDW_COLLATE_SAFE &&
				 collation == inner_cxt.collation)
			state = FDW_COLLATE_SAFE;
		else if (collation == DEFAULT_COLLATION_OID)
			state = FDW_COLLATE_NONE;
		else
			state = FDW_COLLATE_UNSAFE;
	}
	break;

	case T_DistinctExpr:
		/* IS DISTINCT FROM */
		return false;
	default:

		/*
		 * If it's anything else, assume it's unsafe.  This list can be
		 * expanded later, but don't forget to add deparse support below.
		 */
		return false;
	}

	/*
	 * 如果表达式的返回类型不是内置类型，则不能下推到远程执行，
	 * 因为可能在远程端有不兼容的语义
	 */
	if (check_type && !tdengine_is_builtin(exprType(node)))
		return false;

	/*
	 * 将当前节点的排序规则信息合并到父节点的状态中
	 */
	if (state > outer_cxt->state)
	{
		/* 覆盖父节点之前的排序规则状态 */
		outer_cxt->collation = collation;
		outer_cxt->state = state;
	}
	else if (state == outer_cxt->state)
	{
		/* 合并排序规则状态，或检测排序规则冲突 */
		switch (state)
		{
		case FDW_COLLATE_NONE:
			/* 无排序规则 + 无排序规则 = 仍无排序规则 */
			break;
		case FDW_COLLATE_SAFE:
			if (collation != outer_cxt->collation)
			{
				/*
				 * 非默认排序规则总是覆盖默认排序规则
				 */
				if (outer_cxt->collation == DEFAULT_COLLATION_OID)
				{
					/* 覆盖父节点之前的排序规则状态 */
					outer_cxt->collation = collation;
				}
				else if (collation != DEFAULT_COLLATION_OID)
				{
					/*
					 * 排序规则冲突，将状态标记为不确定
					 * 不立即返回false，因为父节点可能不关心排序规则
					 */
					outer_cxt->state = FDW_COLLATE_UNSAFE;
				}
			}
			break;
		case FDW_COLLATE_UNSAFE:
			/* 仍然存在冲突状态... */
			break;
		}
	}

	/* It looks OK */
	return true;
}

/*
 * tdengine_build_tlist_to_deparse - 构建用于反解析为SELECT子句的目标列表
 *
 * 参数:
 *   @foreignrel: 外部关系信息(RelOptInfo结构指针)
 *
 * 返回值:
 *   List* - 包含需要从外部服务器获取的列和表达式的目标列表
 *
 * 功能说明:
 *   1. 对于上层关系(RELOPT_UPPER_REL)，直接返回预构建的分组目标列表
 *   2. 对于基础关系，构建包含以下内容的目标列表:
 *      - 关系目标表达式中的列
 *      - 评估本地条件所需的列
 *   3. 返回构建完成的目标列表
 *
 * 注意事项:
 *   - 上层关系的目标列表在检查可下推性时已构建
 *   - 基础关系需要动态构建目标列表
 */
List *
tdengine_build_tlist_to_deparse(RelOptInfo *foreignrel)
{
	/* 初始化空目标列表 */
	List *tlist = NIL;
	/* 获取外部表私有信息 */
	TDengineFdwRelationInfo *fpinfo = (TDengineFdwRelationInfo *)foreignrel->fdw_private;
	ListCell *lc;

	/*
	 * 处理上层关系(如分组、聚合等):
	 * 直接返回预构建的分组目标列表，避免重复构建
	 */
	if (foreignrel->reloptkind == RELOPT_UPPER_REL)
		return fpinfo->grouped_tlist;

	/*
	 * 构建基础关系目标列表:
	 * 1. 添加关系目标表达式中的所有变量
	 * 2. 添加评估本地条件所需的所有变量
	 */
	tlist = add_to_flat_tlist(tlist,
							  pull_var_clause((Node *)foreignrel->reltarget->exprs,
											  PVC_RECURSE_PLACEHOLDERS));

	/* 遍历所有本地条件，提取其中引用的变量 */
	foreach (lc, fpinfo->local_conds)
	{
		RestrictInfo *rinfo = lfirst_node(RestrictInfo, lc);

		/* 将条件子句中的变量添加到目标列表 */
		tlist = add_to_flat_tlist(tlist,
								  pull_var_clause((Node *)rinfo->clause,
												  PVC_RECURSE_PLACEHOLDERS));
	}

	/* 返回构建完成的目标列表 */
	return tlist;
}

/*
 * 反解析远程DELETE语句
 * 功能: 构建TDengine兼容的DELETE语句并输出到缓冲区
 *
 * 参数:
 *   @buf: 输出缓冲区，用于存储生成的SQL语句
 *   @root: 规划器信息，包含查询树和规划上下文
 *   @rtindex: 范围表索引，标识要删除的表
 *   @rel: 关系描述符，包含表结构信息
 *   @attname: 属性名称列表，用于WHERE条件
 *
 * 处理流程:
 *   1. 初始化DELETE语句基础部分
 *   2. 添加表名引用
 *   3. 遍历属性列表构建WHERE条件
 *   4. 输出调试日志
 */
void
tdengine_deparse_delete(StringInfo buf, PlannerInfo *root,
                        Index rtindex, Relation rel,
                        List *attname)
{
    int         i = 0;          // 参数计数器，用于构建参数占位符($1, $2等)
    ListCell   *lc;             // 列表迭代器

    /* 添加DELETE FROM关键字 */
    appendStringInfoString(buf, "DELETE FROM ");
    
    /* 反解析表名并添加到缓冲区 */
    tdengine_deparse_relation(buf, rel);

    /* 遍历属性列表构建WHERE条件 */
    foreach(lc, attname)
    {
        int         attnum = lfirst_int(lc);  // 获取当前属性编号

        /* 添加WHERE或AND连接符(第一个条件用WHERE，后续用AND) */
        appendStringInfo(buf, i == 0 ? " WHERE " : " AND ");
        
        /* 反解析列引用(表名.列名形式) */
        tdengine_deparse_column_ref(buf, rtindex, attnum, -1, root, false, false);
        
        /* 添加参数占位符($1, $2等) */
        appendStringInfo(buf, "=$%d", i + 1);
        i++;  // 递增参数计数器
    }

    /* 输出生成的DELETE语句到调试日志 */
    elog(DEBUG1, "delete:%s", buf->data);
}


/*
 * 反解析SELECT语句
 * 功能: 根据给定的关系、目标列和条件构建TDengine兼容的SELECT语句
 *
 * 参数:
 *   @buf: 输出缓冲区，用于存储生成的SQL语句
 *   @root: 规划器信息，包含查询树和规划上下文
 *   @rel: 关系信息(基础表、连接或上层关系)
 *   @tlist: 目标列列表(仅用于非基础表关系)
 *   @remote_conds: 远程条件列表(WHERE/HAVING子句)
 *   @pathkeys: 排序路径键列表(ORDER BY子句)
 *   @is_subquery: 是否为子查询
 *   @retrieved_attrs: 输出参数，返回选择的列索引列表
 *   @params_list: 输出参数，返回需要作为参数传递的值列表
 *   @has_limit: 是否包含LIMIT子句
 *
 * 处理流程:
 *   1. 初始化反解析上下文(context)
 *   2. 构建SELECT子句:
 *      a. 基础表: 使用fpinfo->attrs_used确定选择的列
 *      b. 连接/上层关系: 使用传入的tlist
 *   3. 构建FROM子句:
 *      a. 基础表: 直接使用表名
 *      b. 连接关系: 反解析连接表达式
 *   4. 构建WHERE/HAVING子句:
 *      a. 上层关系: 使用HAVING子句
 *      b. 其他关系: 使用WHERE子句
 *   5. 构建GROUP BY子句(上层关系)
 *   6. 构建ORDER BY子句(如果有pathkeys)
 *   7. 构建LIMIT子句(如果有)
 */
void tdengine_deparse_select_stmt_for_rel(StringInfo buf, PlannerInfo *root, RelOptInfo *rel,
                                         List *tlist, List *remote_conds, List *pathkeys,
                                         bool is_subquery, List **retrieved_attrs,
                                         List **params_list,
                                         bool has_limit)
{
    // 反解析上下文结构体
    deparse_expr_cxt context;
    // 获取FDW关系私有信息
    TDengineFdwRelationInfo *fpinfo = (TDengineFdwRelationInfo *)rel->fdw_private;
    // 条件表达式列表
    List *quals;

    /*
     * 验证关系类型:
     * 支持基础表(RELOPT_BASEREL)、连接关系(RELOPT_JOINREL)、
     * 其他成员关系(RELOPT_OTHER_MEMBER_REL)和上层关系(RELOPT_UPPER_REL)
     */
    Assert(rel->reloptkind == RELOPT_JOINREL ||
           rel->reloptkind == RELOPT_BASEREL ||
           rel->reloptkind == RELOPT_OTHER_MEMBER_REL ||
           rel->reloptkind == RELOPT_UPPER_REL);

    /* 初始化反解析上下文 */
    context.buf = buf;           // 输出缓冲区
    context.root = root;         // 规划器信息
    context.foreignrel = rel;    // 当前反解析的关系
    // 上层关系使用外部关系作为扫描关系，其他使用自身
    context.scanrel = (rel->reloptkind == RELOPT_UPPER_REL) ? fpinfo->outerrel : rel;
    context.params_list = params_list;  // 参数列表
    context.op_type = UNKNOWN_OPERATOR; // 操作符类型初始化为未知
    context.is_tlist = false;     // 是否在处理目标列表
    context.can_skip_cast = false; // 是否可以跳过类型转换
    context.convert_to_timestamp = false; // 是否转换为时间戳
    context.has_bool_cmp = false; // 是否有布尔比较

    /* 构建SELECT子句 */
    tdengine_deparse_select(tlist, retrieved_attrs, &context);

    /*
     * 处理条件表达式:
     * 上层关系使用底层扫描关系的远程条件
     * 其他关系直接使用传入的remote_conds
     */
    if (rel->reloptkind == RELOPT_UPPER_REL)
    {
        // 获取外部关系的FDW信息
        TDengineFdwRelationInfo *ofpinfo = (TDengineFdwRelationInfo *)fpinfo->outerrel->fdw_private;
        quals = ofpinfo->remote_conds;  // 使用外部关系的远程条件
    }
    else
    {
        quals = remote_conds;  // 直接使用传入的条件
    }

    /* 构建FROM和WHERE子句 */
    tdengine_deparse_from_expr(quals, &context);

    /* 处理上层关系的特殊子句 */
    if (rel->reloptkind == RELOPT_UPPER_REL)
    {
        /* 添加GROUP BY子句 */
        tdengine_append_group_by_clause(tlist, &context);

        /* 添加HAVING子句(如果有远程条件) */
        if (remote_conds)
        {
            appendStringInfo(buf, " HAVING ");  // 添加HAVING关键字
            tdengine_append_conditions(remote_conds, &context);  // 反解析条件
        }
    }

    /* 添加ORDER BY子句(如果有pathkeys) */
    if (pathkeys)
        tdengine_append_order_by_clause(pathkeys, &context);

    /* 添加LIMIT子句(如果需要) */
    if (has_limit)
        tdengine_append_limit_clause(&context);
}


/**
 * get_proname - 根据函数OID获取函数名称并添加到输出缓冲区
 *
 * 功能: 通过查询pg_proc系统目录，获取指定OID的函数名称并添加到输出字符串缓冲区
 *
 * 参数:
 *   @oid: 输入参数，要查询的函数OID
 *   @proname: 输出参数，用于存储函数名称的StringInfo缓冲区
 *
 * 处理流程:
 *   1. 通过PROCOID在系统缓存中查找函数元组
 *   2. 检查元组有效性，无效则报错
 *   3. 从元组中提取函数名称
 *   4. 将函数名称添加到输出缓冲区
 *   5. 释放系统缓存元组
 */
static void
get_proname(Oid oid, StringInfo proname)
{
	HeapTuple proctup;	   // 函数元组指针
	Form_pg_proc procform; // 函数元组结构体
	const char *name;	   // 函数名称字符串

	// 在系统缓存中查找函数元组
	proctup = SearchSysCache1(PROCOID, ObjectIdGetDatum(oid));
	// 检查元组有效性
	if (!HeapTupleIsValid(proctup))
		elog(ERROR, "cache lookup failed for function %u", oid);

	// 获取函数元组结构体
	procform = (Form_pg_proc)GETSTRUCT(proctup);

	// 从结构体中提取函数名称
	name = NameStr(procform->proname);
	// 将函数名称添加到输出缓冲区
	appendStringInfoString(proname, name);

	// 释放系统缓存元组
	ReleaseSysCache(proctup);
}

/*
 * 反解析SELECT语句
 * 功能: 根据查询计划构建TDengine兼容的SELECT语句
 *
 * 参数:
 *   @tlist: 目标列列表(TargetEntry节点列表)
 *   @retrieved_attrs: 输出参数，返回从远程服务器获取的属性列表
 *   @context: 反解析上下文，包含输出缓冲区和相关状态
 *
 * 处理流程:
 *   1. 初始化SELECT关键字
 *   2. 根据关系类型选择不同的处理方式:
 *      a. 连接关系(RELOPT_JOINREL)或上层关系(RELOPT_UPPER_REL):
 *         - 使用显式目标列表反解析(tdengine_deparse_explicit_target_list)
 *      b. 基础关系:
 *         - 打开表获取关系描述符
 *         - 根据是否为无模式表选择不同的反解析方式:
 *           * 无模式表: 使用tdengine_deparse_target_list_schemaless
 *           * 普通表: 使用tdengine_deparse_target_list
 *         - 关闭表
 *
 * 注意事项:
 *   - 连接关系和上层关系直接使用输入的目标列表
 *   - 基础关系使用fpinfo->attrs_used确定需要获取的列
 *   - 无模式表需要特殊处理
 */
static void
tdengine_deparse_select(List *tlist, List **retrieved_attrs, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;														  // 输出缓冲区
	PlannerInfo *root = context->root;													  // 规划器信息
	RelOptInfo *foreignrel = context->foreignrel;										  // 外部关系信息
	TDengineFdwRelationInfo *fpinfo = (TDengineFdwRelationInfo *)foreignrel->fdw_private; // FDW私有信息

	/* 添加SELECT关键字 */
	appendStringInfoString(buf, "SELECT ");

	/* 处理连接关系或上层关系 */
	if (foreignrel->reloptkind == RELOPT_JOINREL ||
		fpinfo->is_tlist_func_pushdown == true ||
		foreignrel->reloptkind == RELOPT_UPPER_REL)
	{
		/*
		 * 对于连接关系或上层关系，直接使用输入的目标列表
		 * 因为这些关系已经确定了需要从远程服务器获取的列
		 */
		tdengine_deparse_explicit_target_list(tlist, retrieved_attrs, context);
	}
	else
	{
		/*
		 * 对于基础关系，使用fpinfo->attrs_used确定需要获取的列
		 */
		RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root); // 获取范围表条目

		/*
		 * 核心代码已经对每个关系持有锁，所以这里可以使用NoLock
		 */
		Relation rel = table_open(rte->relid, NoLock); // 打开表

		/* 根据是否为无模式表选择不同的反解析方式 */
		if (fpinfo->slinfo.schemaless)
			tdengine_deparse_target_list_schemaless(buf, rel, rte->relid,
													fpinfo->attrs_used, retrieved_attrs,
													fpinfo->all_fieldtag,
													fpinfo->slcols);
		else
			tdengine_deparse_target_list(buf, root, foreignrel->relid, rel, fpinfo->attrs_used, retrieved_attrs);

		table_close(rel, NoLock); // 关闭表
	}
}

/*
 * 反解析FROM子句表达式
 * 功能: 构建SQL语句中的FROM子句和可选的WHERE子句
 *
 * 参数:
 *   @quals: 条件表达式列表，将用于构建WHERE子句
 *   @context: 反解析上下文，包含输出缓冲区和相关状态信息
 *
 * 处理流程:
 *   1. 验证扫描关系的类型(对于上层关系必须是连接关系或基础关系)
 *   2. 构建FROM子句:
 *      a. 添加"FROM"关键字
 *      b. 调用tdengine_deparse_from_expr_for_rel反解析关系表达式
 *   3. 如果存在条件表达式:
 *      a. 添加"WHERE"关键字
 *      b. 调用tdengine_append_conditions反解析条件表达式
 *
 * 注意事项:
 *   - 对于上层关系，扫描关系必须是连接关系或基础关系
 *   - 当quals参数为空列表(NIL)时，不生成WHERE子句
 *   - 条件表达式之间默认使用AND连接
 */
static void
tdengine_deparse_from_expr(List *quals, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;			// 输出缓冲区
	RelOptInfo *scanrel = context->scanrel; // 扫描关系信息

	/* 验证上层关系的扫描关系类型 */
	Assert(context->foreignrel->reloptkind != RELOPT_UPPER_REL ||
		   scanrel->reloptkind == RELOPT_JOINREL ||
		   scanrel->reloptkind == RELOPT_BASEREL);

	/* 构建FROM子句 */
	appendStringInfoString(buf, " FROM ");
	tdengine_deparse_from_expr_for_rel(buf, context->root, scanrel,
									   (bms_num_members(scanrel->relids) > 1),
									   context->params_list);

	/* 构建WHERE子句(如果存在条件表达式) */
	if (quals != NIL)
	{
		appendStringInfo(buf, " WHERE ");
		tdengine_append_conditions(quals, context);
	}
}

/*
 * 反解析条件表达式列表
 * 功能: 将PostgreSQL条件表达式列表转换为TDengine兼容的SQL条件片段
 *
 * 参数:
 *   @exprs: 条件表达式列表(Expr节点列表)
 *   @context: 反解析上下文，包含输出缓冲区和相关状态
 *
 * 处理流程:
 *   1. 设置传输模式确保常量值可移植输出
 *   2. 遍历表达式列表中的每个条件:
 *      a. 从RestrictInfo中提取实际条件子句(如果需要)
 *      b. 使用"AND"连接多个条件表达式
 *      c. 为每个条件添加括号
 *      d. 调用tdengine_deparse_expr反解析条件表达式
 *   3. 重置传输模式
 *
 * 注意事项:
 *   - 条件表达式列表中的条件默认为AND关系
 *   - 用于WHERE子句、JOIN ON子句和HAVING子句的反解析
 *   - 每个条件都会被括号包围以确保优先级
 *   - 临时设置has_bool_cmp标志用于布尔比较处理
 */
static void
tdengine_append_conditions(List *exprs, deparse_expr_cxt *context)
{
	int nestlevel;				   // GUC嵌套级别
	ListCell *lc;				   // 列表迭代器
	bool is_first = true;		   // 是否是第一个条件
	StringInfo buf = context->buf; // 输出缓冲区

	/* 设置传输模式确保常量值可移植输出 */
	nestlevel = tdengine_set_transmission_modes();

	/* 遍历条件表达式列表 */
	foreach (lc, exprs)
	{
		Expr *expr = (Expr *)lfirst(lc); // 获取当前条件表达式

		/* 从RestrictInfo中提取实际条件子句 */
		if (IsA(expr, RestrictInfo))
			expr = ((RestrictInfo *)expr)->clause;

		/* 使用"AND"连接多个条件表达式 */
		if (!is_first)
			appendStringInfoString(buf, " AND ");

		/* 设置布尔比较标志 */
		context->has_bool_cmp = true;

		/* 为条件添加括号并反解析 */
		appendStringInfoChar(buf, '(');
		tdengine_deparse_expr(expr, context);
		appendStringInfoChar(buf, ')');

		/* 重置布尔比较标志 */
		context->has_bool_cmp = false;

		is_first = false; // 标记已处理第一个条件
	}

	/* 重置传输模式 */
	tdengine_reset_transmission_modes(nestlevel);
}

/*
 * 反解析显式目标列表到SQL SELECT语句
 * 功能: 将PostgreSQL查询计划中的目标列表转换为TDengine兼容的SELECT列列表
 *
 * 参数:
 *   @tlist: 目标条目列表(TargetEntry节点列表)
 *   @retrieved_attrs: 输出参数，返回获取的属性索引列表
 *   @context: 反解析上下文，包含输出缓冲区和相关状态
 *
 * 处理流程:
 *   1. 初始化目标列表和状态变量
 *   2. 遍历每个目标条目:
 *      a. 检查是否为无模式变量(schemaless var)
 *      b. 检查是否为分组目标列
 *      c. 处理不同类型的目标表达式:
 *         - 聚合函数
 *         - 操作符表达式
 *         - 函数调用
 *         - 变量引用
 *      d. 检查是否需要添加字段键(field key)
 *   3. 处理特殊情况(全字段标签或空列表)
 *   4. 返回获取的属性索引列表
 *
 * 注意事项:
 *   - 分组目标列不会直接出现在SELECT列表中
 *   - 如果所有目标列都是标签键，需要额外添加一个字段键
 *   - 特殊处理无模式表查询
 */
static void
tdengine_deparse_explicit_target_list(List *tlist, List **retrieved_attrs,
									  deparse_expr_cxt *context)
{
	ListCell *lc;																				   // 列表迭代器
	StringInfo buf = context->buf;																   // 输出缓冲区
	int i = 0;																					   // 属性计数器
	bool first = true;																			   // 是否是第一个列
	bool is_col_grouping_target = false;														   // 是否是分组目标列
	bool need_field_key = true;																	   // 是否需要添加字段键
	bool is_need_comma = false;																	   // 是否需要添加逗号分隔符
	bool selected_all_fieldtag = false;															   // 是否选择了所有字段标签
	TDengineFdwRelationInfo *fpinfo = (TDengineFdwRelationInfo *)context->foreignrel->fdw_private; // FDW关系信息

	*retrieved_attrs = NIL; // 初始化返回的属性索引列表

	/*
	 * 我们不直接在SELECT语句中构造分组目标列，
	 * 而是检查是否需要额外添加字段键
	 */
	context->is_tlist = true; // 标记当前正在处理目标列表

	/* 遍历目标列表中的每个条目 */
	foreach (lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc); // 获取当前目标条目
		bool is_slvar = false;							 // 是否是无模式变量

		/* 检查是否是无模式变量 */
		if (tdengine_is_slvar_fetch((Node *)tle->expr, &(fpinfo->slinfo)))
			is_slvar = true;

		/* 检查是否是分组目标列 */
		if (!fpinfo->is_tlist_func_pushdown && IsA((Expr *)tle->expr, Var))
		{
			is_col_grouping_target = tdengine_is_grouping_target(tle, context->root->parse);
		}

		/* 处理无模式变量的分组目标检查 */
		if (is_slvar)
		{
			is_col_grouping_target = tdengine_is_grouping_target(tle, context->root->parse);
		}

		/* 处理不同类型的表达式 */
		if (IsA((Expr *)tle->expr, Aggref) ||										// 聚合函数
			(IsA((Expr *)tle->expr, OpExpr) && !is_slvar) ||						// 操作符表达式(非无模式变量)
			IsA((Expr *)tle->expr, FuncExpr) ||										// 函数调用
			((IsA((Expr *)tle->expr, Var) || is_slvar) && !is_col_grouping_target)) // 变量引用(非分组目标)
		{
			bool is_skip_expr = false; // 是否跳过当前表达式

			/* 特殊处理某些函数调用 */
			if (IsA((Expr *)tle->expr, FuncExpr))
			{
				FuncExpr *fe = (FuncExpr *)tle->expr;
				StringInfo func_name = makeStringInfo();

				get_proname(fe->funcid, func_name);
				/* 跳过特定函数 */
				if (strcmp(func_name->data, "tdengine_time") == 0 ||
					strcmp(func_name->data, "tdengine_fill_numeric") == 0 ||
					strcmp(func_name->data, "tdengine_fill_option") == 0)
					is_skip_expr = true;
			}

			/* 添加逗号分隔符(如果需要) */
			if (is_need_comma && !is_skip_expr)
				appendStringInfoString(buf, ", ");
			need_field_key = false; // 标记不需要额外字段键

			if (!is_skip_expr)
			{
				if (fpinfo->is_tlist_func_pushdown && fpinfo->all_fieldtag)
					selected_all_fieldtag = true; // 标记选择了所有字段标签
				else
				{
					first = false;
					/* 反解析表达式到输出缓冲区 */
					tdengine_deparse_expr((Expr *)tle->expr, context);
					is_need_comma = true;
				}
			}
		}

		/*
		 * 检查所有目标列是否都是标签键，
		 * 如果是则需要额外添加一个字段键
		 */
		if (IsA((Expr *)tle->expr, Var) && need_field_key)
		{
			RangeTblEntry *rte = planner_rt_fetch(context->scanrel->relid, context->root);
			char *colname = tdengine_get_column_name(rte->relid, ((Var *)tle->expr)->varattno);

			if (!tdengine_is_tag_key(colname, rte->relid))
				need_field_key = false; // 发现非标签键列，不需要额外字段键
		}

		/* 将属性索引添加到返回列表 */
		*retrieved_attrs = lappend_int(*retrieved_attrs, i + 1);
		i++;
	}
	context->is_tlist = false; // 结束目标列表处理

	/* 处理特殊情况 */
	if (i == 0 || selected_all_fieldtag)
	{
		appendStringInfoString(buf, "*"); // 空列表或全字段标签时使用*
		return;
	}

	/* 如果所有目标列都是标签键，添加一个字段键 */
	if (need_field_key)
	{
		RangeTblEntry *rte = planner_rt_fetch(context->scanrel->relid, context->root);
		Relation rel = table_open(rte->relid, NoLock);
		TupleDesc tupdesc = RelationGetDescr(rel);

		/* 添加字段键到输出 */
		tdengine_append_field_key(tupdesc, context->buf, context->scanrel->relid, context->root, first);

		table_close(rel, NoLock);
		return;
	}
}

/*
 * 反解析FROM子句表达式
 * 功能: 根据关系类型构建FROM子句的SQL片段
 *
 * 参数:
 *   @buf: 输出字符串缓冲区
 *   @root: 规划器信息
 *   @foreignrel: 外部关系信息
 *   @use_alias: 是否使用别名(当前未使用)
 *   @params_list: 参数列表(当前未使用)
 *
 * 处理流程:
 *   1. 检查关系类型:
 *      a. 连接关系(RELOPT_JOINREL): 当前不支持，触发断言
 *      b. 基础关系:
 *         - 获取范围表条目(RangeTblEntry)
 *         - 以NoLock模式打开表
 *         - 调用tdengine_deparse_relation反解析表名
 *         - 关闭表
 *
 * 注意事项:
 *   - 当前仅支持基础关系的反解析
 *   - 连接关系下推功能尚未实现
 *   - 使用NoLock模式打开表，因为规划阶段已持有锁
 */
static void
tdengine_deparse_from_expr_for_rel(StringInfo buf, PlannerInfo *root, RelOptInfo *foreignrel,
								   bool use_alias, List **params_list)
{
	Assert(!use_alias); // 确保不使用别名(当前实现限制)
	if (foreignrel->reloptkind == RELOPT_JOINREL)
	{
		/* 连接关系下推当前不支持 */
		Assert(false); // 触发断言失败
	}
	else
	{
		/* 获取范围表条目 */
		RangeTblEntry *rte = planner_rt_fetch(foreignrel->relid, root);

		/*
		 * 核心代码已经对每个关系持有锁，所以这里可以使用NoLock
		 */
		Relation rel = table_open(rte->relid, NoLock); // 打开表

		/* 反解析关系名称到输出缓冲区 */
		tdengine_deparse_relation(buf, rel);

		table_close(rel, NoLock); // 关闭表
	}
}

void tdengine_deparse_analyze(StringInfo sql, char *dbname, char *relname)
{
	appendStringInfo(sql, "SELECT");
	appendStringInfo(sql, " round(((data_length + index_length)), 2)");
	appendStringInfo(sql, " FROM information_schema.TABLES");
	appendStringInfo(sql, " WHERE table_schema = '%s' AND table_name = '%s'", dbname, relname);
}

/*
 * 反解析目标列列表，生成SELECT语句中的列名列表
 *
 * 参数:
 *   @buf: 输出字符串缓冲区
 *   @root: 规划器信息
 *   @rtindex: 范围表索引
 *   @rel: 关系描述符
 *   @attrs_used: 位图集，表示需要使用的属性列
 *   @retrieved_attrs: 输出参数，返回实际获取的属性编号列表
 */
static void
tdengine_deparse_target_list(StringInfo buf,
							 PlannerInfo *root,
							 Index rtindex,
							 Relation rel,
							 Bitmapset *attrs_used,
							 List **retrieved_attrs)
{
	// 获取关系的元组描述符
	TupleDesc tupdesc = RelationGetDescr(rel);
	bool have_wholerow; // 是否有整行引用
	bool first;			// 是否是第一个列
	int i;
	bool need_field_key; // 是否需要添加字段键

	/* 检查是否有整行引用(如SELECT *) */
	have_wholerow = bms_is_member(0 - FirstLowInvalidHeapAttributeNumber,
								  attrs_used);

	first = true;
	need_field_key = true;

	*retrieved_attrs = NIL; // 初始化返回的属性列表

	/* 遍历所有属性列 */
	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		/* 跳过已删除的属性 */
		if (attr->attisdropped)
			continue;

		/* 检查列是否被使用(整行引用或位图集中标记) */
		if (have_wholerow ||
			bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
						  attrs_used))
		{
			RangeTblEntry *rte = planner_rt_fetch(rtindex, root);
			char *name = tdengine_get_column_name(rte->relid, i);

			/* 跳过时间列 */
			if (!TDENGINE_IS_TIME_COLUMN(name))
			{
				// 如果列不是标签键，则不需要额外添加字段键
				if (!tdengine_is_tag_key(name, rte->relid))
					need_field_key = false;

				// 如果不是第一个列，添加逗号分隔符
				if (!first)
					appendStringInfoString(buf, ", ");
				first = false;

				// 反解析列引用并添加到缓冲区
				tdengine_deparse_column_ref(buf, rtindex, i, -1, root, false, false);
			}

			// 将属性编号添加到返回列表
			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
		}
	}

	/* 如果没有找到任何列，使用'*'代替NULL */
	if (first)
	{
		appendStringInfoString(buf, "*");
		return;
	}

	/* 如果目标列表全是标签键，需要额外添加一个字段键 */
	if (need_field_key)
	{
		tdengine_append_field_key(tupdesc, buf, rtindex, root, first);
	}
}

/*
 * tdengine_deparse_column_ref - 反解析列引用并输出到缓冲区
 *
 * 参数:
 *   @buf: 输出字符串缓冲区
 *   @varno: 范围表索引(必须不是特殊变量号)
 *   @varattno: 属性编号
 *   @vartype: 变量数据类型OID
 *   @root: 规划器信息
 *   @convert: 是否需要进行类型转换
 *   @can_delete_directly: 输出参数，指示DELETE语句是否能直接下推
 *
 * 功能说明:
 *   1. 根据列引用信息获取列名
 *   2. 处理DELETE语句下推限制条件
 *   3. 对布尔类型进行特殊转换处理
 *   4. 对时间列进行特殊处理
 *   5. 其他列添加引号后输出
 */
static void
tdengine_deparse_column_ref(StringInfo buf, int varno, int varattno, Oid vartype,
							PlannerInfo *root, bool convert, bool *can_delete_directly)
{
	RangeTblEntry *rte;
	char *colname = NULL;

	/* varno必须不是OUTER_VAR、INNER_VAR或INDEX_VAR等特殊变量号 */
	Assert(!IS_SPECIAL_VARNO(varno));

	/* 从规划器信息中获取范围表条目 */
	rte = planner_rt_fetch(varno, root);

	/* 获取列名 */
	colname = tdengine_get_column_name(rte->relid, varattno);

	/*
	 * 检查DELETE语句是否能直接下推:
	 * 如果WHERE子句包含非时间列且非标签键的字段，则不能直接下推
	 */
	if (can_delete_directly)
		if (!TDENGINE_IS_TIME_COLUMN(colname) && !tdengine_is_tag_key(colname, rte->relid))
			*can_delete_directly = false;

	/* 处理布尔类型转换 */
	if (convert && vartype == BOOLOID)
	{
		appendStringInfo(buf, "(%s=true)", tdengine_quote_identifier(colname, QUOTE));
	}
	else
	{
		/* 特殊处理时间列 */
		if (TDENGINE_IS_TIME_COLUMN(colname))
			appendStringInfoString(buf, "time");
		else
			/* 普通列添加引号 */
			appendStringInfoString(buf, tdengine_quote_identifier(colname, QUOTE));
	}
}

/*
 * 添加反斜杠转义特殊字符到字符串缓冲区
 *
 * 功能: 检查字符是否为正则表达式特殊字符，如果是则添加反斜杠转义
 *
 * 参数:
 *   @buf: 输出字符串缓冲区，用于构建转义后的字符串
 *   @ptr: 指向当前要处理的字符的指针
 *   @regex_special: 正则表达式特殊字符集合字符串
 *
 * 处理逻辑:
 *   1. 检查当前字符是否在特殊字符集合中
 *   2. 如果是特殊字符，先添加反斜杠再添加字符本身
 *   3. 如果不是特殊字符，直接添加字符
 *
 * 正则表达式特殊字符包括: \^$.|?*+()[{%
 */
static void
add_backslash(StringInfo buf, const char *ptr, const char *regex_special)
{
	char ch = *ptr; // 获取当前字符

	/* 检查是否是正则表达式特殊字符 */
	if (strchr(regex_special, ch) != NULL)
	{
		/* 添加反斜杠转义 */
		appendStringInfoChar(buf, '\\'); // 先添加反斜杠
		appendStringInfoChar(buf, ch);	 // 再添加字符本身
	}
	else
	{
		/* 非特殊字符直接添加 */
		appendStringInfoChar(buf, ch);
	}
}
/*
 * 检查字符串中最后一个百分号(%)是否被转义
 *
 * 功能:
 *   判断字符串末尾的百分号是否被反斜杠转义，用于LIKE模式转换为正则表达式时的边界处理
 *
 * 参数:
 *   @val: 输入字符串，可能包含百分号和反斜杠转义字符
 *
 * 返回值:
 *   true:  1. 字符串中没有百分号
 *          2. 字符串末尾的百分号被转义(前面有奇数个反斜杠)
 *   false: 字符串末尾的百分号未被转义(前面有偶数个反斜杠或没有反斜杠)
 *
 * 算法说明:
 *   1. 从字符串末尾向前扫描
 *   2. 统计连续的反斜杠数量
 *   3. 根据反斜杠数量的奇偶性判断是否转义
 *      - 奇数个反斜杠: 转义有效
 *      - 偶数个反斜杠: 转义无效
 */
static bool
tdengine_last_percent_sign_check(const char *val)
{
	int len;				 // 字符串长度索引
	int count_backslash = 0; // 连续反斜杠计数器

	// 处理空指针情况
	if (val == NULL)
		return false;

	// 定位到字符串最后一个字符
	len = strlen(val) - 1;

	// 如果最后一个字符不是百分号，直接返回true
	if (val[len] != '%')
		return true;

	// 从倒数第二个字符开始向前扫描反斜杠
	len--;
	while (len >= 0 && val[len] == '\\')
	{
		count_backslash++; // 统计连续反斜杠数量
		len--;
	}

	// 根据反斜杠数量的奇偶性判断是否转义
	if (count_backslash % 2 == 0)
		return false; // 偶数个反斜杠，百分号未被转义

	return true; // 奇数个反斜杠，百分号被转义
}
/*
 * 将PostgreSQL的LIKE模式转换为TDengine兼容的正则表达式模式
 *
 * 功能:
 *   1. 将PostgreSQL的LIKE模式(包含%和_通配符)转换为TDengine兼容的正则表达式
 *   2. 处理大小写敏感/不敏感匹配
 *   3. 正确处理转义字符和正则表达式特殊字符
 *
 * 参数:
 *   @buf: 输出缓冲区，用于存储生成的正则表达式
 *   @val: 输入的LIKE模式字符串
 *   @op_type: 操作符类型(区分LIKE/ILIKE等)
 *
 * 转换规则:
 *   - % 转换为 (.*) 匹配任意数量字符
 *   - _ 转换为 (.{1}) 匹配单个字符
 *   - 非通配符字符按原样输出，特殊字符需转义
 *   - ILIKE操作符添加(?i)前缀实现不区分大小写
 *   - 字符串开头无%时添加^锚定开头
 *   - 字符串结尾无%或转义%时添加$锚定结尾
 *
 * 正则表达式特殊字符处理:
 *   需要转义的字符: \^$.|?*+()[{%
 */
static void
tdengine_deparse_string_like_pattern(StringInfo buf, const char *val, PatternMatchingOperator op_type)
{
	// 定义需要转义的正则表达式特殊字符
	const char *regex_special = "\\^$.|?*+()[{%";
	const char *ptr = val;

	// 添加正则表达式开始分隔符'/'
	appendStringInfoChar(buf, '/');

	// 处理大小写不敏感操作符(ILIKE)
	if (op_type == ILIKE_OPERATOR || op_type == NOT_ILIKE_OPERATOR)
		appendStringInfoString(buf, "(?i)");

	// 如果字符串不以%开头，添加^锚定开头
	if (val[0] != '%')
		appendStringInfoChar(buf, '^');

	// 遍历输入字符串的每个字符
	while (*ptr != '\0')
	{
		switch (*ptr)
		{
		case '%':
			// 将%转换为正则表达式的(.*)
			appendStringInfoString(buf, "(.*)");
			break;
		case '_':
			// 将_转换为正则表达式的(.{1})
			appendStringInfoString(buf, "(.{1})");
			break;
		case '\\':
			// 处理转义字符
			ptr++; // 跳过反斜杠

			// 检查转义字符后是否有有效字符
			if (*ptr == '\0')
			{
				elog(ERROR, "invalid pattern matching");
			}
			else
			{
				// 对转义后的字符进行特殊字符检查
				add_backslash(buf, ptr, regex_special);
			}
			break;
		default:
			// 处理普通字符，检查是否需要转义
			add_backslash(buf, ptr, regex_special);
			break;
		}

		ptr++; // 移动到下一个字符
	}

	// 检查字符串结尾是否需要添加$锚定
	if (tdengine_last_percent_sign_check(val))
		appendStringInfoChar(buf, '$');

	// 添加正则表达式结束分隔符'/'
	appendStringInfoChar(buf, '/');

	return;
}
/*
 * 将PostgreSQL的正则表达式模式转换为TDengine兼容的格式
 *
 * 功能:
 *   1. 将PostgreSQL的正则表达式转换为TDengine兼容的格式
 *   2. 处理大小写敏感/不敏感匹配
 *   3. 保持正则表达式内容不变，仅添加格式修饰符
 *
 * 参数:
 *   @buf: 输出缓冲区，用于存储转换后的正则表达式
 *   @val: 输入的正则表达式字符串
 *   @op_type: 操作符类型(区分大小写敏感/不敏感)
 *
 * 转换规则:
 *   - 在正则表达式前后添加'/'分隔符
 *   - 对于大小写不敏感操作符，添加(?i)前缀
 *   - 正则表达式内容保持原样输出
 *
 * 示例:
 *   - 输入("abc", REGEX_MATCH_CASE_SENSITIVE_OPERATOR) -> /abc/
 *   - 输入("abc", REGEX_MATCH_CASE_INSENSITIVE_OPERATOR) -> /(?i)abc/
 */
static void
tdengine_deparse_string_regex_pattern(StringInfo buf, const char *val, PatternMatchingOperator op_type)
{
	// 添加正则表达式开始分隔符'/'
	appendStringInfoChar(buf, '/');

	// 处理大小写不敏感操作符
	if (op_type == REGEX_MATCH_CASE_INSENSITIVE_OPERATOR ||
		op_type == REGEX_NOT_MATCH_CASE_INSENSITIVE_OPERATOR)
		appendStringInfoString(buf, "(?i)");

	// 添加原始正则表达式内容
	appendStringInfoString(buf, val);

	// 添加正则表达式结束分隔符'/'
	appendStringInfoChar(buf, '/');
	return;
}

/*
 * 反解析填充选项值到字符串缓冲区
 *
 * 功能: 将填充选项值直接作为字符串字面量添加到输出缓冲区
 * 用途: 用于处理TDengine查询中的fill()函数参数
 *
 * 参数:
 *   @buf: 输出字符串缓冲区，用于构建SQL语句
 *   @val: 填充选项值字符串，如"linear"、"none"或数值
 *
 * 示例:
 *   tdengine_deparse_fill_option(buf, "linear") -> 输出缓冲区添加"linear"
 *   tdengine_deparse_fill_option(buf, "100") -> 输出缓冲区添加"100"
 */
static void
tdengine_deparse_fill_option(StringInfo buf, const char *val)
{
	// 直接将填充选项值格式化为字符串添加到缓冲区
	appendStringInfo(buf, "%s", val);
}
/*
 * 将字符串转换为SQL字面量格式并追加到缓冲区
 *
 * 功能:
 *   1. 将普通字符串转换为SQL标准的单引号包围的字面量
 *   2. 处理字符串中的特殊字符，确保SQL语法正确
 *   3. 支持PostgreSQL的字符串转义规则
 *
 * 参数:
 *   @buf: 输出缓冲区，用于存储转换后的SQL字符串
 *   @val: 输入的原始字符串
 *
 * 处理规则:
 *   - 在字符串前后添加单引号
 *   - 对需要转义的字符进行双重转义(如单引号转义为两个单引号)
 *   - 保持其他字符不变
 *
 * 示例:
 *   - 输入"hello" -> 'hello'
 *   - 输入"O'Reilly" -> 'O''Reilly'
 */
void tdengine_deparse_string_literal(StringInfo buf, const char *val)
{
	const char *valptr;

	// 添加起始单引号
	appendStringInfoChar(buf, '\'');

	// 遍历字符串每个字符
	for (valptr = val; *valptr; valptr++)
	{
		char ch = *valptr;

		// 对需要转义的字符进行双重转义
		if (SQL_STR_DOUBLE(ch, true))
			appendStringInfoChar(buf, ch); // 添加转义字符

		// 添加字符本身
		appendStringInfoChar(buf, ch);
	}

	// 添加结束单引号
	appendStringInfoChar(buf, '\'');
}

/*
 * 反解析表达式主函数
 * 功能: 将PostgreSQL表达式树转换为TDengine兼容的SQL字符串
 *
 * 参数:
 *   @node: 要反解析的表达式节点
 *   @context: 反解析上下文，包含输出缓冲区和状态信息
 *
 * 处理流程:
 *   1. 保存当前上下文的状态(类型转换和时间戳转换标志)
 *   2. 根据表达式节点类型分发到对应的反解析函数
 *   3. 恢复上下文状态
 *
 * 支持的表达式类型:
 *   - 变量(Var)
 *   - 常量(Const)
 *   - 参数(Param)
 *   - 函数调用(FuncExpr)
 *   - 操作符表达式(OpExpr)
 *   - 标量数组操作(ScalarArrayOpExpr)
 *   - 类型重标记(RelabelType)
 *   - 布尔表达式(BoolExpr)
 *   - NULL测试(NullTest)
 *   - 数组构造(ArrayExpr)
 *   - 聚合函数(Aggref)
 *   - IO转换(CoerceViaIO)
 *
 * 注意事项:
 *   - 使用简单的硬编码括号方案
 *   - 比Var/Const/函数调用/类型转换更复杂的表达式需要自行处理括号
 */
static void
tdengine_deparse_expr(Expr *node, deparse_expr_cxt *context)
{
	// 保存外部上下文的状态标志
	bool outer_can_skip_cast = context->can_skip_cast;
	bool outer_convert_to_timestamp = context->convert_to_timestamp;

	// 空节点直接返回
	if (node == NULL)
		return;

	// 重置上下文标志
	context->can_skip_cast = false;
	context->convert_to_timestamp = false;

	// 根据节点类型分发处理
	switch (nodeTag(node))
	{
	case T_Var:
		// 恢复时间戳转换标志并处理变量节点
		context->convert_to_timestamp = outer_convert_to_timestamp;
		tdengine_deparse_var((Var *)node, context);
		break;
	case T_Const:
		// 恢复时间戳转换标志并处理常量节点
		context->convert_to_timestamp = outer_convert_to_timestamp;
		tdengine_deparse_const((Const *)node, context, 0);
		break;
	case T_Param:
		// 处理参数节点
		tdengine_deparse_param((Param *)node, context);
		break;
	case T_FuncExpr:
		// 恢复类型转换标志并处理函数调用
		context->can_skip_cast = outer_can_skip_cast;
		tdengine_deparse_func_expr((FuncExpr *)node, context);
		break;
	case T_OpExpr:
		// 恢复时间戳转换标志并处理操作符表达式
		context->convert_to_timestamp = outer_convert_to_timestamp;
		tdengine_deparse_op_expr((OpExpr *)node, context);
		break;
	case T_ScalarArrayOpExpr:
		// 处理标量数组操作
		tdengine_deparse_scalar_array_op_expr((ScalarArrayOpExpr *)node, context);
		break;
	case T_RelabelType:
		// 处理类型重标记(二进制兼容的类型转换)
		tdengine_deparse_relabel_type((RelabelType *)node, context);
		break;
	case T_BoolExpr:
		// 处理布尔表达式(AND/OR/NOT)
		tdengine_deparse_bool_expr((BoolExpr *)node, context);
		break;
	case T_NullTest:
		// 处理NULL测试(IS NULL/IS NOT NULL)
		tdengine_deparse_null_test((NullTest *)node, context);
		break;
	case T_ArrayExpr:
		// 处理数组构造表达式(ARRAY[...])
		tdengine_deparse_array_expr((ArrayExpr *)node, context);
		break;
	case T_Aggref:
		// 处理聚合函数调用
		tdengine_deparse_aggref((Aggref *)node, context);
		break;
	case T_CoerceViaIO:
		// 处理通过输入/输出函数进行的类型转换
		tdengine_deparse_coerce_via_io((CoerceViaIO *)node, context);
		break;
	default:
		// 不支持的表达式类型报错
		elog(ERROR, "unsupported expression type for deparse: %d",
			 (int)nodeTag(node));
		break;
	}
}

/*
 * 反解析Var节点到context->buf缓冲区
 *
 * 功能:
 *   1. 如果Var属于外部表关系，直接输出其远程列名
 *   2. 否则视为参数处理(运行时实际会成为Param)
 *
 * 参数:
 *   @node: 要反解析的Var节点
 *   @context: 反解析上下文，包含输出缓冲区和相关状态
 */
static void
tdengine_deparse_var(Var *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;			  // 输出缓冲区
	Relids relids = context->scanrel->relids; // 扫描关系ID集合

	/* 当涉及多个关系时限定列名(当前注释掉的代码) */
	/* bool qualify_col = (bms_num_members(relids) > 1); */

	// 检查Var是否属于当前扫描的关系且不是上层引用(varlevelsup=0)
	if (bms_is_member(node->varno, relids) && node->varlevelsup == 0)
	{
		// 布尔类型Var只在布尔比较时需要特殊转换
		bool convert = context->has_bool_cmp;

		/* Var属于外部表 - 反解析列引用 */
		tdengine_deparse_column_ref(buf, node->varno, node->varattno,
									node->vartype, context->root,
									convert, &context->can_delete_directly);
	}
	else
	{
		/* 作为参数处理 */
		if (context->params_list) // 如果有参数列表
		{
			int pindex = 0;
			ListCell *lc;

			/* 在参数列表中查找当前Var的索引 */
			foreach (lc, *context->params_list)
			{
				pindex++;
				if (equal(node, (Node *)lfirst(lc)))
					break;
			}
			if (lc == NULL) // 如果不在列表中
			{
				/* 添加到参数列表 */
				pindex++;
				*context->params_list = lappend(*context->params_list, node);
			}
			// 打印远程参数占位符(如$1)
			tdengine_print_remote_param(pindex, node->vartype, node->vartypmod, context);
		}
		else
		{
			// 无参数列表时打印占位符(用于EXPLAIN等场景)
			tdengine_print_remote_placeholder(node->vartype, node->vartypmod, context);
		}
	}
}

/*
 * 反解析常量值到输出缓冲区
 *
 * 功能:
 *   1. 将PostgreSQL常量值转换为TDengine兼容的SQL表示形式
 *   2. 处理各种数据类型(数值、布尔、时间戳等)的特殊格式要求
 *   3. 保持与PostgreSQL的ruleutils.c中get_const_expr函数同步
 *
 * 参数:
 *   @node: 要反解析的Const节点
 *   @context: 反解析上下文，包含输出缓冲区和相关状态
 *   @showtype: 控制是否显示类型修饰符
 *     -1: 从不显示"::typename"类型修饰
 *     0: 仅在需要时显示类型修饰
 *     1: 总是显示类型修饰
 *
 * 处理规则:
 *   - NULL值直接输出为"NULL"
 *   - 数值类型特殊处理NaN等特殊值
 *   - 布尔值转换为true/false
 *   - 时间戳类型根据上下文可能需要时区转换
 *   - 其他类型通过标准输出函数转换
 */
static void
tdengine_deparse_const(Const *node, deparse_expr_cxt *context, int showtype)
{
	StringInfo buf = context->buf; // 输出缓冲区
	Oid typoutput;				   // 类型输出函数OID
	bool typIsVarlena;			   // 是否为变长类型
	char *extval;				   // 转换后的字符串值
	char *type_name;			   // 类型名称

	// 处理NULL值
	if (node->constisnull)
	{
		appendStringInfoString(buf, "NULL");
		return;
	}

	// 获取类型的输出函数信息
	getTypeOutputInfo(node->consttype,
					  &typoutput, &typIsVarlena);

	// 根据不同类型进行特殊处理
	switch (node->consttype)
	{
	case INT2OID:
	case INT4OID:
	case INT8OID:
	case OIDOID:
	case FLOAT4OID:
	case FLOAT8OID:
	case NUMERICOID:
	{
		// 调用类型输出函数转换数值
		extval = OidOutputFunctionCall(typoutput, node->constvalue);

		/*
		 * 数值类型特殊处理:
		 * 1. 普通数值直接输出
		 * 2. 带符号数值加括号
		 * 3. NaN等特殊值加引号
		 */
		if (strspn(extval, "0123456789+-eE.") == strlen(extval))
		{
			if (extval[0] == '+' || extval[0] == '-')
				appendStringInfo(buf, "(%s)", extval);
			else
				appendStringInfoString(buf, extval);
		}
		else
			appendStringInfo(buf, "'%s'", extval);
	}
	break;
	case BITOID:
	case VARBITOID:
		// 处理位串类型(BIT和VARBIT)
		extval = OidOutputFunctionCall(typoutput, node->constvalue);
		// 输出为B'1010'格式的位串字面量
		appendStringInfo(buf, "B'%s'", extval);
		break;
	case BOOLOID:
		// 处理布尔类型
		extval = OidOutputFunctionCall(typoutput, node->constvalue);
		// PostgreSQL用't'/'f'表示true/false，转换为TDengine的true/false关键字
		if (strcmp(extval, "t") == 0)
			appendStringInfoString(buf, "true");
		else
			appendStringInfoString(buf, "false");
		break;

	case BYTEAOID:
		/*
		 * 处理二进制数据类型(BYTEA)
		 * PostgreSQL的BYTEA输出格式为"\\x##"的十六进制字符串
		 * 例如'hi'::bytea会输出为"\x6869"
		 * 转换为TDengine的X'6869'格式
		 */
		extval = OidOutputFunctionCall(typoutput, node->constvalue);
		// 跳过前两个字符"\\x"，输出为X'6869'格式
		appendStringInfo(buf, "X\'%s\'", extval + 2);
		break;
	case TIMESTAMPTZOID:
	{
		Datum datum;

		/*
		 * 处理带时区的时间戳类型
		 * 对于时间键列，需要从TIMESTAMPTZ转换为TIMESTAMP
		 * 例如'2015-08-18 09:00:00+09' -> '2015-08-18 00:00:00'
		 */
		if (context->convert_to_timestamp)
		{
			// 转换为UTC时区
			datum = DirectFunctionCall2(timestamptz_zone, CStringGetTextDatum("UTC"), node->constvalue);
			// 获取TIMESTAMP类型的输出函数
			getTypeOutputInfo(TIMESTAMPOID, &typoutput, &typIsVarlena);
		}
		else
		{
			// 保持原样，不转换时区
			datum = node->constvalue;
			getTypeOutputInfo(TIMESTAMPTZOID, &typoutput, &typIsVarlena);
		}

		// 转换为字符串并添加单引号
		extval = OidOutputFunctionCall(typoutput, datum);
		appendStringInfo(buf, "'%s'", extval);
		break;
	}
	case INTERVALOID:
	{
		// 处理时间间隔类型
		Interval *interval = DatumGetIntervalP(node->constvalue);
		// 根据PostgreSQL版本使用不同的时间结构体
		// #if (PG_VERSION_NUM >= 150000)
		struct pg_itm tm;

		// 将Interval转换为时间结构体
		interval2itm(*interval, &tm);
		// #else
		//              struct pg_tm tm;
		//              fsec_t      fsec;
		//              interval2tm(*interval, &tm, &fsec);
		// #endif

		// 输出为"ddhhmmssuu"格式，例如"1d2h3m4s5u"
		// #if (PG_VERSION_NUM >= 150000)
		appendStringInfo(buf, "%dd%ldh%dm%ds%du", tm.tm_mday, tm.tm_hour,
						 tm.tm_min, tm.tm_sec, tm.tm_usec
						 // #else
						 //              appendStringInfo(buf, "%dd%dh%dm%ds%du", tm.tm_mday, tm.tm_hour,
						 //                               tm.tm_min, tm.tm_sec, fsec
						 // #endif
		);
		break;
	}
	default:
		// 处理其他未明确列出的数据类型
		extval = OidOutputFunctionCall(typoutput, node->constvalue);

		// 获取数据类型名称
		type_name = tdengine_get_data_type_name(node->consttype);

		// 特殊处理填充选项枚举类型
		// TODO:
		if (strcmp(type_name, "tdengine_fill_enum") == 0)
		{
			tdengine_deparse_fill_option(buf, extval);
		}
		else if (context->op_type != UNKNOWN_OPERATOR)
		{
			// 根据操作符类型进行特殊处理
			switch (context->op_type)
			{
			case LIKE_OPERATOR:
			case NOT_LIKE_OPERATOR:
			case ILIKE_OPERATOR:
			case NOT_ILIKE_OPERATOR:
				// 将LIKE模式转换为正则表达式
				tdengine_deparse_string_like_pattern(buf, extval, context->op_type);
				break;
			case REGEX_MATCH_CASE_SENSITIVE_OPERATOR:
			case REGEX_NOT_MATCH_CASE_SENSITIVE_OPERATOR:
			case REGEX_MATCH_CASE_INSENSITIVE_OPERATOR:
			case REGEX_NOT_MATCH_CASE_INSENSITIVE_OPERATOR:
				// 处理正则表达式匹配
				tdengine_deparse_string_regex_pattern(buf, extval, context->op_type);
				break;
			default:
				elog(ERROR, "OPERATOR is not supported");
				break;
			}
		}
		else
		{
			// 默认处理：转换为字符串字面量
			tdengine_deparse_string_literal(buf, extval);
		}
		break;
	}
}

/*
 * 反解析Param节点到输出缓冲区
 *
 * 功能:
 *   1. 处理查询参数(Param节点)的反解析
 *   2. 在生成实际查询时，将参数添加到参数列表并分配索引
 *   3. 在EXPLAIN等场景下，仅生成参数占位符
 *
 * 参数:
 *   @node: 要反解析的Param节点
 *   @context: 反解析上下文，包含输出缓冲区和相关状态
 *
 * 处理逻辑:
 *   - 如果有参数列表(context->params_list不为空):
 *     1. 查找参数是否已在列表中
 *     2. 如果不在则添加到列表末尾
 *     3. 使用参数在列表中的索引作为远程参数编号(如$1)
 *   - 如果没有参数列表(如EXPLAIN场景):
 *     1. 仅生成通用参数占位符
 */
static void
tdengine_deparse_param(Param *node, deparse_expr_cxt *context)
{
	// 检查是否存在参数列表(实际查询场景)
	if (context->params_list)
	{
		int pindex = 0;
		ListCell *lc;

		/* 在参数列表中查找当前Param节点的索引 */
		foreach (lc, *context->params_list)
		{
			pindex++;
			if (equal(node, (Node *)lfirst(lc)))
				break;
		}

		// 如果参数不在列表中
		if (lc == NULL)
		{
			/* 添加到列表末尾并递增索引 */
			pindex++;
			*context->params_list = lappend(*context->params_list, node);
		}

		// 打印远程参数占位符(如$1)
		tdengine_print_remote_param(pindex, node->paramtype, node->paramtypmod, context);
	}
	else
	{
		// 无参数列表时打印通用占位符
		tdengine_print_remote_placeholder(node->paramtype, node->paramtypmod, context);
	}
}

// TODO: 
/*
 * 将PostgreSQL函数名转换为TDengine对应的等效函数名
 *
 * 参数:
 *   @in: 输入的PostgreSQL函数名称字符串
 *
 * 返回值:
 *   返回对应的TDengine函数名称字符串指针
 *   如果没有匹配的转换，则返回原始输入字符串
 *
 * 功能说明:
 *   1. 处理PostgreSQL与TDengine函数命名差异
 *   2. 主要转换规则:
 *      - 去除"_all"后缀的函数
 *      - 特定函数名映射(如btrim->trim)
 *      - 保留不匹配的原始函数名
 *   3. 支持多种函数类别:
 *      - 聚合函数(count, sum等)
 *      - 数学函数(abs, sqrt等)
 *      - 时间序列分析函数(moving_average等)
 */
char *
tdengine_replace_function(char *in)
{
	if (strcmp(in, "btrim") == 0)
		return "trim";
	else if (strcmp(in, "tdengine_count") == 0 || strcmp(in, "tdengine_count_all") == 0)
		return "count";
	else if (strcmp(in, "tdengine_distinct") == 0)
		return "distinct";
	else if (strcmp(in, "integral_all") == 0)
		return "integral";
	else if (strcmp(in, "mean_all") == 0)
		return "mean";
	else if (strcmp(in, "median_all") == 0)
		return "median";
	else if (strcmp(in, "tdengine_mode") == 0 || strcmp(in, "tdengine_mode_all") == 0)
		return "mode";
	else if (strcmp(in, "spread_all") == 0)
		return "spread";
	else if (strcmp(in, "stddev_all") == 0)
		return "stddev";
	else if (strcmp(in, "tdengine_sum") == 0 || strcmp(in, "tdengine_sum_all") == 0)
		return "sum";
	else if (strcmp(in, "first_all") == 0)
		return "first";
	else if (strcmp(in, "last_all") == 0)
		return "last";
	else if (strcmp(in, "tdengine_max") == 0 || strcmp(in, "tdengine_max_all") == 0)
		return "max";
	else if (strcmp(in, "tdengine_min") == 0 || strcmp(in, "tdengine_min_all") == 0)
		return "min";
	else if (strcmp(in, "percentile_all") == 0)
		return "percentile";
	else if (strcmp(in, "sample_all") == 0)
		return "sample";
	else if (strcmp(in, "abs_all") == 0)
		return "abs";
	else if (strcmp(in, "acos_all") == 0)
		return "acos";
	else if (strcmp(in, "asin_all") == 0)
		return "asin";
	else if (strcmp(in, "atan_all") == 0)
		return "atan";
	else if (strcmp(in, "atan2_all") == 0)
		return "atan2";
	else if (strcmp(in, "ceil_all") == 0)
		return "ceil";
	else if (strcmp(in, "cos_all") == 0)
		return "cos";
	else if (strcmp(in, "cumulative_sum_all") == 0)
		return "cumulative_sum";
	else if (strcmp(in, "derivative_all") == 0)
		return "derivative";
	else if (strcmp(in, "difference_all") == 0)
		return "difference";
	else if (strcmp(in, "elapsed_all") == 0)
		return "elapsed";
	else if (strcmp(in, "exp_all") == 0)
		return "exp";
	else if (strcmp(in, "floor_all") == 0)
		return "floor";
	else if (strcmp(in, "ln_all") == 0)
		return "ln";
	else if (strcmp(in, "log_all") == 0)
		return "log";
	else if (strcmp(in, "log2_all") == 0)
		return "log2";
	else if (strcmp(in, "log10_all") == 0)
		return "log10";
	else if (strcmp(in, "moving_average_all") == 0)
		return "moving_average";
	else if (strcmp(in, "non_negative_derivative_all") == 0)
		return "non_negative_derivative";
	else if (strcmp(in, "non_negative_difference_all") == 0)
		return "non_negative_difference";
	else if (strcmp(in, "pow_all") == 0)
		return "pow";
	else if (strcmp(in, "round_all") == 0)
		return "round";
	else if (strcmp(in, "sin_all") == 0)
		return "sin";
	else if (strcmp(in, "sqrt_all") == 0)
		return "sqrt";
	else if (strcmp(in, "tan_all") == 0)
		return "tan";
	else if (strcmp(in, "chande_momentum_oscillator_all") == 0)
		return "chande_momentum_oscillator";
	else if (strcmp(in, "exponential_moving_average_all") == 0)
		return "exponential_moving_average";
	else if (strcmp(in, "double_exponential_moving_average_all") == 0)
		return "double_exponential_moving_average";
	else if (strcmp(in, "kaufmans_efficiency_ratio_all") == 0)
		return "kaufmans_efficiency_ratio";
	else if (strcmp(in, "kaufmans_adaptive_moving_average_all") == 0)
		return "kaufmans_adaptive_moving_average";
	else if (strcmp(in, "triple_exponential_moving_average_all") == 0)
		return "triple_exponential_moving_average";
	else if (strcmp(in, "triple_exponential_derivative_all") == 0)
		return "triple_exponential_derivative";
	else if (strcmp(in, "relative_strength_index_all") == 0)
		return "relative_strength_index";
	else
		return in;
}

/*
 * 反解析函数表达式(FuncExpr节点)
 *
 * 功能: 将PostgreSQL函数调用转换为TDengine兼容的SQL函数调用格式
 *
 * 参数:
 *   @node: 要反解析的函数表达式节点
 *   @context: 反解析上下文，包含输出缓冲区和相关状态
 */
static void
tdengine_deparse_func_expr(FuncExpr *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; // 输出缓冲区
	char *proname;				   // 函数名称
	bool first;					   // 是否是第一个参数
	ListCell *arg;				   // 参数列表迭代器
	bool arg_swap = false;		   // 是否需要交换参数顺序
	bool can_skip_cast = false;	   // 是否可以跳过类型转换
	bool is_star_func = false;	   // 是否是星号函数(需要添加*参数)
	List *args = node->args;	   // 函数参数列表

	/* 获取函数名称 */
	proname = get_func_name(node->funcid);

	/*
	 * 处理fill()函数:
	 * 1. fill()函数必须放在GROUP BY子句的末尾
	 * 2. 在此阶段保存fill表达式但不进行反解析
	 */
	if (strcmp(proname, "tdengine_fill_numeric") == 0 ||
		strcmp(proname, "tdengine_fill_option") == 0)
	{
		Assert(list_length(args) == 1); // 确保fill函数只有一个参数

		/* 在SELECT目标列表中不反解析此函数 */
		if (context->is_tlist)
			return;

		/*
		 * 处理特殊情况:
		 * 当fill()作为time()函数的参数时，已经反解析了", "，
		 * 这里需要回退缓冲区指针，移除这个", "
		 */
		buf->len = buf->len - 2;

		/* 保存fill()节点以便后续在GROUP BY子句中反解析 */
		context->tdengine_fill_expr = node;
		return;
	}

	/*
	 * 处理tdengine_time()函数转换:
	 * 将PostgreSQL风格的tdengine_time()转换为TDengine兼容的time()函数格式
	 * 示例转换:
	 * tdengine_time(time, interval '2h') → time(2h)
	 * tdengine_time(time, interval '2h', interval '1h') → time(2h, 1h)
	 * tdengine_time(time, interval '2h', tdengine_fill_numeric(100)) → time(2h) fill(100)
	 */
	if (strcmp(proname, "tdengine_time") == 0)
	{
		int idx = 0; // 参数索引

		// 验证参数数量(2-4个)
		Assert(list_length(args) == 2 ||
			   list_length(args) == 3 ||
			   list_length(args) == 4);

		// 在SELECT目标列表中不反解析此函数
		if (context->is_tlist)
			return;

		appendStringInfo(buf, "time("); // 输出函数名开始
		first = true;
		foreach (arg, args)
		{
			if (idx == 0)
			{
				/* 跳过第一个参数(时间列) */
				idx++;
				continue;
			}
			if (idx >= 2)
				appendStringInfoString(buf, ", "); // 参数分隔符

			// 反解析参数表达式
			tdengine_deparse_expr((Expr *)lfirst(arg), context);
			idx++;
		}
		appendStringInfoChar(buf, ')'); // 结束函数调用
		return;
	}

	/*
	 * 处理类型转换函数:
	 * 如果父函数可以处理不带转换的参数，则跳过显式转换
	 * 主要处理float8和numeric类型转换
	 */
	if (context->can_skip_cast == true &&
		(strcmp(proname, "float8") == 0 || strcmp(proname, "numeric") == 0))
	{
		arg = list_head(args);
		context->can_skip_cast = false;						 // 重置标志
		tdengine_deparse_expr((Expr *)lfirst(arg), context); // 直接反解析参数
		return;
	}

	/* 处理log函数参数顺序调整 */
	if (strcmp(proname, "log") == 0)
	{
		arg_swap = true; // 标记需要交换参数顺序
	}

	/*
	 * 检查内部函数是否可以跳过类型转换:
	 * 1. 如果是TDengine特有函数
	 * 2. 或者是TDengine支持的PostgreSQL内置函数
	 */
	if (tdengine_is_unique_func(node->funcid, proname) ||
		tdengine_is_supported_builtin_func(node->funcid, proname))
		can_skip_cast = true; // 标记可以跳过类型转换

	// 检查是否是星号函数(如count(*))
	is_star_func = tdengine_is_star_func(node->funcid, proname);

	/* 将PostgreSQL函数名转换为TDengine兼容的函数名 */
	proname = tdengine_replace_function(proname);

	/* 输出函数名和左括号 */
	appendStringInfo(buf, "%s(", proname);

	/* 如果需要交换参数顺序(如log函数)且参数数量为2 */
	if (arg_swap && list_length(args) == 2)
	{
		// 交换参数顺序: 将第二个参数放到第一个位置
		args = list_make2(lfirst(list_tail(args)), lfirst(list_head(args)));
	}

	/* 处理函数参数列表 */
	first = true; // 标记是否是第一个参数

	// 如果是星号函数，直接添加*作为参数
	if (is_star_func)
	{
		appendStringInfoChar(buf, '*');
		first = false; // 已经处理了一个参数
	}

	// 遍历所有参数
	foreach (arg, args)
	{
		Expr *exp = (Expr *)lfirst(arg); // 获取当前参数表达式

		// 如果不是第一个参数，添加逗号分隔符
		if (!first)
			appendStringInfoString(buf, ", ");

		// 处理常量参数的特殊情况
		if (IsA((Node *)exp, Const))
		{
			Const *arg = (Const *)exp;
			char *extval;

			// 如果是文本类型常量
			if (arg->consttype == TEXTOID)
			{
				bool is_regex = tdengine_is_regex_argument(arg, &extval);

				/* 如果是正则表达式参数，直接输出 */
				if (is_regex == true)
				{
					appendStringInfo(buf, "%s", extval);
					first = false;
					continue; // 跳过后续处理
				}
			}
		}
		// ... 后续代码 ...

		if (can_skip_cast)
			context->can_skip_cast = true;
		tdengine_deparse_expr((Expr *)exp, context);
		first = false;
	}
	appendStringInfoChar(buf, ')');
}

/*
 * 反解析操作符表达式(OpExpr节点)
 * 功能: 将PostgreSQL操作符表达式转换为TDengine兼容的SQL表达式
 *
 * 参数:
 *   @node: 要反解析的操作符表达式节点
 *   @context: 反解析上下文，包含输出缓冲区和相关状态
 *
 * 处理流程:
 *   1. 从系统目录获取操作符信息
 *   2. 验证操作符类型和参数数量
 *   3. 处理无模式变量(schemaless var)的特殊情况
 *   4. 处理时间键列的转换标记
 *   5. 反解析操作符表达式(带括号保证优先级)
 */
static void
tdengine_deparse_op_expr(OpExpr *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;	  // 输出缓冲区
	HeapTuple tuple;				  // 系统表元组
	Form_pg_operator form;			  // 操作符系统表结构
	char oprkind;					  // 操作符类型(l-一元/b-二元)
	TDengineFdwRelationInfo *fpinfo = // FDW关系信息
		(TDengineFdwRelationInfo *)(context->foreignrel->fdw_private);

	// 获取当前扫描关系的范围表条目
	RangeTblEntry *rte = planner_rt_fetch(context->scanrel->relid, context->root);

	/* 从系统目录获取操作符信息 */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator)GETSTRUCT(tuple);
	oprkind = form->oprkind; // 获取操作符类型(一元/二元)

	/* 验证操作符类型与参数数量是否匹配 */
	Assert((oprkind == 'l' && list_length(node->args) == 1) ||
		   (oprkind == 'b' && list_length(node->args) == 2));

	/* 检查是否为无模式变量 */
	if (tdengine_is_slvar_fetch((Node *)node, &(fpinfo->slinfo)))
	{
		// 反解析无模式变量并释放系统表元组
		tdengine_deparse_slvar((Node *)node, linitial_node(Var, node->args),
							   lsecond_node(Const, node->args), context);
		ReleaseSysCache(tuple);
		return;
	}

	/* 处理时间键列的特殊转换 */
	if (oprkind == 'b' &&
		tdengine_contain_time_key_column(rte->relid, node->args))
	{
		context->convert_to_timestamp = true; // 标记需要转换为时间戳
	}

	/* 始终为表达式添加括号以保证优先级 */
	appendStringInfoChar(buf, '(');

	/* 反解析左操作数(如果是二元操作符) */
	if (oprkind == 'b')
	{
		tdengine_deparse_expr(linitial(node->args), context);
		appendStringInfoChar(buf, ' '); // 操作数后添加空格
	}

	/* 反解析操作符名称. */
	tdengine_deparse_operator_name(buf, form, &context->op_type);

	/* 反解析左操作数. */
	appendStringInfoChar(buf, ' ');

	tdengine_deparse_expr(llast(node->args), context);

	/* 为下一个操作重置操作符类型 */
	context->op_type = UNKNOWN_OPERATOR;

	appendStringInfoChar(buf, ')');

	ReleaseSysCache(tuple);
}

// TODO: 
/*
 * 反解析操作符名称
 * 功能: 将PostgreSQL操作符转换为TDengine兼容的操作符表示形式
 *
 * 参数:
 *   @buf: 输出缓冲区
 *   @opform: 操作符系统表信息
 *   @op_type: 输出参数，记录操作符类型(用于模式匹配操作符)
 *
 * 处理逻辑:
 *   1. 非pg_catalog模式的操作符: 使用完整限定名(OPERATOR(schema.op))
 *   2. pg_catalog模式的操作符: 转换为TDengine兼容形式
 *     - 模式匹配操作符(~~, !~~等)转换为=~, !~等
 *     - 正则表达式操作符(~, !~等)保持原样或转换
 *     - 其他操作符直接输出
 */
static void
tdengine_deparse_operator_name(StringInfo buf, Form_pg_operator opform, PatternMatchingOperator *op_type)
{
	/* 操作符名称(不需要引号引用) */
	cur_opname = NameStr(opform->oprname);
	*op_type = UNKNOWN_OPERATOR; // 初始化操作符类型

	/* 非pg_catalog模式的操作符: 输出完整限定名 */
	if (opform->oprnamespace != PG_CATALOG_NAMESPACE)
	{
		const char *opnspname;

		// 获取模式名并输出OPERATOR(schema.op)格式
		opnspname = get_namespace_name(opform->oprnamespace);
		appendStringInfo(buf, "OPERATOR(%s.%s)",
						 tdengine_quote_identifier(opnspname, QUOTE), cur_opname);
	}
	else
	{
		/* pg_catalog模式操作符的特殊处理 */
		if (strcmp(cur_opname, "~~") == 0) // LIKE操作符
		{
			appendStringInfoString(buf, "=~");
			*op_type = LIKE_OPERATOR;
		}
		else if (strcmp(cur_opname, "!~~") == 0) // NOT LIKE操作符
		{
			appendStringInfoString(buf, "!~");
			*op_type = NOT_LIKE_OPERATOR;
		}
		else if (strcmp(cur_opname, "~~*") == 0) // ILIKE操作符
		{
			appendStringInfoString(buf, "=~");
			*op_type = ILIKE_OPERATOR;
		}
		else if (strcmp(cur_opname, "!~~*") == 0) // NOT ILIKE操作符
		{
			appendStringInfoString(buf, "!~");
			*op_type = NOT_ILIKE_OPERATOR;
		}
		else if (strcmp(cur_opname, "~") == 0) // 正则匹配(区分大小写)
		{
			appendStringInfoString(buf, "=~");
			*op_type = REGEX_MATCH_CASE_SENSITIVE_OPERATOR;
		}
		else if (strcmp(cur_opname, "!~") == 0) // 正则不匹配(区分大小写)
		{
			appendStringInfoString(buf, "!~");
			*op_type = REGEX_NOT_MATCH_CASE_SENSITIVE_OPERATOR;
		}
		else if (strcmp(cur_opname, "~*") == 0)
		{
			appendStringInfoString(buf, "=~");
			*op_type = REGEX_MATCH_CASE_INSENSITIVE_OPERATOR;
		}
		else if (strcmp(cur_opname, "!~*") == 0)
		{
			appendStringInfoString(buf, "!~");
			*op_type = REGEX_NOT_MATCH_CASE_INSENSITIVE_OPERATOR;
		}
		else
		{
			appendStringInfoString(buf, cur_opname);
		}
	}
}

/*
 * 反解析ScalarArrayOpExpr表达式(数组操作表达式)
 * 功能: 将PostgreSQL的数组操作表达式(如IN/NOT IN)转换为TDengine兼容的SQL表达式
 *       TDengine不支持IN/NOT IN语法，需要转换为OR/AND连接的多个条件
 *
 * 转换示例:
 *   expr IN (c1, c2, c3)    → expr == c1 OR expr == c2 OR expr == c3
 *   expr NOT IN (c1, c2, c3) → expr <> c1 AND expr <> c2 AND expr <> c3
 *
 * 参数:
 *   @node: 要反解析的ScalarArrayOpExpr节点
 *   @context: 反解析上下文，包含输出缓冲区和相关状态
 */
static void
tdengine_deparse_scalar_array_op_expr(ScalarArrayOpExpr *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; // 输出缓冲区
	HeapTuple tuple;			   // 系统表元组
	Expr *arg1;					   // 左操作数(通常是列引用)
	Expr *arg2;					   // 右操作数(数组常量或数组表达式)
	Form_pg_operator form;		   // 操作符系统表结构
	char *opname = NULL;		   // 操作符名称
	Oid typoutput;				   // 类型输出函数OID
	bool typIsVarlena;			   // 是否为可变长度类型

	/* 从系统目录获取操作符信息 */
	tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(node->opno));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for operator %u", node->opno);
	form = (Form_pg_operator)GETSTRUCT(tuple);

	/* 验证参数数量(必须为2个: 左操作数和右操作数) */
	Assert(list_length(node->args) == 2);

	/* 复制操作符名称并释放系统表元组 */
	opname = pstrdup(NameStr(form->oprname));
	ReleaseSysCache(tuple);

	/* 获取左右操作数 */
	arg1 = linitial(node->args); // 第一个参数(左操作数)
	arg2 = lsecond(node->args);	 // 第二个参数(右操作数)

	/* 根据右操作数类型进行不同处理 */
	switch (nodeTag((Node *)arg2))
	{
	case T_Const: // 右操作数是常量数组
	{
		char *extval;			 // 数组常量转换后的字符串
		Const *c;				 // 常量节点
		bool isstr;				 // 是否为字符串类型数组
		const char *valptr;		 // 数组字符串遍历指针
		int i = -1;				 // 字符位置索引
		bool deparseLeft = true; // 是否需要反解析左操作数
		bool inString = false;	 // 是否在字符串常量中
		bool isEscape = false;	 // 是否遇到转义字符

		c = (Const *)arg2;
		if (!c->constisnull) // 只处理非NULL常量
		{
			/* 获取类型的输出函数信息 */
			getTypeOutputInfo(c->consttype, &typoutput, &typIsVarlena);

			/* 调用输出函数将数组常量转换为字符串形式 */
			extval = OidOutputFunctionCall(typoutput, c->constvalue);

			/* 判断数组元素类型是否为字符串 */
			switch (c->consttype)
			{
			case BOOLARRAYOID:	 // 布尔数组
			case INT8ARRAYOID:	 // bigint数组
			case INT2ARRAYOID:	 // smallint数组
			case INT4ARRAYOID:	 // integer数组
			case OIDARRAYOID:	 // OID数组
			case FLOAT4ARRAYOID: // real数组
			case FLOAT8ARRAYOID: // double precision数组
				isstr = false;
				break;
			default: // 其他类型视为字符串数组
				isstr = true;
				break;
			}

			/* 遍历数组字符串，逐个处理数组元素 */
			for (valptr = extval; *valptr; valptr++)
			{
				char ch = *valptr; // 当前字符
				i++;

				/* 处理左操作数(在每个OR/AND条件前输出一次) */
				if (deparseLeft)
				{
					if (c->consttype == BOOLARRAYOID) // 布尔数组特殊处理
					{
						if (arg1 != NULL && IsA(arg1, Var)) // 列引用
						{
							Var *var = (Var *)arg1;
							/* 反解析列引用，不进行类型转换 */
							tdengine_deparse_column_ref(buf, var->varno,
														var->varattno, var->vartype,
														context->root, false, false);
						}
						else if (arg1 != NULL && IsA(arg1, CoerceViaIO)) // 类型转换
						{
							bool has_bool_cmp = context->has_bool_cmp;
							context->has_bool_cmp = false;
							tdengine_deparse_expr(arg1, context);
							context->has_bool_cmp = has_bool_cmp;
						}
					}
					else // 非布尔数组
					{
						tdengine_deparse_expr(arg1, context);
					}

					/* 添加操作符和空格 */
					appendStringInfo(buf, " %s ", opname);

					/* 字符串类型数组需要添加引号 */
					if (isstr)
						appendStringInfoChar(buf, '\'');

					deparseLeft = false; // 标记左操作数已处理
				}

				/* 跳过数组的大括号 */
				if ((ch == '{' && i == 0) || (ch == '}' && (i == (strlen(extval) - 1))))
					continue;

				/* 处理字符串常量中的双引号 */
				if (ch == '\"' && !isEscape)
				{
					inString = !inString; // 切换字符串状态
					continue;
				}

				/* 处理字符串中的单引号(需要转义) */
				if (ch == '\'')
					appendStringInfoChar(buf, '\'');

				/* 处理转义字符 */
				if (ch == '\\' && !isEscape)
				{
					isEscape = true; // 标记遇到转义字符
					continue;
				}
				isEscape = false; // 重置转义标记

				/* 遇到逗号且不在字符串中，表示数组元素分隔符 */
				if (ch == ',' && !inString)
				{
					/* 如果是字符串类型，添加右引号 */
					if (isstr)
						appendStringInfoChar(buf, '\'');

					/* 根据IN/NOT IN添加连接符(OR/AND) */
					if (node->useOr)
						appendStringInfo(buf, " OR ");
					else
						appendStringInfo(buf, " AND ");

					deparseLeft = true; // 下一个元素需要重新输出左操作数
					continue;
				}

				/* 布尔数组特殊处理(true/false) */
				if (c->consttype == BOOLARRAYOID)
				{
					if (ch == 't')
						appendStringInfo(buf, "true");
					else
						appendStringInfo(buf, "false");
					continue;
				}

				/* 输出当前字符 */
				appendStringInfoChar(buf, ch);
			}

			/* 如果是字符串类型，添加右引号 */
			if (isstr)
				appendStringInfoChar(buf, '\'');
		}
		break;
	}
	case T_ArrayExpr: // 右操作数是数组表达式(非常量)
	{
		bool first = true; // 是否是第一个元素
		ListCell *lc;	   // 数组元素列表迭代器

		/* 遍历数组表达式中的每个元素 */
		foreach (lc, ((ArrayExpr *)arg2)->elements)
		{
			if (!first) // 非第一个元素需要添加连接符
			{
				if (node->useOr)
					appendStringInfoString(buf, " OR ");
				else
					appendStringInfoString(buf, " AND ");
			}

			/* 输出左括号和左操作数 */
			appendStringInfoChar(buf, '(');
			tdengine_deparse_expr(arg1, context);

			/* 输出操作符 */
			appendStringInfo(buf, " %s ", opname);

			/* 输出数组元素 */
			tdengine_deparse_expr(lfirst(lc), context);
			appendStringInfoChar(buf, ')');

			first = false; // 标记已处理第一个元素
		}
		break;
	}
	default:
		/* 不支持的数组表达式类型 */
		elog(ERROR, "unsupported expression type for deparse: %d",
			 (int)nodeTag(node));
		break;
	}
}

/*
 * 反解析RelabelType节点(二进制兼容的类型转换)
 * 功能: 处理PostgreSQL中的二进制兼容类型转换表达式
 *       RelabelType节点表示不改变底层二进制表示的类型转换
 *
 * 参数:
 *   @node: 要反解析的RelabelType节点
 *   @context: 反解析上下文，包含输出缓冲区和相关状态
 *
 * 处理逻辑:
 *   1. 直接递归反解析节点的参数表达式
 *   2. 不添加任何显式类型转换语法
 *
 * 注意事项:
 *   - 仅处理二进制兼容的类型转换
 *   - 不会生成CAST或::等类型转换语法
 *   - 与CoerceViaIO等需要实际转换的节点处理方式不同
 */
static void
tdengine_deparse_relabel_type(RelabelType *node, deparse_expr_cxt *context)
{
	/* 递归反解析参数表达式 */
	tdengine_deparse_expr(node->arg, context);
}

/*
 * 反解析布尔表达式(BoolExpr节点)
 * 功能: 将PostgreSQL布尔表达式(AND/OR/NOT)转换为TDengine兼容的SQL表达式
 *
 * 参数:
 *   @node: 要反解析的BoolExpr节点
 *   @context: 反解析上下文，包含输出缓冲区和相关状态
 *
 * 处理逻辑:
 *   1. 根据boolop类型(AND/OR/NOT)进行不同处理
 *   2. AND/OR表达式已被展平为N参数形式，需要处理多个参数
 *   3. NOT表达式处理单个参数
 *   4. 所有表达式都添加括号保证优先级
 *
 * 注意事项:
 *   - 输入的AND/OR表达式可能包含多个参数(已展平)
 *   - 输出格式符合TDengine语法要求
 */
static void
tdengine_deparse_bool_expr(BoolExpr *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; // 输出缓冲区
	const char *op = NULL;		   // 操作符字符串(AND/OR)
	bool first;					   // 是否是第一个参数
	ListCell *lc;				   // 参数列表迭代器

	/* 根据布尔操作类型处理 */
	switch (node->boolop)
	{
	case AND_EXPR: // AND表达式
		op = "AND";
		break;
	case OR_EXPR: // OR表达式
		op = "OR";
		break;
	case NOT_EXPR:											  // NOT表达式(特殊处理)
		appendStringInfoString(buf, "(NOT ");				  // 添加NOT前缀
		tdengine_deparse_expr(linitial(node->args), context); // 反解析参数
		appendStringInfoChar(buf, ')');						  // 添加右括号
		return;												  // NOT表达式处理完成直接返回
	}

	/* 处理AND/OR表达式 */
	appendStringInfoChar(buf, '('); // 添加左括号
	first = true;					// 标记第一个参数
	foreach (lc, node->args)		// 遍历所有参数
	{
		if (!first) // 非第一个参数需要添加操作符
			appendStringInfo(buf, " %s ", op);
		tdengine_deparse_expr((Expr *)lfirst(lc), context); // 反解析参数
		first = false;										// 标记已处理第一个参数
	}
	appendStringInfoChar(buf, ')'); // 添加右括号
}

/*
 * 反解析NULL测试表达式(NullTest节点)
 * 功能: 将PostgreSQL的IS NULL/IS NOT NULL表达式转换为TDengine兼容的SQL表达式
 *      TDengine不支持直接的IS NULL语法，转换为与空字符串比较的形式
 *
 * 转换规则:
 *   IS NULL → = ''
 *   IS NOT NULL → <> ''
 *
 * 参数:
 *   @node: 要反解析的NullTest节点
 *   @context: 反解析上下文，包含输出缓冲区和相关状态
 *
 * 处理流程:
 *   1. 添加左括号
 *   2. 反解析测试参数表达式
 *   3. 根据测试类型添加相应的比较运算符
 *   4. 添加右括号
 *
 * 注意事项:
 *   - 使用空字符串比较来模拟NULL测试
 *   - 所有表达式都添加括号保证优先级
 */
static void
tdengine_deparse_null_test(NullTest *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; // 获取输出缓冲区

	appendStringInfoChar(buf, '(');			   // 添加左括号
	tdengine_deparse_expr(node->arg, context); // 反解析测试参数

	// 根据测试类型添加比较运算符
	if (node->nulltesttype == IS_NULL)
		appendStringInfoString(buf, " = '')"); // IS NULL → = ''
	else
		appendStringInfoString(buf, " <> '')"); // IS NOT NULL → <> ''
}

/*
 * 反解析数组表达式(ArrayExpr节点)
 * 功能: 将PostgreSQL的ARRAY[...]数组构造语法转换为TDengine兼容的SQL表达式
 *
 * 参数:
 *   @node: 要反解析的ArrayExpr节点
 *   @context: 反解析上下文，包含输出缓冲区和相关状态
 *
 * 处理流程:
 *   1. 输出"ARRAY["前缀
 *   2. 遍历数组元素列表，逐个反解析元素表达式
 *   3. 在元素之间添加逗号分隔符
 *   4. 输出"]"后缀
 *
 * 注意事项:
 *   - 保持PostgreSQL的ARRAY[...]语法格式
 *   - 数组元素可以是任意有效的表达式
 *   - 元素表达式会递归调用tdengine_deparse_expr进行反解析
 */
static void
tdengine_deparse_array_expr(ArrayExpr *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; // 获取输出缓冲区
	bool first = true;			   // 标记是否是第一个元素
	ListCell *lc;				   // 数组元素列表迭代器

	// 输出数组构造语法前缀
	appendStringInfoString(buf, "ARRAY[");

	// 遍历数组元素列表
	foreach (lc, node->elements)
	{
		// 非第一个元素需要添加逗号分隔符
		if (!first)
			appendStringInfoString(buf, ", ");

		// 递归反解析当前数组元素
		tdengine_deparse_expr(lfirst(lc), context);
		first = false; // 标记已处理第一个元素
	}

	// 输出数组构造语法后缀
	appendStringInfoChar(buf, ']');
}

/*
 * tdengine_print_remote_param - 打印远程参数的表示形式
 *
 * 功能: 生成用于发送到远程服务器的参数占位符
 *
 * 参数:
 *   @paramindex: 参数索引号(从1开始)
 *   @paramtype: 参数类型OID(未实际使用)
 *   @paramtypmod: 参数类型修饰符(未实际使用)
 *   @context: 反解析上下文，包含输出缓冲区
 *
 * 输出格式:
 *   "$1", "$2" 等形式
 *
 * 注意事项:
 *   1. 不直接使用类型OID，避免远程和本地类型OID不一致的问题
 *   2. 参数类型检查由上层调用者保证
 *   3. 使用StringInfo缓冲区避免内存分配问题
 */
static void
tdengine_print_remote_param(int paramindex, Oid paramtype, int32 paramtypmod,
							deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; // 获取输出缓冲区

	// 生成参数占位符，格式为$加索引号
	appendStringInfo(buf, "$%d", paramindex);
}
/*
 * tdengine_print_remote_placeholder - 生成远程参数占位符(用于EXPLAIN场景)
 *
 * 功能: 为EXPLAIN命令生成参数占位符，避免远程服务器依赖实际参数值生成执行计划
 *
 * 参数:
 *   @paramtype: 参数类型OID(未实际使用)
 *   @paramtypmod: 参数类型修饰符(未实际使用)
 *   @context: 反解析上下文，包含输出缓冲区
 *
 * 输出格式:
 *   "(SELECT null)" - 表示这是一个占位参数
 *
 * 使用场景:
 *   1. 仅用于EXPLAIN命令生成查询计划时
 *   2. 实际执行时会替换为真实参数值
 *
 * 注意事项:
 *   - 不直接使用参数类型信息，确保计划生成不依赖具体参数值
 *   - 使用无害的SELECT null表达式作为占位符
 */
static void
tdengine_print_remote_placeholder(Oid paramtype, int32 paramtypmod,
								  deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; // 获取输出缓冲区

	// 生成无害的SQL表达式作为参数占位符
	appendStringInfo(buf, "(SELECT null)");
}

/*
 * 检查给定的OID是否属于PostgreSQL内置对象
 *
 * 参数:
 *   @oid: 要检查的对象ID
 *
 * 返回值:
 *   true - 是内置对象
 *   false - 不是内置对象
 *
 * 功能说明:
 *   1. 根据PostgreSQL版本使用不同的判断标准:
 *      - PG12+版本使用FirstGenbkiObjectId作为分界点
 *      - 旧版本使用FirstBootstrapObjectId作为分界点
 *   2. 所有小于分界点的OID被认为是内置对象
 *
 * 注意事项:
 *   - 内置对象的OID通常是手动分配的
 *   - 该判断方法在不同PostgreSQL版本间保持兼容
 */
bool tdengine_is_builtin(Oid oid)
{
#if (PG_VERSION_NUM >= 120000)
	return (oid < FirstGenbkiObjectId);
#else
	return (oid < FirstBootstrapObjectId);
#endif
}

/*
 * 检查常量节点是否为正则表达式参数(以'/'开头和结尾)
 * 参数:
 *   @node: 常量节点指针
 *   @extval: 输出参数，用于返回转换后的字符串值
 * 功能说明:
 *   1. 获取常量节点的类型输出函数
 *   2. 将常量值转换为字符串形式
 *   3. 检查字符串是否以'/'开头和结尾
 * 注意事项:
 *   - 调用者需负责释放extval返回的字符串内存
 *   - 仅支持文本类型的正则表达式判断
 */
bool tdengine_is_regex_argument(Const *node, char **extval)
{
	Oid typoutput;	   /* 类型输出函数OID */
	bool typIsVarlena; /* 是否为可变长度类型 */
	const char *first; /* 字符串起始指针 */
	const char *last;  /* 字符串结束指针 */

	/* 获取类型的输出函数信息 */
	getTypeOutputInfo(node->consttype,
					  &typoutput, &typIsVarlena);

	/* 调用输出函数将常量值转换为字符串 */
	(*extval) = OidOutputFunctionCall(typoutput, node->constvalue);
	first = *extval;
	last = *extval + strlen(*extval) - 1;

	/* 检查字符串是否以'/'开头和结尾 */
	if (*first == '/' && *last == '/')
		return true;
	else
		return false;
}

/*
 * 检查函数是否为星号函数(需要添加*作为第一个参数)
 * 参数:
 *   @funcid: 函数OID
 *   @in: 函数名称字符串
 * 判断规则:
 *   1. 排除内置函数
 *   2. 函数名必须以"_all"结尾
 *   3. 函数必须在TDengineStableStarFunction列表中
 */
bool tdengine_is_star_func(Oid funcid, char *in)
{
	char *eof = "_all"; /* 函数名后缀 */
	size_t func_len = strlen(in);
	size_t eof_len = strlen(eof);

	if (tdengine_is_builtin(funcid))
		return false;

	if (func_len > eof_len && strcmp(in + func_len - eof_len, eof) == 0 &&
		exist_in_function_list(in, TDengineStableStarFunction))
		return true;

	return false;
}
/*
 * 检查函数是否为唯一函数
 * 参数:
 *   @funcid: 函数OID
 *   @in: 函数名称字符串
 * 判断规则:
 *   1. 排除内置函数
 *   2. 函数必须在TDengineUniqueFunction列表中
 */
static bool
tdengine_is_unique_func(Oid funcid, char *in)
{
	if (tdengine_is_builtin(funcid))
		return false;

	if (exist_in_function_list(in, TDengineUniqueFunction))
		return true;

	return false;
}
/*
 * 检查函数是否为支持的TDengine内置函数
 * 参数:
 *   @funcid: 函数OID
 *   @in: 函数名称字符串
 * 判断规则:
 *   1. 必须是内置函数
 *   2. 函数必须在TDengineSupportedBuiltinFunction列表中
 */
static bool
tdengine_is_supported_builtin_func(Oid funcid, char *in)
{
	if (!tdengine_is_builtin(funcid))
		return false;

	if (exist_in_function_list(in, TDengineSupportedBuiltinFunction))
		return true;

	return false;
}

/*
 * 反解析聚合函数节点(Aggref)
 * 功能: 将PostgreSQL聚合函数转换为TDengine兼容的SQL语法
 *
 * 参数:
 *   @node: 要反解析的Aggref节点
 *   @context: 反解析上下文，包含输出缓冲区和相关状态
 *
 * 处理流程:
 *   1. 检查聚合函数是否支持(仅支持基本聚合)
 *   2. 处理特殊聚合函数(first/last)
 *   3. 处理星号函数(需要添加*参数)
 *   4. 处理普通聚合函数参数
 *   5. 处理DISTINCT和VARIADIC修饰符
 *
 * 注意事项:
 *   - 仅支持AGGSPLIT_SIMPLE类型的聚合
 *   - 特殊处理first/last函数，去掉时间参数
 *   - 星号函数需要添加*作为第一个参数
 */
static void
tdengine_deparse_aggref(Aggref *node, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; // 输出缓冲区
	bool use_variadic;			   // 是否使用VARIADIC修饰符
	char *func_name;			   // 函数名称
	bool is_star_func;			   // 是否是星号函数

	/* 仅支持基本聚合，不支持部分聚合 */
	Assert(node->aggsplit == AGGSPLIT_SIMPLE);

	/* 检查是否需要添加VARIADIC修饰符 */
	use_variadic = node->aggvariadic;

	/* 从系统目录获取函数名称 */
	func_name = get_func_name(node->aggfnoid);

	/* 特殊处理first/last函数 */
	if (!node->aggstar)
	{
		if ((strcmp(func_name, "last") == 0 || strcmp(func_name, "first") == 0) &&
			list_length(node->args) == 2)
		{
			/* 将first(time,value)/last(time,value)转换为first(value)/last(value) */
			Assert(list_length(node->args) == 2);
			appendStringInfo(buf, "%s(", func_name);
			// 只反解析第二个参数(value)
			tdengine_deparse_expr((Expr *)(((TargetEntry *)list_nth(node->args, 1))->expr), context);
			appendStringInfoChar(buf, ')');
			return;
		}
	}

	/* 检查是否是星号函数(需要添加*参数) */
	is_star_func = tdengine_is_star_func(node->aggfnoid, func_name);
	// 替换函数名为TDengine兼容的名称
	func_name = tdengine_replace_function(func_name);
	// 输出函数名
	appendStringInfo(buf, "%s", func_name);

	// 添加左括号
	appendStringInfoChar(buf, '(');

	/* 处理DISTINCT修饰符 */
	appendStringInfo(buf, "%s", (node->aggdistinct != NIL) ? "DISTINCT " : "");

	/* 处理星号参数(如count(*)) */
	if (node->aggstar)
		appendStringInfoChar(buf, '*');
	else
	{
		ListCell *arg;	   // 参数列表迭代器
		bool first = true; // 是否是第一个参数

		/* 如果是星号函数，添加*作为第一个参数 */
		if (is_star_func)
		{
			appendStringInfoChar(buf, '*');
			first = false;
		}

		/* 遍历所有参数 */
		foreach (arg, node->args)
		{
			TargetEntry *tle = (TargetEntry *)lfirst(arg);
			Node *n = (Node *)tle->expr;

			/* 处理正则表达式参数 */
			if (IsA(n, Const))
			{
				Const *arg = (Const *)n;
				char *extval;

				if (arg->consttype == TEXTOID)
				{
					bool is_regex = tdengine_is_regex_argument(arg, &extval);

					/* 如果是正则表达式，直接输出 */
					if (is_regex == true)
					{
						appendStringInfo(buf, "%s", extval);
						first = false;
						continue;
					}
				}
			}

			/* 跳过标记为resjunk的参数 */
			if (tle->resjunk)
				continue;

			/* 非第一个参数需要添加逗号分隔符 */
			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;

			/* 处理VARIADIC修饰符(最后一个参数) */
#if (PG_VERSION_NUM >= 130000)
			if (use_variadic && lnext(node->args, arg) == NULL)
#else
			if (use_variadic && lnext(arg) == NULL)
#endif
				appendStringInfoString(buf, "VARIADIC ");

			/* 反解析参数表达式 */
			tdengine_deparse_expr((Expr *)n, context);
		}
	}

	// 添加右括号
	appendStringInfoChar(buf, ')');
}

/*
 * 反解析GROUP BY子句
 * 功能: 将PostgreSQL的GROUP BY子句转换为TDengine兼容的SQL语法
 *
 * 参数:
 *   @tlist: 目标列表(包含所有可引用的表达式)
 *   @context: 反解析上下文，包含输出缓冲区和相关状态
 *
 * 处理流程:
 *   1. 检查查询是否有GROUP BY子句，没有则直接返回
 *   2. 添加"GROUP BY"关键字
 *   3. 验证查询不包含分组集(grouping sets)
 *   4. 初始化fill()函数指针为NULL
 *   5. 遍历GROUP BY子句中的每个分组项:
 *      a. 添加逗号分隔符(非第一个分组项)
 *      b. 调用tdengine_deparse_sort_group_clause反解析分组项
 *   6. 如果存在fill()函数，追加到GROUP BY子句末尾
 *
 * 注意事项:
 *   - 不处理分组集(grouping sets)，这类查询不会被下推
 *   - 使用原始GROUP BY子句而非处理后的版本，由远程规划器处理冗余项
 *   - fill()函数是TDengine特有的时间序列填充功能
 */
static void
tdengine_append_group_by_clause(List *tlist, deparse_expr_cxt *context)
{
	// 获取输出缓冲区和查询树
	StringInfo buf = context->buf;		 // 字符串输出缓冲区
	Query *query = context->root->parse; // 查询解析树
	ListCell *lc;						 // 列表迭代器
	bool first = true;					 // 标记是否是第一个分组项

	/* 检查查询是否有GROUP BY子句，没有则直接返回 */
	if (!query->groupClause)
		return;

	/* 添加GROUP BY关键字到输出缓冲区 */
	appendStringInfo(buf, " GROUP BY ");

	/*
	 * 验证查询不包含分组集(grouping sets)
	 * 这类查询不会被下推到TDengine执行
	 */
	Assert(!query->groupingSets);

	/* 初始化fill()函数指针为NULL */
	context->tdengine_fill_expr = NULL;

	/*
	 * 遍历原始GROUP BY子句(而非处理后的版本)
	 * 这样可以让远程规划器自行处理冗余项
	 */
	foreach (lc, query->groupClause)
	{
		SortGroupClause *grp = (SortGroupClause *)lfirst(lc); // 获取当前分组项

		/* 非第一个分组项需要添加逗号分隔符 */
		if (!first)
			appendStringInfoString(buf, ", ");
		first = false; // 标记已处理第一个分组项

		/* 反解析当前分组项 */
		tdengine_deparse_sort_group_clause(grp->tleSortGroupRef, tlist, context);
	}

	/* 如果存在fill()函数，追加到GROUP BY子句末尾 */
	if (context->tdengine_fill_expr)
	{
		ListCell *arg; // 参数列表迭代器

		/* 添加fill(前缀 */
		appendStringInfo(buf, " fill(");

		/* 遍历fill函数的所有参数并反解析 */
		foreach (arg, context->tdengine_fill_expr->args)
		{
			tdengine_deparse_expr((Expr *)lfirst(arg), context);
		}

		/* 添加右括号 */
		appendStringInfoChar(buf, ')');
	}
}
/*
 * 反解析LIMIT/OFFSET子句
 * 功能: 将PostgreSQL的LIMIT/OFFSET子句转换为TDengine兼容的SQL语法
 *
 * 参数:
 *   @context: 反解析上下文，包含输出缓冲区和相关状态
 *
 * 处理流程:
 *   1. 设置传输模式确保常量表达式可移植输出
 *   2. 检查并处理LIMIT子句(如果存在)
 *   3. 检查并处理OFFSET子句(如果存在)
 *   4. 恢复原始传输模式
 *
 * 注意事项:
 *   - 使用tdengine_set_transmission_modes确保常量可移植
 *   - LIMIT和OFFSET子句都是可选的
 *   - 子句顺序固定为LIMIT在前，OFFSET在后
 */
static void
tdengine_append_limit_clause(deparse_expr_cxt *context)
{
	PlannerInfo *root = context->root; // 查询规划信息
	StringInfo buf = context->buf;     // 输出缓冲区
	int nestlevel;                     // 传输模式嵌套级别

	/* 设置传输模式确保常量表达式可移植输出 */
	nestlevel = tdengine_set_transmission_modes();

	/* 处理LIMIT子句(如果存在) */
	if (root->parse->limitCount)
	{
		appendStringInfoString(buf, " LIMIT "); // 添加LIMIT关键字
		tdengine_deparse_expr((Expr *)root->parse->limitCount, context); // 反解析LIMIT值
	}

	/* 处理OFFSET子句(如果存在) */
	if (root->parse->limitOffset)
	{
		appendStringInfoString(buf, " OFFSET "); // 添加OFFSET关键字
		tdengine_deparse_expr((Expr *)root->parse->limitOffset, context); // 反解析OFFSET值
	}

	/* 恢复原始传输模式 */
	tdengine_reset_transmission_modes(nestlevel);
}
/*
 * 查找等价类中完全来自指定关系的成员表达式
 * 功能: 在等价类中查找所有变量都来自指定关系的成员表达式
 *
 * 参数:
 *   @ec: 等价类指针，包含多个等价成员
 *   @rel: 关系信息指针，指定要查找的关系
 *
 * 返回值:
 *   成功: 返回符合条件的等价成员表达式指针
 *   失败: 返回NULL
 *
 * 处理流程:
 *   1. 遍历等价类的所有成员
 *   2. 检查每个成员的变量是否完全来自指定关系
 *   3. 找到第一个符合条件的成员表达式并返回
 *
 * 注意事项:
 *   - 如果有多个符合条件的成员，任意返回其中一个即可
 *   - 使用bms_is_subset检查变量关系包含性
 *   - 主要用于ORDER BY子句的反解析
 */
static Expr *
tdengine_find_em_expr_for_rel(EquivalenceClass *ec, RelOptInfo *rel)
{
	ListCell *lc_em; // 等价成员列表迭代器

	// 遍历等价类的所有成员
	foreach (lc_em, ec->ec_members)
	{
		EquivalenceMember *em = lfirst(lc_em); // 获取当前等价成员

		/*
		 * 检查当前成员的所有变量是否都来自指定关系
		 * bms_is_subset确保em->em_relids是rel->relids的子集
		 */
		if (bms_is_subset(em->em_relids, rel->relids))
		{
			/*
			 * 如果找到多个符合条件的成员，任意返回其中一个即可
			 * 因为它们在等价类中是语义等价的
			 */
			return em->em_expr;
		}
	}

	/* 没有找到符合条件的等价类成员表达式 */
	return NULL;
}
/*
 * 反解析ORDER BY子句
 * 功能: 根据给定的pathkeys将排序子句转换为TDengine兼容的SQL语法
 *
 * 参数:
 *   @pathkeys: 路径键列表，包含排序表达式和排序方向
 *   @context: 反解析上下文，包含输出缓冲区和相关状态
 *
 * 处理流程:
 *   1. 设置传输模式确保常量表达式可移植输出
 *   2. 添加"ORDER BY"关键字
 *   3. 遍历每个pathkey:
 *      a. 查找等价类中完全来自基表的成员表达式
 *      b. 反解析排序表达式
 *      c. 根据排序策略添加ASC/DESC修饰符
 *      d. 检查并处理NULLS FIRST(不支持则报错)
 *   4. 恢复原始传输模式
 *
 * 注意事项:
 *   - 不支持NULLS FIRST语法，遇到会抛出错误
 *   - 使用tdengine_set_transmission_modes确保常量可移植
 *   - 排序表达式必须完全来自基表
 */
static void
tdengine_append_order_by_clause(List *pathkeys, deparse_expr_cxt *context)
{
	ListCell *lcell;						// 路径键列表迭代器
	int nestlevel;							// 传输模式嵌套级别
	char *delim = " ";						// 分隔符(初始为空格)
	RelOptInfo *baserel = context->scanrel; // 基表关系信息
	StringInfo buf = context->buf;			// 输出缓冲区

	/* 设置传输模式确保常量表达式可移植输出 */
	nestlevel = tdengine_set_transmission_modes();

	/* 添加ORDER BY关键字 */
	appendStringInfo(buf, " ORDER BY");

	/* 遍历每个pathkey */
	foreach (lcell, pathkeys)
	{
		PathKey *pathkey = lfirst(lcell); // 当前路径键
		Expr *em_expr;					  // 等价成员表达式

		/* 查找等价类中完全来自基表的成员表达式 */
		em_expr = tdengine_find_em_expr_for_rel(pathkey->pk_eclass, baserel);
		Assert(em_expr != NULL); // 必须找到有效表达式

		/* 添加分隔符并反解析排序表达式 */
		appendStringInfoString(buf, delim);
		tdengine_deparse_expr(em_expr, context);

		/* 根据排序策略添加ASC/DESC修饰符 */
		if (pathkey->pk_strategy == BTLessStrategyNumber)
			appendStringInfoString(buf, " ASC"); // 升序
		else
			appendStringInfoString(buf, " DESC"); // 降序

		/* 检查并处理NULLS FIRST(不支持则报错) */
		if (pathkey->pk_nulls_first)
			elog(ERROR, "NULLS FIRST not supported");

		/* 后续项使用逗号分隔 */
		delim = ", ";
	}

	/* 恢复原始传输模式 */
	tdengine_reset_transmission_modes(nestlevel);
}

/*
 * 反解析排序或分组子句
 * 功能: 将PostgreSQL的排序或分组子句转换为TDengine兼容的SQL语法
 *
 * 参数:
 *   @ref: 排序/分组引用号
 *   @tlist: 目标列表(包含所有可引用的表达式)
 *   @context: 反解析上下文，包含输出缓冲区和相关状态
 *
 * 返回值:
 *   返回反解析的表达式树节点，方便调用者直接使用
 *
 * 处理流程:
 *   1. 根据引用号从目标列表中查找对应的目标条目(TargetEntry)
 *   2. 获取目标条目的表达式:
 *      a. 如果是常量表达式(Const):
 *         - 强制添加类型转换，避免被误解为列位置
 *      b. 如果是空表达式或变量引用(Var):
 *         - 直接反解析表达式
 *      c. 其他复杂表达式:
 *         - 添加括号包围后再反解析
 *
 * 注意事项:
 *   - 类似于get_rule_sortgroupclause()函数，但专为TDengine适配
 *   - 常量表达式必须显式转换以避免歧义
 *   - 复杂表达式总是用括号包围以确保优先级
 */
static Node *
tdengine_deparse_sort_group_clause(Index ref, List *tlist, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; // 输出缓冲区
	TargetEntry *tle;			   // 目标条目
	Expr *expr;					   // 表达式节点

	// 根据引用号获取目标条目
	tle = get_sortgroupref_tle(ref, tlist);
	expr = tle->expr;

	// 处理常量表达式(必须添加类型转换)
	if (expr && IsA(expr, Const))
	{
		tdengine_deparse_const((Const *)expr, context, 1);
	}
	// 处理空表达式或简单变量引用
	else if (!expr || IsA(expr, Var))
	{
		tdengine_deparse_expr(expr, context);
	}
	// 处理其他复杂表达式(添加括号包围)
	else
	{
		appendStringInfoString(buf, "(");
		tdengine_deparse_expr(expr, context);
		appendStringInfoString(buf, ")");
	}

	return (Node *)expr; // 返回表达式树节点
}

/*
 * tdengine_get_data_type_name: 根据数据类型OID获取类型名称
 *
 * 参数:
 *   @data_type_id: 要查询的数据类型OID
 *
 * 返回值:
 *   成功: 返回类型名称字符串(需调用者释放内存)
 *   失败: 抛出错误
 *
 * 功能说明:
 *   1. 通过系统缓存查询数据类型信息
 *   2. 提取类型名称并返回其副本
 *   3. 调用者需负责释放返回的字符串内存
 *
 * 注意事项:
 *   - 使用pstrdup分配内存，需用pfree释放
 *   - 如果找不到对应类型会抛出ERROR
 */
char *tdengine_get_data_type_name(Oid data_type_id)
{
	HeapTuple tuple;   /* 系统缓存元组 */
	Form_pg_type type; /* 类型信息结构体 */
	char *type_name;   /* 返回的类型名称 */

	/* 从系统缓存查询类型信息 */
	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(data_type_id));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for data type id %u", data_type_id);

	/* 获取类型结构体并复制类型名称 */
	type = (Form_pg_type)GETSTRUCT(tuple);
	/* 总是返回类型名称的副本 */
	type_name = pstrdup(type->typname.data);

	/* 释放系统缓存元组 */
	ReleaseSysCache(tuple);
	return type_name;
}

/*
 * 检查表达式列表中是否包含时间列
 *
 * 参数:
 *   @exprs: 要检查的表达式列表
 *   @pslinfo: 无模式信息结构体指针
 *
 * 功能说明:
 *   1. 遍历表达式列表中的每个表达式
 *   2. 检查表达式类型:
 *      - 如果是Var类型(列引用)，检查其数据类型是否为时间类型
 *      - 如果是CoerceViaIO类型(类型转换)，检查其参数是否为时间键列且转换结果为时间类型
 *   3. 发现时间列立即返回true
 *
 * 注意事项:
 *   - 依赖于TDENGINE_IS_TIME_TYPE宏定义来判断时间类型
 *   - 对于CoerceViaIO类型，会调用tdengine_contain_time_key_column进行递归检查
 */
static bool
tdengine_contain_time_column(List *exprs, schemaless_info *pslinfo)
{
	ListCell *lc;

	foreach (lc, exprs)
	{
		Expr *expr = (Expr *)lfirst(lc);

		if (IsA(expr, Var))
		{
			Var *var = (Var *)expr;

			if (TDENGINE_IS_TIME_TYPE(var->vartype))
			{
				return true;
			}
		}
		else if (IsA(expr, CoerceViaIO))
		{
			CoerceViaIO *cio = (CoerceViaIO *)expr;
			Node *arg = (Node *)cio->arg;

			if (tdengine_contain_time_key_column(arg, pslinfo))
			{
				if (TDENGINE_IS_TIME_TYPE(cio->resulttype))
				{
					return true;
				}
			}
		}
	}
	return false;
}
/*
 * 检查表达式列表中是否包含时间键列
 *
 * 参数:
 *   @relid: 外表OID
 *   @exprs: 表达式列表
 *
 * 功能说明:
 *   1. 遍历表达式列表中的每个表达式
 *   2. 检查表达式是否为Var类型(列引用)
 *   3. 检查列是否为时间类型(TDENGINE_IS_TIME_TYPE)
 *   4. 获取列名并检查是否为时间键列(TDENGINE_IS_TIME_COLUMN)
 *
 * 注意事项:
 *   - 仅检查Var类型的表达式节点
 *   - 依赖于TDENGINE_IS_TIME_TYPE和TDENGINE_IS_TIME_COLUMN宏定义
 */
static bool
tdengine_contain_time_key_column(Oid relid, List *exprs)
{
	ListCell *lc;

	/* 遍历表达式列表 */
	foreach (lc, exprs)
	{
		Expr *expr = (Expr *)lfirst(lc);
		Var *var;

		/* 跳过非Var类型的表达式 */
		if (!IsA(expr, Var))
			continue;

		var = (Var *)expr;

		/* 检查变量类型是否为时间类型 */
		if (TDENGINE_IS_TIME_TYPE(var->vartype))
		{
			/* 获取列名并检查是否为时间关键列 */
			char *column_name = tdengine_get_column_name(relid, var->varattno);

			if (TDENGINE_IS_TIME_COLUMN(column_name))
				return true;
		}
	}

	return false;
}
/*
 * 检查表达式列表中是否包含时间表达式(排除Var/Const/Param/FuncExpr类型节点)
 *
 * 参数:
 *   @exprs: 要检查的表达式列表
 *
 * 功能说明:
 *   1. 遍历表达式列表中的每个表达式
 *   2. 跳过Var/Const/Param/FuncExpr类型的节点
 *   3. 检查剩余节点的返回类型是否为时间类型
 *   4. 发现时间类型表达式立即返回true
 *
 * 注意事项:
 *   - 仅检查非基本表达式节点(Var/Const/Param/FuncExpr除外)
 *   - 依赖于TDENGINE_IS_TIME_TYPE宏定义来判断时间类型
 */
static bool
tdengine_contain_time_expr(List *exprs)
{
	ListCell *lc;

	foreach (lc, exprs)
	{
		Expr *expr = (Expr *)lfirst(lc);
		Oid type;

		if (IsA(expr, Var) ||
			IsA(expr, Const) ||
			IsA(expr, Param) ||
			IsA(expr, FuncExpr))
		{
			continue;
		}

		type = exprType((Node *)expr);

		if (TDENGINE_IS_TIME_TYPE(type))
		{
			return true;
		}
	}
	return false;
}
/*
 * 检查表达式列表中是否包含时间函数
 *
 * 参数:
 *   @exprs: 要检查的表达式列表
 *
 * 功能说明:
 *   1. 遍历表达式列表中的每个表达式
 *   2. 检查表达式是否为函数表达式(FuncExpr类型)
 *   3. 检查函数返回类型是否为时间类型(TDENGINE_IS_TIME_TYPE)
 *   4. 发现时间函数立即返回true
 *
 * 注意事项:
 *   - 仅检查FuncExpr类型的表达式节点
 *   - 依赖于TDENGINE_IS_TIME_TYPE宏定义来判断时间类型
 */
static bool
tdengine_contain_time_function(List *exprs)
{
	ListCell *lc;

	foreach (lc, exprs)
	{
		Expr *expr = (Expr *)lfirst(lc);
		FuncExpr *func_expr;

		if (!IsA(expr, FuncExpr))
			continue;

		func_expr = (FuncExpr *)expr;

		if (TDENGINE_IS_TIME_TYPE(func_expr->funcresulttype))
		{
			return true;
		}
	}
	return false;
}
/*
 * 检查表达式列表中是否包含时间类型的参数节点(Param)
 *
 * 参数:
 *   @exprs: 要检查的表达式列表
 *
 * 功能说明:
 *   1. 遍历表达式列表中的每个表达式
 *   2. 检查表达式是否为参数节点(Param类型)
 *   3. 获取参数节点的数据类型
 *   4. 检查数据类型是否为时间类型(TDENGINE_IS_TIME_TYPE)
 *   5. 发现时间类型参数立即返回true
 *
 * 注意事项:
 *   - 仅检查Param类型的表达式节点
 *   - 依赖于TDENGINE_IS_TIME_TYPE宏定义来判断时间类型
 */
static bool
tdengine_contain_time_param(List *exprs)
{
	ListCell *lc;

	foreach (lc, exprs)
	{
		Expr *expr = (Expr *)lfirst(lc);
		Oid type;

		if (!IsA(expr, Param))
			continue;

		type = exprType((Node *)expr);

		if (TDENGINE_IS_TIME_TYPE(type))
		{
			return true;
		}
	}

	return false;
}
/*
 * 检查表达式列表中是否包含时间类型的常量节点(Const)
 *
 * 参数:
 *   @exprs: 要检查的表达式列表
 *
 * 功能说明:
 *   1. 遍历表达式列表中的每个表达式
 *   2. 检查表达式是否为常量节点(Const类型)
 *   3. 获取常量节点的数据类型
 *   4. 检查数据类型是否为时间类型(TDENGINE_IS_TIME_TYPE)
 *   5. 发现时间类型常量立即返回true
 *
 * 注意事项:
 *   - 仅检查Const类型的表达式节点
 *   - 依赖于TDENGINE_IS_TIME_TYPE宏定义来判断时间类型
 */
static bool
tdengine_contain_time_const(List *exprs)
{
	ListCell *lc;

	foreach (lc, exprs)
	{
		Expr *expr = (Expr *)lfirst(lc);
		Oid type;

		if (!IsA(expr, Const))
			continue;

		type = exprType((Node *)expr);

		if (TDENGINE_IS_TIME_TYPE(type))
		{
			return true;
		}
	}

	return false;
}

/*
 * tdengine_is_grouping_target: 检查给定的目标条目(TargetEntry)是否是GROUP BY子句的分组目标
 *
 * 参数:
 *   @tle: 要检查的目标条目(TargetEntry结构指针)
 *   @query: 查询树(Query结构指针)
 *
 * 返回值:
 *   true - 是GROUP BY的分组目标
 *   false - 不是GROUP BY的分组目标
 *
 * 处理逻辑:
 *   1. 首先检查查询是否有GROUP BY子句，没有则直接返回false
 *   2. 遍历GROUP BY子句中的所有分组项
 *   3. 比较每个分组项的tleSortGroupRef与目标条目的ressortgroupref
 *   4. 如果匹配则返回true，否则继续检查下一个分组项
 *   5. 遍历结束后仍未找到匹配则返回false
 */
bool tdengine_is_grouping_target(TargetEntry *tle, Query *query)
{
	ListCell *lc;

	/* 如果查询没有GROUP BY子句，直接返回false */
	if (!query->groupClause)
		return false;

	/* 遍历GROUP BY子句中的每个分组项 */
	foreach (lc, query->groupClause)
	{
		SortGroupClause *grp = (SortGroupClause *)lfirst(lc);

		/* 检查当前分组项是否与目标条目匹配 */
		if (grp->tleSortGroupRef == tle->ressortgroupref)
		{
			return true;
		}
	}

	/* 没有找到匹配的分组项 */
	return false;
}

/*
 * tdengine_append_field_key - 向缓冲区添加第一个找到的字段键(field key)
 *
 * 参数:
 *   @tupdesc: 表元组描述符
 *   @buf: 输出字符串缓冲区
 *   @rtindex: 范围表索引
 *   @root: 规划器信息
 *   @first: 标记是否是第一个输出项
 *
 * 功能说明:
 *   1. 遍历表的所有属性列
 *   2. 跳过已删除的属性和时间列/标签键列
 *   3. 找到第一个符合条件的字段键列后:
 *      a. 如果不是第一个输出项，添加逗号分隔符
 *      b. 反解析列引用并添加到缓冲区
 *      c. 立即返回(只处理第一个找到的字段键)
 *
 * 注意事项:
 *   - 该函数只处理第一个找到的字段键列
 *   - 依赖于TDENGINE_IS_TIME_COLUMN和tdengine_is_tag_key宏/函数
 */
void tdengine_append_field_key(TupleDesc tupdesc, StringInfo buf, Index rtindex, PlannerInfo *root, bool first)
{
	int i;

	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);
		RangeTblEntry *rte = planner_rt_fetch(rtindex, root);
		char *name = tdengine_get_column_name(rte->relid, i);

		/* 跳过已删除的属性 */
		if (attr->attisdropped)
			continue;

		/* 跳过时间列和标签键列 */
		if (!TDENGINE_IS_TIME_COLUMN(name) && !tdengine_is_tag_key(name, rte->relid))
		{
			/* 如果不是第一个输出项，添加逗号分隔符 */
			if (!first)
				appendStringInfoString(buf, ", ");
			/* 反解析列引用并添加到缓冲区 */
			tdengine_deparse_column_ref(buf, rtindex, i, -1, root, false, false);
			return;
		}
	}
}

/*
 * 获取外表对应的远程表名
 *
 * 参数:
 *   @rel: 本地Relation对象
 *
 * 功能说明:
 *   1. 首先从外表的FDW选项中查找"table"定义
 *   2. 如果未找到自定义表名，则使用Relation对象自身的名称
 *
 * 注意事项:
 *   - 返回的字符串指针由调用者管理，不应释放
 *   - 优先使用FDW选项中定义的表名
 */
char *
tdengine_get_table_name(Relation rel)
{
	ForeignTable *table;
	char *relname = NULL;
	ListCell *lc = NULL;

	/* 获取外表元数据 */
	table = GetForeignTable(RelationGetRelid(rel));

	/*
	 * 优先使用FDW选项中定义的表名
	 * 而不是Relation对象自身的名称
	 */
	foreach (lc, table->options)
	{
		DefElem *def = (DefElem *)lfirst(lc);

		if (strcmp(def->defname, "table") == 0)
			relname = defGetString(def);
	}

	/* 未找到FDW选项中的表名则使用Relation标准名称 */
	if (relname == NULL)
		relname = RelationGetRelationName(rel);

	return relname;
}

/*
 * 获取外表中指定列的列名
 *
 * 参数:
 *   @relid: 外表OID
 *   @attnum: 列属性编号
 *
 * 功能说明:
 *   1. 首先从外表的列选项中查找"column_name"定义
 *   2. 如果未找到自定义列名，则从系统目录获取默认列名
 *
 * 注意事项:
 *   - 返回的字符串指针由调用者管理，不应释放
 *   - 兼容PostgreSQL 11+版本
 */
char *
tdengine_get_column_name(Oid relid, int attnum)
{
	List *options = NULL;
	ListCell *lc_opt;
	char *colname = NULL;

	/* 获取外表的列选项 */
	options = GetForeignColumnOptions(relid, attnum);

	/* 遍历选项查找自定义列名 */
	foreach (lc_opt, options)
	{
		DefElem *def = (DefElem *)lfirst(lc_opt);

		if (strcmp(def->defname, "column_name") == 0)
		{
			colname = defGetString(def);
			break;
		}
	}

	/* 未找到自定义列名则获取系统默认列名 */
	if (colname == NULL)
		colname = get_attname(relid, attnum
#if (PG_VERSION_NUM >= 110000)
							  ,
							  false /* 不抛出错误 */
#endif
		);
	return colname;
}

/*
 * 检查指定列是否为标签键(tag key)
 *
 * 参数:
 *   @colname: 要检查的列名
 *   @reloid: 表对象ID
 *
 * 处理流程:
 *   1. 获取表的FDW选项配置
 *   2. 检查tags_list是否为空(空表示所有列都是字段)
 *   3. 遍历tags_list检查列名是否匹配
 *
 *   - 该函数用于确定列的分类以便正确处理查询下推
 */
bool tdengine_is_tag_key(const char *colname, Oid reloid)
{
	tdengine_opt *options;
	ListCell *lc;

	/* 获取表的FDW选项配置 */
	options = tdengine_get_options(reloid, GetUserId());

	/* 如果tags_list为空，表示所有列都是字段 */
	if (!options->tags_list)
		return false;

	/* 遍历tags_list检查列名是否匹配 */
	foreach (lc, options->tags_list)
	{
		char *name = (char *)lfirst(lc);

		if (strcmp(colname, name) == 0)
			return true;
	}

	return false;
}

/*****************************************************************************
 *		函数相关子句检查
 *****************************************************************************/

/*
 * tdengine_contain_functions_walker - 递归检查子句中是否包含函数调用
 *
 * 功能:
 *   递归遍历语法树节点，检查是否存在函数调用(包括操作符实现的函数)
 *
 * 参数:
 *   @node: 要检查的语法树节点
 *   @context: 上下文指针(当前未使用)
 *
 * 处理逻辑:
 *   1. 检查节点本身是否为函数表达式(FuncExpr)
 *   2. 对于查询节点(Query)，递归检查子查询
 *   3. 对于其他节点，递归检查所有子节点
 *
 * 注意事项:
 *   - 会递归检查TargetEntry表达式
 *   - 操作符也会被视为函数调用(因为操作符底层由函数实现)
 */
static bool
tdengine_contain_functions_walker(Node *node, void *context)
{
	/* 空节点直接返回false */
	if (node == NULL)
		return false;

	/* 检查当前节点是否为函数表达式 */
	if (nodeTag(node) == T_FuncExpr)
	{
		return true; /* 发现函数调用 */
	}

	/* 处理查询节点(子查询) */
	if (IsA(node, Query))
	{
		/* 递归检查子查询中的函数调用 */
		return query_tree_walker((Query *)node,
								 tdengine_contain_functions_walker,
								 context, 0);
	}

	/* 对于其他类型节点，递归检查所有子节点 */
	return expression_tree_walker(node,
								  tdengine_contain_functions_walker,
								  context);
}

/*
 * tdengine_is_foreign_function_tlist - 检查目标列表是否可以在远程服务器上安全执行
 *
 * 功能:
 *   检查目标列表中的表达式是否可以被安全下推到TDengine服务器执行
 *
 * 参数:
 *   @root: 规划器信息
 *   @baserel: 基础关系信息
 *   @tlist: 要检查的目标列表
 *
 * 返回值:
 *   true - 可以安全下推到远程服务器执行
 *   false - 不能下推到远程服务器
 *
 * 处理流程:
 *   1. 检查关系类型是否为基础关系或成员关系
 *   2. 检查目标列表是否包含函数调用
 *   3. 初始化上下文信息
 *   4. 遍历目标列表中的每个表达式:
 *      a. 设置全局上下文(规划器状态、外部表信息等)
 *      b. 设置局部上下文(排序规则、下推标志等)
 *      c. 递归检查表达式树是否可下推
 *      d. 检查排序规则安全性
 *      e. 检查可变函数
 *   5. 处理无模式类型变量的特殊情况
 */
bool tdengine_is_foreign_function_tlist(PlannerInfo *root,
										RelOptInfo *baserel,
										List *tlist)
{
	TDengineFdwRelationInfo *fpinfo = (TDengineFdwRelationInfo *)(baserel->fdw_private);
	ListCell *lc;
	bool is_contain_function;
	bool have_slvar_fields = false;
	foreign_glob_cxt glob_cxt;
	foreign_loc_cxt loc_cxt;

	/* 只处理基础关系或成员关系 */
	if (!(baserel->reloptkind == RELOPT_BASEREL ||
		  baserel->reloptkind == RELOPT_OTHER_MEMBER_REL))
		return false;

	/* 检查目标列表是否包含函数调用 */
	is_contain_function = false;
	foreach (lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		if (tdengine_contain_functions_walker((Node *)tle->expr, NULL))
		{
			is_contain_function = true;
			break;
		}
	}

	/* 目标列表不包含函数则直接返回false */
	if (!is_contain_function)
		return false;

	/* 初始化tdengine_time函数检查标志 */
	loc_cxt.have_otherfunc_tdengine_time_tlist = false;

	/* 遍历目标列表检查每个表达式 */
	foreach (lc, tlist)
	{
		TargetEntry *tle = lfirst_node(TargetEntry, lc);

		/* 设置全局上下文 */
		glob_cxt.root = root;
		glob_cxt.foreignrel = baserel;
		glob_cxt.relid = fpinfo->table->relid;
		glob_cxt.mixing_aggref_status = TDENGINE_TARGETS_MIXING_AGGREF_SAFE;
		glob_cxt.for_tlist = true;
		glob_cxt.is_inner_func = false;

		/* 设置关系ID集合 */
		if (baserel->reloptkind == RELOPT_UPPER_REL)
			glob_cxt.relids = fpinfo->outerrel->relids;
		else
			glob_cxt.relids = baserel->relids;

		/* 初始化局部上下文 */
		loc_cxt.collation = InvalidOid;
		loc_cxt.state = FDW_COLLATE_NONE;
		loc_cxt.can_skip_cast = false;
		loc_cxt.can_pushdown_stable = false;
		loc_cxt.can_pushdown_volatile = false;
		loc_cxt.tdengine_fill_enable = false;
		loc_cxt.has_time_key = false;
		loc_cxt.has_sub_or_add_operator = false;

		/* 递归检查表达式树 */
		if (!tdengine_foreign_expr_walker((Node *)tle->expr, &glob_cxt, &loc_cxt))
			return false;

		/*
		 * 当选择多个包含星号(*)或正则表达式函数的目标时不下推查询
		 * 原因: 多个星号或正则函数可能导致结果集过大或性能问题
		 */
		if (list_length(tlist) > 1 && loc_cxt.can_pushdown_stable)
		{
			elog(WARNING, "Selecting multiple functions with regular expression or star. The query are not pushed down.");
			return false;
		}

		/*
		 * 如果表达式有不安全的排序规则(非来自外部变量)，则不能下推
		 * 原因: 远程服务器可能无法正确处理本地排序规则
		 */
		if (loc_cxt.state == FDW_COLLATE_UNSAFE)
			return false;

		/*
		 * 检查可变函数(volatile functions):
		 * 1. 跳过FieldSelect类型的表达式
		 * 2. 如果不能下推可变函数:
		 *    a. 如果可以下推稳定函数，检查是否包含可变函数
		 *    b. 否则检查是否包含可变或稳定函数
		 * 原因: 可变函数结果不稳定(如now()等)，远程执行可能导致不一致
		 */
		if (!IsA(tle->expr, FieldSelect))
		{
			if (!loc_cxt.can_pushdown_volatile)
			{
				if (loc_cxt.can_pushdown_stable)
				{
					if (contain_volatile_functions((Node *)tle->expr))
						return false;
				}
				else
				{
					if (contain_mutable_functions((Node *)tle->expr))
						return false;
				}
			}
		}

		/* 检查变量节点是否为无模式字段键 */
		if (IsA(tle->expr, Var))
		{
			Var *var = (Var *)tle->expr;
			bool is_field_key = false;

			if (tdengine_is_slvar(var->vartype, var->varattno, &fpinfo->slinfo, NULL, &is_field_key) && is_field_key)
			{
				have_slvar_fields = true;
			}
		}
	}

	/*
	 * 处理无模式类型变量的特殊情况:
	 * 1. 如果存在无模式字段变量，且目标列表中有除tdengine_time()外的其他函数，则不下推
	 * 2. 标记需要从远程获取所有实际字段以构建JSON值
	 * 原因: 函数结果和无模式字段混合时无法正确区分结果列
	 */
	if (have_slvar_fields)
	{
		if (loc_cxt.have_otherfunc_tdengine_time_tlist)
		{
			return false;
		}
		fpinfo->all_fieldtag = true;
	}

	/* 目标列表中的函数可以安全地在远程服务器上执行 */
	return true;
}

/*
 * 检查节点是否为字符串类型
 *
 * 参数:
 *   @node: 要检查的语法树节点
 *   @pslinfo: 无模式信息结构体指针
 *
 * 功能说明:
 *   1. 处理不同类型的节点:
 *      - Var节点: 获取变量类型
 *      - Const节点: 获取常量类型
 *      - OpExpr节点: 处理操作符表达式
 *      - CoerceViaIO节点: 处理类型转换表达式
 *      - 其他节点: 递归检查子节点
 *   2. 检查类型OID是否为字符串类型(CHAROID/VARCHAROID/TEXTOID/BPCHAROID/NAMEOID)
 *
 * 注意事项:
 *   - 使用expression_tree_walker递归处理子节点
 *   - 对于无模式变量会调用tdengine_is_slvar_fetch进行特殊处理
 */
static bool
tdengine_is_string_type(Node *node, schemaless_info *pslinfo)
{
	Oid oidtype = 0;

	if (node == NULL)
		return false;

	if (IsA(node, Var))
	{
		Var *var = (Var *)node;
		oidtype = var->vartype;
	}
	else if (IsA(node, Const))
	{
		Const *c = (Const *)node;
		oidtype = c->consttype;
	}
	else if (IsA(node, OpExpr))
	{
		OpExpr *oe = (OpExpr *)node;

		if (tdengine_is_slvar_fetch(node, pslinfo))
		{
			oidtype = oe->opresulttype;
		}
		else
			return expression_tree_walker(node, tdengine_is_string_type, pslinfo);
	}
	else if (IsA(node, CoerceViaIO))
	{
		CoerceViaIO *cio = (CoerceViaIO *)node;
		Node *arg = (Node *)cio->arg;

		if (tdengine_is_slvar_fetch(arg, pslinfo))
		{
			oidtype = cio->resulttype;
		}
		else
			return expression_tree_walker(node, tdengine_is_string_type, pslinfo);
	}
	else
	{
		return expression_tree_walker(node, tdengine_is_string_type, pslinfo);
	}

	switch (oidtype)
	{
	case CHAROID:
	case VARCHAROID:
	case TEXTOID:
	case BPCHAROID:
	case NAMEOID:
		return true;
	default:
		return false;
	}
}

/*
 * 检查函数名是否存在于给定的函数列表中
 *
 * 参数:
 *   @funcname: 要检查的函数名称字符串
 *   @funclist: 以NULL结尾的函数名称数组(字符串指针数组)
 *
 * 注意事项:
 *   - 函数名比较区分大小写
 */
static bool
exist_in_function_list(char *funcname, const char **funclist)
{
	int i;

	for (i = 0; funclist[i]; i++)
	{
		if (strcmp(funcname, funclist[i]) == 0)
			return true;
	}
	return false;
}

/*
 * tdengine_is_select_all: 检查是否为全表查询(select *)
 *
 * 参数:
 *   @rte: 范围表条目
 *   @tlist: 目标列表
 *   @pslinfo: 无模式信息结构体指针
 *
 * 返回值:
 *   true - 是全表查询或需要所有列的情况
 *   false - 不是全表查询
 *
 * 功能说明:
 *   1. 检查目标列表是否包含所有有效列
 *   2. 处理以下特殊情况:
 *      - 包含关系类型ID的变量
 *      - 包含整行引用(varattno=0)
 *      - 包含无模式类型变量
 *   3. 统计匹配的列数
 *
 * 处理流程:
 *   1. 打开表获取元组描述符
 *   2. 遍历所有属性
 *   3. 跳过已删除的属性
 *   4. 检查目标列表中的每个变量
 *   5. 判断是否满足全表查询条件
 */
bool tdengine_is_select_all(RangeTblEntry *rte, List *tlist, schemaless_info *pslinfo)
{
	int i;
	int natts = 0;
	int natts_valid = 0;
	Relation rel = table_open(rte->relid, NoLock);
	TupleDesc tupdesc = RelationGetDescr(rel);
	Oid rel_type_id;
	bool has_rel_type_id = false;
	bool has_slcol = false;
	bool has_wholerow = false;

	/* 打开表并获取元组描述符 */
	Relation rel = table_open(rte->relid, NoLock);
	TupleDesc tupdesc = RelationGetDescr(rel);

	/* 获取关系类型ID */
	rel_type_id = get_rel_type_id(rte->relid);

	/* 遍历表的所有属性 */
	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);
		ListCell *lc;

		/* 跳过已删除的属性 */
		if (attr->attisdropped)
			continue;

		/* 遍历目标列表 */
		foreach (lc, tlist)
		{
			Node *node = (Node *)lfirst(lc);

			if (IsA(node, TargetEntry))
				node = (Node *)((TargetEntry *)node)->expr;

			if (IsA(node, Var))
			{
				Var *var = (Var *)node;

				/* 检查关系类型ID匹配 */
				if (var->vartype == rel_type_id)
				{
					has_rel_type_id = true;
					break;
				}

				/* 检查整行引用 */
				if (var->varattno == 0)
				{
					has_wholerow = true;
					break;
				}

				/* 检查无模式类型变量 */
				if (tdengine_is_slvar(var->vartype, var->varattno, pslinfo, NULL, NULL))
				{
					has_slcol = true;
					break;
				}

				/* 检查属性匹配 */
				if (var->varattno == attr->attnum)
				{
					natts++;
					break;
				}
			}
		}
	}

	/* 关闭表并返回结果 */
	table_close(rel, NoLock);
	return ((natts == natts_valid) || has_rel_type_id || has_slcol || has_wholerow);
}

/*
 * 检查无模式表中是否没有字段键(field key)
 * 功能: 判断给定的无模式列列表中是否只包含时间列和标签键列
 *
 * 参数:
 *   @reloid: 表对象ID
 *   @slcols: 无模式列列表(StringInfo结构列表)
 *
 * 返回值:
 *   true - 列表中没有字段键(只有时间列和标签键列)
 *   false - 列表中存在至少一个字段键
 *
 * 处理流程:
 *   1. 初始化返回值为true(假设没有字段键)
 *   2. 遍历无模式列列表中的每个列:
 *      a. 跳过时间列(TDENGINE_IS_TIME_COLUMN)
 *      b. 检查当前列是否为标签键(tdengine_is_tag_key)
 *      c. 如果发现非标签键列，设置返回值为false并终止检查
 *   3. 返回最终检查结果
 *
 * 注意事项:
 *   - 该函数主要用于无模式表(schemaless table)处理
 *   - 时间列和标签键列不被视为字段键
 */
static bool
tdengine_is_no_field_key(Oid reloid, List *slcols)
{
	int i;
	bool no_field_key = true; // 初始假设没有字段键

	/* 遍历无模式列列表 */
	for (i = 1; i <= list_length(slcols); i++)
	{
		StringInfo *rcol = (StringInfo *)list_nth(slcols, i - 1); // 获取当前列
		char *colname = strVal(rcol);							  // 获取列名

		/* 跳过时间列 */
		if (!TDENGINE_IS_TIME_COLUMN(colname))
		{
			/* 检查是否为标签键列 */
			if (!tdengine_is_tag_key(colname, reloid))
			{
				/* 发现字段键列，设置标志并终止循环 */
				no_field_key = false;
				break;
			}
		}
	}

	return no_field_key;
}
/*
 * 反解析无模式表的目标列表
 * 功能: 为无模式表生成远程查询的SELECT列列表
 *
 * 参数:
 *   @buf: 输出字符串缓冲区
 *   @rel: 表关系对象
 *   @reloid: 表OID
 *   @attrs_used: 使用的属性位图集合
 *   @retrieved_attrs: 返回获取的属性索引列表
 *   @all_fieldtag: 是否选择所有字段标签
 *   @slcols: 无模式列列表
 *
 * 处理流程:
 *   1. 检查是否没有字段键(只有标签键和时间列)
 *   2. 遍历表的所有属性:
 *      a. 跳过已删除的属性
 *      b. 如果选择所有字段标签或没有字段键或属性被使用，则添加到返回列表
 *   3. 如果选择所有字段标签或没有字段键，直接输出"*"并返回
 *   4. 否则遍历无模式列列表:
 *      a. 跳过时间列
 *      b. 添加逗号分隔符(非第一个列)
 *      c. 添加带引号的列名
 *
 * 注意事项:
 *   - 用于SELECT和RETURNING目标列表
 *   - 无模式表的列处理需要特殊逻辑
 *   - 时间列(TDENGINE_IS_TIME_COLUMN)会被自动跳过
 */
static void
tdengine_deparse_target_list_schemaless(StringInfo buf,
										Relation rel,
										Oid reloid,
										Bitmapset *attrs_used,
										List **retrieved_attrs,
										bool all_fieldtag,
										List *slcols)
{
	TupleDesc tupdesc = RelationGetDescr(rel); // 获取表元组描述符
	bool first;								   // 是否是第一个列
	int i;									   // 循环计数器
	bool no_field_key;						   // 是否没有字段键

	// 检查是否没有字段键(只有标签键和时间列)
	no_field_key = tdengine_is_no_field_key(reloid, slcols);

	// 初始化返回的属性索引列表
	*retrieved_attrs = NIL;

	// 遍历表的所有属性
	for (i = 1; i <= tupdesc->natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i - 1);

		/* 跳过已删除的属性 */
		if (attr->attisdropped)
			continue;

		/* 如果选择所有字段标签或没有字段键或属性被使用，则添加到返回列表 */
		if (all_fieldtag || no_field_key ||
			bms_is_member(i - FirstLowInvalidHeapAttributeNumber,
						  attrs_used))
			*retrieved_attrs = lappend_int(*retrieved_attrs, i);
	}

	/* 如果选择所有字段标签或没有字段键，直接输出"*"并返回 */
	if (all_fieldtag || no_field_key)
	{
		appendStringInfoString(buf, "*");
		return;
	}

	/* 遍历无模式列列表 */
	first = true;
	for (i = 1; i <= list_length(slcols); i++)
	{
		StringInfo *rcol = (StringInfo *)list_nth(slcols, i - 1);
		char *colname = strVal(rcol);

		/* 跳过时间列 */
		if (!TDENGINE_IS_TIME_COLUMN(colname))
		{
			/* 非第一个列时添加逗号分隔符 */
			if (!first)
				appendStringInfoString(buf, ", ");
			first = false;
			/* 添加带引号的列名 */
			appendStringInfoString(buf, tdengine_quote_identifier(colname, QUOTE));
		}
	}
}

/*
 * 反解析CoerceViaIO类型转换节点
 * 功能: 处理通过输入/输出函数进行的类型转换表达式
 *
 * 参数:
 *   @cio: 要反解析的CoerceViaIO节点
 *   @context: 反解析上下文，包含输出缓冲区和相关状态
 *
 * 处理流程:
 *   1. 检查是否为无模式(schemaless)表查询
 *   2. 处理两种特殊情况:
 *      a. 无模式变量(tdengine_tags/tdengine_fields): 调用tdengine_deparse_slvar处理
 *      b. 参数节点: 调用tdengine_deparse_param处理
 *   3. 特殊处理布尔类型转换: 添加"= true"比较
 *
 * 注意事项:
 *   - 仅用于无模式表查询
 *   - 布尔类型转换需要特殊处理以兼容TDengine语法
 */
static void
tdengine_deparse_coerce_via_io(CoerceViaIO *cio, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf; // 输出缓冲区
	TDengineFdwRelationInfo *fpinfo =
		(TDengineFdwRelationInfo *)(context->foreignrel->fdw_private); // 获取FDW关系信息
	OpExpr *oe = (OpExpr *)cio->arg;								   // 获取转换参数表达式

	// 确保是无模式表查询
	Assert(fpinfo->slinfo.schemaless);

	/* 检查是否为无模式变量 */
	if (tdengine_is_slvar_fetch((Node *)oe, &(fpinfo->slinfo)))
	{
		// 反解析无模式变量表达式
		tdengine_deparse_slvar((Node *)cio,
							   linitial_node(Var, oe->args),  // 获取变量节点
							   lsecond_node(Const, oe->args), // 获取常量节点
							   context);
	}
	/* 检查是否为参数节点 */
	else if (tdengine_is_param_fetch((Node *)oe, &(fpinfo->slinfo)))
	{
		// 反解析参数表达式
		tdengine_deparse_param((Param *)cio, context);
	}

	/* 特殊处理布尔类型转换 */
	if (cio->resulttype == BOOLOID && context->has_bool_cmp)
	{
		appendStringInfoString(buf, " = true"); // 添加布尔比较
	}
}

/*
 * 反解析无模式变量(tdengine_tags/tdengine_fields)表达式
 * 功能: 将无模式变量(Var)和常量(Const)组合的表达式转换为SQL字符串
 *
 * 参数:
 *   @node: 表达式节点(包含Var和Const的组合)
 *   @var: 变量节点(表示tdengine_tags/tdengine_fields列)
 *   @cnst: 常量节点(表示标签或字段名)
 *   @context: 反解析上下文
 *
 * 处理逻辑:
 *   1. 检查变量是否属于当前扫描关系
 *     - 如果是: 直接引用常量值作为标识符
 *     - 如果不是: 作为参数处理
 *   2. 参数处理分为两种情况:
 *     - 如果存在参数列表: 查找或添加参数索引
 *     - 否则: 生成参数占位符
 */
static void
tdengine_deparse_slvar(Node *node, Var *var, Const *cnst, deparse_expr_cxt *context)
{
	StringInfo buf = context->buf;			  // 输出缓冲区
	Relids relids = context->scanrel->relids; // 当前扫描关系ID集合

	/* 检查变量是否属于当前扫描关系且没有上层引用 */
	if (bms_is_member(var->varno, relids) && var->varlevelsup == 0)
	{
		// 直接引用常量值作为标识符(添加引号处理)
		appendStringInfo(buf, "%s", tdengine_quote_identifier(TextDatumGetCString(cnst->constvalue), QUOTE));
	}
	else
	{
		/* 作为参数处理 */
		if (context->params_list)
		{
			int pindex = 0; // 参数索引
			ListCell *lc;	// 列表迭代器

			/* 在参数列表中查找当前节点的索引 */
			foreach (lc, *context->params_list)
			{
				pindex++;
				if (equal(node, (Node *)lfirst(lc)))
					break;
			}
			if (lc == NULL)
			{
				/* 如果不在列表中，则添加 */
				pindex++;
				*context->params_list = lappend(*context->params_list, node);
			}
			// 打印远程参数引用
			tdengine_print_remote_param(pindex, var->vartype, var->vartypmod, context);
		}
		else
		{
			// 打印远程参数占位符
			tdengine_print_remote_placeholder(var->vartype, var->vartypmod, context);
		}
	}
}
