#include "postgres.h"
#include "tdengine_fdw.h"

#include "access/htup_details.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "nodes/nodeFuncs.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

// TODO: 查文档看具体支持的函数
/* List of stable function with star argument of TDengine */
static const char *TDengineStableStarFunction[] = {
	"influx_count_all",
	"influx_mode_all",
	"influx_max_all",
	"influx_min_all",
	"influx_sum_all",
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
	"influx_count",
	"integral",
	"spread",
	"first",
	"last",
	"sample",
	"influx_time",
	"influx_fill_numeric",
	"influx_fill_option",
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
 *   @influx_fill_enable: 是否在influx_time()内解析子表达式 TODO:
 *   @have_otherfunc_influx_time_tlist: 目标列表中是否有除influx_time()外的其他函数 TODO:
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
	bool influx_fill_enable;			   // TODO:
	bool have_otherfunc_influx_time_tlist; // TODO:
	bool has_time_key;
	bool has_sub_or_add_operator;
	bool is_comparison;
} foreign_loc_cxt;

/*
 * Returns true if given expr is safe to evaluate on the foreign server.
 */
bool tdengine_is_foreign_expr(PlannerInfo *root,
							  RelOptInfo *baserel,
							  Expr *expr,
							  bool for_tlist)
{
	foreign_glob_cxt glob_cxt;
	foreign_loc_cxt loc_cxt;
	TDengineFdwRelationInfo *fpinfo = (TDengineFdwRelationInfo *)(baserel->fdw_private);

	/*
	 * Check that the expression consists of nodes that are safe to execute
	 * remotely.
	 */
	glob_cxt.root = root;
	glob_cxt.foreignrel = baserel;
	glob_cxt.relid = fpinfo->table->relid;
	glob_cxt.mixing_aggref_status = TDENGINE_TARGETS_MIXING_AGGREF_SAFE;
	glob_cxt.for_tlist = for_tlist;
	glob_cxt.is_inner_func = false;

	/*
	 * For an upper relation, use relids from its underneath scan relation,
	 * because the upperrel's own relids currently aren't set to anything
	 * meaningful by the core code.  For other relation, use their own relids.
	 */
	if (baserel->reloptkind == RELOPT_UPPER_REL)
		glob_cxt.relids = fpinfo->outerrel->relids;
	else
		glob_cxt.relids = baserel->relids;
	loc_cxt.collation = InvalidOid;
	loc_cxt.state = FDW_COLLATE_NONE;
	loc_cxt.can_skip_cast = false;
	loc_cxt.influx_fill_enable = false;
	loc_cxt.has_time_key = false;
	loc_cxt.has_sub_or_add_operator = false;
	loc_cxt.is_comparison = false;
	if (!tdengine_foreign_expr_walker((Node *)expr, &glob_cxt, &loc_cxt))
		return false;

	/*
	 * If the expression has a valid collation that does not arise from a
	 * foreign var, the expression can not be sent over.
	 */
	if (loc_cxt.state == FDW_COLLATE_UNSAFE)
		return false;

	/* OK to evaluate on the remote server */
	return true;
}

/*
 * is_valid_type: 检查给定的OID是否为TDengine支持的有效数据类型
 *
 * 参数:
 *   @type: 要检查的数据类型OID
 *
 * 返回值:
 *   true - 是TDengine支持的数据类型
 *   false - 不是TDengine支持的数据类型
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
 * 返回值:
 *   true - 表达式可以安全下推到TDengine执行
 *   false - 表达式不能下推到TDengine执行
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
	inner_cxt.influx_fill_enable = false;
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
		 * 功能: 检查常量类型是否为特殊类型"influx_fill_enum"
		 *      如果是则跳过内置类型检查
		 */
		type_name = tdengine_get_data_type_name(c->consttype);
		if (strcmp(type_name, "influx_fill_enum") == 0)
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
		/* fill() must be inside influx_time() */
		if (strcmp(opername, "influx_fill_numeric") == 0 ||
			strcmp(opername, "influx_fill_option") == 0)
		{
			if (outer_cxt->influx_fill_enable == false)
				elog(ERROR, "tdengine_fdw: syntax error influx_fill_numeric() or influx_fill_option() must be embedded inside influx_time() function\n");
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
		 * Allow influx_fill_numeric/influx_fill_option() inside
		 * influx_time() function
		 */
		if (strcmp(opername, "influx_time") == 0)
		{
			inner_cxt.influx_fill_enable = true;
		}
		else
		{
			/* There is another function than influx_time in tlist */
			outer_cxt->have_otherfunc_influx_time_tlist = true;
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
		inner_cxt.influx_fill_enable = false;

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
		 *      - 填充函数启用标志(influx_fill_enable)
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
		inner_cxt.influx_fill_enable = outer_cxt->influx_fill_enable;
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

		// TODO: 适配tdengine
		/* these function can be passed to TDengine */
		if ((strcmp(opername, "sum") == 0 ||
			 strcmp(opername, "max") == 0 ||
			 strcmp(opername, "min") == 0 ||
			 strcmp(opername, "count") == 0 ||
			 strcmp(opername, "influx_distinct") == 0 || // TODO: 适配tdengine
			 strcmp(opername, "spread") == 0 ||
			 strcmp(opername, "sample") == 0 ||
			 strcmp(opername, "first") == 0 ||
			 strcmp(opername, "last") == 0 ||
			 strcmp(opername, "integral") == 0 ||
			 strcmp(opername, "mean") == 0 ||
			 strcmp(opername, "median") == 0 ||
			 strcmp(opername, "influx_count") == 0 || // TODO: 适配tdengine
			 strcmp(opername, "influx_mode") == 0 ||
			 strcmp(opername, "stddev") == 0 ||
			 strcmp(opername, "influx_sum") == 0 || // TODO: 适配tdengine
			 strcmp(opername, "influx_max") == 0 || // TODO: 适配tdengine
			 strcmp(opername, "influx_min") == 0))	// TODO: 适配tdengine
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
 *   - 对于无模式变量会调用influxdb_is_slvar_fetch进行特殊处理
 */
static bool
influxdb_is_string_type(Node *node, schemaless_info *pslinfo)
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

		if (influxdb_is_slvar_fetch(node, pslinfo))
		{
			oidtype = oe->opresulttype;
		}
		else
			return expression_tree_walker(node, influxdb_is_string_type, pslinfo);
	}
	else if (IsA(node, CoerceViaIO))
	{
		CoerceViaIO *cio = (CoerceViaIO *)node;
		Node *arg = (Node *)cio->arg;

		if (influxdb_is_slvar_fetch(arg, pslinfo))
		{
			oidtype = cio->resulttype;
		}
		else
			return expression_tree_walker(node, influxdb_is_string_type, pslinfo);
	}
	else
	{
		return expression_tree_walker(node, influxdb_is_string_type, pslinfo);
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
