// 

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

extern char *tdengine_replace_function(char *in);

/*
 * tdengine_convert_to_pg: Convert tdengine string data into PostgreSQL's compatible data types
 */
Datum
tdengine_convert_to_pg(Oid pgtyp, int pgtypmod, char *value)
{
	Datum		value_datum = 0;
	Datum		valueDatum = 0;
	regproc		typeinput;
	HeapTuple	tuple;
	int			typemod;

	/* get the type's output function */
	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pgtyp));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for type%u", pgtyp);

	typeinput = ((Form_pg_type) GETSTRUCT(tuple))->typinput;
	typemod = ((Form_pg_type) GETSTRUCT(tuple))->typtypmod;
	ReleaseSysCache(tuple);
	valueDatum = CStringGetDatum(value);

	/* convert string value to appropriate type value */
	value_datum = OidFunctionCall3(typeinput, valueDatum, ObjectIdGetDatum(InvalidOid), Int32GetDatum(typemod));

	return value_datum;
}

/*
 * tdengine_convert_record_to_datum: Convert tdengine string data into PostgreSQL's compatible data types
 */
Datum
tdengine_convert_record_to_datum(Oid pgtyp, int pgtypmod, char **row, int attnum, int ntags, int nfield,
								 char **column, char *opername, Oid relid, int ncol, bool is_schemaless)
{
	Datum		value_datum = 0;
	Datum		valueDatum = 0;
	regproc		typeinput;
	HeapTuple	tuple;
	int			typemod;
	int			i;
	StringInfoData fields_jsstr;
	StringInfo	record = makeStringInfo();
	bool		first = true;
	bool		is_sc_agg_starregex = false;
	bool		need_enclose_brace = false;
	char	   *foreignColName = NULL;
	char	   *tdengineFuncName = tdengine_replace_function(opername);
	int			nmatch = 0;

	/* get the type's output function */
	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pgtyp));
	if (!HeapTupleIsValid(tuple))
		elog(ERROR, "cache lookup failed for type%u", pgtyp);

	typeinput = ((Form_pg_type) GETSTRUCT(tuple))->typinput;
	typemod = ((Form_pg_type) GETSTRUCT(tuple))->typtypmod;
	ReleaseSysCache(tuple);

	/* Build the fields json string value */
	if (is_schemaless)
		initStringInfo(&fields_jsstr);

	/* Append time column */
	appendStringInfo(record, "(%s,", row[0]);

	if (is_schemaless)
	{
		/*
		 * Dummy value for tags jsonb schemaless column,
		 * it is not number of actual tag keys.
		 */
		ntags = 1;
	}

	/* Append tags column as NULL */
	for (i = 0; i < ntags; i++)
		appendStringInfo(record, ",");

	/* Append fields column */
	i = 0;
	do
	{
		if (is_schemaless)
		{
			if (i < ncol)
				foreignColName = column[i];
			else
				foreignColName = NULL;
			i++;

			is_sc_agg_starregex = true;
		}
		else
		foreignColName = get_attname(relid, ++i
#if (PG_VERSION_NUM >= 110000)
									 ,
									 true
#endif
			);

		if (foreignColName != NULL &&
			!TDENGINE_IS_TIME_COLUMN(foreignColName) &&
			!tdengine_is_tag_key(foreignColName, relid))
		{
			bool		match = false;
			int			j;

			for (j = attnum; j < ncol; j++)
			{
				/*
				 * TDengine returns column name of result in format:
				 * functionname_columnname (Example: last_value1). We need to
				 * concatenate string to compare with the column returned from
				 * TDengine.
				 */
				char	   *tdengineColName = column[j];
				char	   *tmpName;

				if (is_sc_agg_starregex)
					tmpName = foreignColName;
				else
					tmpName = psprintf("%s_%s", tdengineFuncName, foreignColName);

				if (strcmp(tmpName, tdengineColName) == 0)
				{
					match = true;
					nmatch++;

					if (is_schemaless)
					{
						char *colname = NULL;
						char *escaped_key = NULL;
						char *escaped_value = NULL;

						/* Escape comma charater in PostgreSQL composite type */
						if (!first)
							appendStringInfoChar(&fields_jsstr, ',');

						/* Get actual column name */
						colname = pstrdup(tmpName + strlen(tdengineFuncName) + 1); /* Skip "functionname_" */

						escaped_key = tdengine_escape_json_string(colname);
						pfree(colname);

						if (escaped_key == NULL)
							elog(ERROR, "Cannot escape json column key");

						escaped_value = tdengine_escape_json_string(row[j]);

						if (need_enclose_brace == false)
						{
							appendStringInfoChar(&fields_jsstr, '{');
							need_enclose_brace = true;
						}

						/* Build json string value for fields column */
						appendStringInfo(&fields_jsstr, "\"%s\" : ", escaped_key); /* \"key\" : */
						if (escaped_value)
							appendStringInfo(&fields_jsstr, "\"%s\"", escaped_value); /* \"value\" */
						else
							appendStringInfoString(&fields_jsstr, "null"); /* null */
					}
					else
					{
						if (!first)
							appendStringInfoChar(record, ',');

						appendStringInfo(record, "%s", row[j] != NULL ? row[j] : "");
					}
					first = false;
					break;
				}
			}
			if (!is_sc_agg_starregex && nmatch == nfield)
				break;

			/* Column of temp table does not match regex, fill as NULL */
			if (!is_schemaless && match == false)
				appendStringInfo(record, ",");
		}

		is_sc_agg_starregex = false;
	} while (foreignColName != NULL);

	if (is_schemaless)
	{
		char *escaped_fields_jsstr = NULL;

		if (need_enclose_brace)
			appendStringInfoString(&fields_jsstr, " }");

		/* Escape json string value in PostgreSQL composite type */
		escaped_fields_jsstr = tdengine_escape_record_string(fields_jsstr.data);

		/* Nested the escaped json string value in record */
		appendStringInfo(record, "%s", (escaped_fields_jsstr != NULL) ? escaped_fields_jsstr : "");
	}

	appendStringInfo(record, ")");
	valueDatum = CStringGetDatum(record->data);

	/* convert string value to appropriate type value */
	value_datum = OidFunctionCall3(typeinput, valueDatum, ObjectIdGetDatum(InvalidOid), Int32GetDatum(typemod));

	return value_datum;
}

/*
 * tdengine_bind_sql_var - 将PostgreSQL数据类型绑定为TDengine兼容类型
 * 功能: 将PostgreSQL的Datum值转换为TDengine支持的变量类型和值
 *
 * 参数:
 *   @type: PostgreSQL数据类型OID
 *   @idx: 参数索引位置
 *   @value: PostgreSQL原始数据值(Datum格式)
 *   @param_column_info: 列信息结构体数组
 *   @param_tdengine_types: 输出参数，存储转换后的TDengine类型
 *   @param_tdengine_values: 输出参数，存储转换后的TDengine值
 *
 * 处理流程:
 *   1. 获取类型的输出函数信息
 *   2. 根据不同类型进行转换:
 *      a. 整数类型(INT2/INT4/INT8): 统一转为TDENGINE_INT64
 *      b. 浮点类型(FLOAT4/FLOAT8/NUMERIC): 转为TDENGINE_DOUBLE
 *      c. 布尔类型: 转为TDENGINE_BOOLEAN
 *      d. 字符串类型: 转为TDENGINE_STRING
 *      e. 时间类型: 根据列类型决定转换方式
 *         - 时间键列: 转为纳秒时间戳(TDENGINE_TIME)
 *         - 其他列: 转为字符串(TDENGINE_STRING)
 *   3. 不支持的类型抛出错误
 */
void
tdengine_bind_sql_var(Oid type, int idx, Datum value, TDengineColumnInfo *param_column_info,
                     TDengineType * param_tdengine_types, TDengineValue * param_tdengine_values)
{
    // 初始化类型输出函数信息
    Oid     outputFunctionId = InvalidOid;
    bool    typeVarLength = false;

    /* 获取类型的输出函数信息 */
    getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);

    /* 根据不同类型进行转换处理 */
    switch (type)
    {
        /* 16位整数处理 */
        case INT2OID:
            {
                // 获取16位整数值
                int16 dat = DatumGetInt16(value);
                // 存储为64位整数
                param_tdengine_values[idx].i = dat;
                param_tdengine_types[idx] = TDENGINE_INT64;
                break;
            }

        /* 32位整数处理 */
        case INT4OID:
            {
                // 获取32位整数值
                int32 dat = DatumGetInt32(value);
                // 存储为64位整数
                param_tdengine_values[idx].i = dat;
                param_tdengine_types[idx] = TDENGINE_INT64;
                break;
            }

        /* 64位整数处理 */
        case INT8OID:
            {
                // 获取64位整数值
                int64 dat = DatumGetInt64(value);
                // 直接存储为64位整数
                param_tdengine_values[idx].i = dat;
                param_tdengine_types[idx] = TDENGINE_INT64;
                break;
            }

        /* 32位浮点数处理 */
        case FLOAT4OID:
            {
                // 获取32位浮点值
                float4 dat = DatumGetFloat4(value);
                // 转换为双精度存储
                param_tdengine_values[idx].d = (double) dat;
                param_tdengine_types[idx] = TDENGINE_DOUBLE;
                break;
            }

        /* 64位浮点数处理 */
        case FLOAT8OID:
            {
                // 获取64位浮点值
                float8 dat = DatumGetFloat8(value);
                // 直接存储为双精度
                param_tdengine_values[idx].d = dat;
                param_tdengine_types[idx] = TDENGINE_DOUBLE;
                break;
            }

        /* 数值类型处理 */
        case NUMERICOID:
            {
                // 先将数值转为浮点数
                Datum valueDatum = DirectFunctionCall1(numeric_float8, value);
                float8 dat = DatumGetFloat8(valueDatum);
                // 存储为双精度
                param_tdengine_values[idx].d = dat;
                param_tdengine_types[idx] = TDENGINE_DOUBLE;
                break;
            }

        /* 布尔类型处理 */
        case BOOLOID:
            {
                // 获取布尔值
                bool dat = DatumGetBool(value);
                // 直接存储
                param_tdengine_values[idx].b = dat;
                param_tdengine_types[idx] = TDENGINE_BOOLEAN;
                break;
            }

        /* 文本类型处理 */
        case TEXTOID:
        case BPCHAROID:
        case VARCHAROID:
            {
                // 初始化字符串输出变量
                char *outputString = NULL;
                outputFunctionId = InvalidOid;
                typeVarLength = false;

                // 获取类型输出函数
                getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
                // 调用输出函数获取字符串表示
                outputString = OidOutputFunctionCall(outputFunctionId, value);
                // 存储字符串
                param_tdengine_values[idx].s = outputString;
                param_tdengine_types[idx] = TDENGINE_STRING;
                break;
            }

        /* 时间类型处理 */
        case TIMEOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID:
            {
                // 检查是否为时间键列
                if (param_column_info[idx].column_type == TDENGINE_TIME_KEY)
                {
                    // 计算PostgreSQL和Unix时间戳的差异(微秒)
                    const int64 epoch_diff = (POSTGRES_EPOCH_JDATE - UNIX_EPOCH_JDATE) * USECS_PER_DAY;
                    // 获取时间戳值
                    Timestamp ts = DatumGetTimestamp(value);
                    // 转换为纳秒时间戳
                    int64 nanos = (ts + epoch_diff) * 1000;
                    
                    // 存储为时间类型
                    param_tdengine_values[idx].i = nanos;
                    param_tdengine_types[idx] = TDENGINE_TIME;
                }
                else
                {
                    // 非时间键列处理为字符串
                    char *outputString = NULL;
                    outputFunctionId = InvalidOid;
                    typeVarLength = false;

                    // 获取类型输出函数
                    getTypeOutputInfo(type, &outputFunctionId, &typeVarLength);
                    // 调用输出函数获取字符串表示
                    outputString = OidOutputFunctionCall(outputFunctionId, value);
                    // 存储字符串
                    param_tdengine_values[idx].s = outputString;
                    param_tdengine_types[idx] = TDENGINE_STRING;
                }
                break;
            }

        /* 不支持的类型处理 */
        default:
            {
                // 抛出错误，报告不支持的类型
                ereport(ERROR, (errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
                              errmsg("无法将常量值转换为TDengine值 %u", type),
                              errhint("常量值数据类型: %u", type)));
                break;
            }
    }
}

