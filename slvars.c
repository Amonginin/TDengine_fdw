 #include "postgres.h"
 #include "catalog/pg_type.h"
 #include "commands/defrem.h"
 #include "nodes/nodeFuncs.h"
 #include "parser/parse_oper.h"
 #include "parser/parse_type.h"
 #include "utils/builtins.h"
 #include "utils/lsyscache.h"
 #include "utils/syscache.h"
 #include "tdengine_fdw.h"
 
 /*
  * Context for schemaless vars walker
  */
 typedef struct pull_slvars_context
 {
     Index				varno;
     schemaless_info		*pslinfo;
     List				*columns;
     bool                extract_raw;
     List                *remote_exprs;
 } pull_slvars_context;
 
 static bool tdengine_slvars_walker(Node *node, pull_slvars_context *context);
 static bool tdengine_is_att_dropped(Oid relid, AttrNumber attnum);
 static void tdengine_validate_foreign_table_sc(Oid reloid);
 
 /*
   * 检查节点是否为无模式(schemaless)类型变量
   *
   * 参数说明:
   * @oid 列的数据类型OID
   * @attnum 列属性编号
   * @pslinfo 无模式信息结构体指针
   * @is_tags 输出参数，返回是否为tags列
   * @is_fields 输出参数，返回是否为fields列
   *
   * 返回值:
   * true - 是无模式类型变量
   * false - 不是无模式类型变量
   *
   * 功能说明:
   * 1. 检查是否启用无模式
   * 2. 获取列选项并检查tags/fields选项
   * 3. 验证数据类型是否为JSONB
   * 4. 返回检查结果和列类型(tags/fields)
   */
  bool
  tdengine_is_slvar(Oid oid, int attnum, schemaless_info *pslinfo, bool *is_tags, bool *is_fields)
  {
      List       *options;
      ListCell   *lc;
      bool       tags_opt = false;
      bool       fields_opt = false;
  
      /* 检查是否启用无模式 */
      if (!pslinfo->schemaless)
          return false;
  
      /* 获取列选项 */
      options = GetForeignColumnOptions(pslinfo->relid, attnum);
      
      /* 遍历选项检查tags/fields设置 */
      foreach(lc, options)
      {
          DefElem    *def = (DefElem *) lfirst(lc);
  
          if (strcmp(def->defname, "tags") == 0)
          {
              tags_opt = defGetBoolean(def);
              break;
          }
          else if (strcmp(def->defname, "fields") == 0)
          {
              fields_opt = defGetBoolean(def);
              break;
          }
      }
  
      /* 设置输出参数 */
      if (is_tags)
          *is_tags = tags_opt;
      if (is_fields)
          *is_fields = fields_opt;
  
      /* 最终检查: 数据类型为JSONB且设置了tags/fields选项 */
      if ((oid == pslinfo->slcol_type_oid) &&
          (tags_opt || fields_opt))
          return true;
  
      return false;
  }
 
 /*
  * 检查节点是否为无模式(schemaless)变量的提取操作
  *
  * 参数说明:
  * @node 要检查的表达式节点
  * @pslinfo 无模式信息结构体指针
  *
  * 返回值:
  * true - 是无模式变量提取操作
  * false - 不是无模式变量提取操作
  *
  * 功能说明:
  * 1. 检查是否启用无模式
  * 2. 处理CoerceViaIO类型转换节点
  * 3. 验证节点是否为操作表达式(OpExpr)
  * 4. 检查操作符是否为JSONB提取操作符(->>)
  * 5. 验证参数数量和类型
  * 6. 最终确认是否为有效的无模式变量提取
  */
 bool
 tdengine_is_slvar_fetch(Node *node, schemaless_info *pslinfo)
 {
     /* 获取操作表达式 */
     OpExpr *oe = (OpExpr *)node;
     Node *arg1;
     Node *arg2;
 
     /* 检查无模式是否启用 */
     if (!pslinfo->schemaless)
         return false;
 
     /* 处理类型转换节点 */
     if (IsA(node, CoerceViaIO))
     {
         node = (Node *) (((CoerceViaIO *)node)->arg);
         oe = (OpExpr *)node;
     }
 
     /* 检查是否为操作表达式 */
     if (!IsA(node, OpExpr))
         return false;
     /* 检查操作符是否为JSONB提取操作符 */
     if (oe->opno != pslinfo->jsonb_op_oid)
         return false;
     /* 检查参数数量是否为2 */
     if (list_length(oe->args) != 2)
         return false;
 
     /* 获取两个参数 */
     arg1 = (Node *)linitial(oe->args);
     arg2 = (Node *)lsecond(oe->args);
     
     /* 检查参数类型: 第一个必须是Var, 第二个必须是Const */
     if (!IsA(arg1, Var) || !IsA(arg2, Const))
         return false;
     
     /* 最终确认是否为无模式变量 */
     if (!tdengine_is_slvar(((Var *)arg1)->vartype, ((Var *)arg1)->varattno, pslinfo, NULL, NULL))
         return false;
     
     return true;
 }
 
/*
 * 检查节点是否为无模式(schemaless)类型参数的提取操作
 * 参数:
 *   @node: 要检查的表达式节点
 *   @pslinfo: 无模式信息结构体指针
 * 功能说明:
 *   1. 检查是否启用无模式
 *   2. 验证节点是否为操作表达式(OpExpr)
 *   3. 检查操作符是否为JSONB提取操作符(->>)
 *   4. 验证参数数量是否为2
 *   5. 检查参数类型: 第一个必须是Param, 第二个必须是Const
 * 注意事项:
 *   - 该函数用于识别预处理语句中的参数提取操作
 *   - 与tdengine_is_slvar_fetch类似但针对参数而非变量
 */
bool
tdengine_is_param_fetch(Node *node, schemaless_info *pslinfo)
{
    OpExpr *oe = (OpExpr *)node;
    Node *arg1;
    Node *arg2;

    /* 检查无模式是否启用 */
    if (!pslinfo->schemaless)
        return false;

    /* 检查是否为操作表达式 */
    if (!IsA(node, OpExpr))
        return false;
    /* 检查操作符是否为JSONB提取操作符 */
    if (oe->opno != pslinfo->jsonb_op_oid)
        return false;
    /* 检查参数数量是否为2 */
    if (list_length(oe->args) != 2)
        return false;

    /* 获取两个参数 */
    arg1 = (Node *)linitial(oe->args);
    arg2 = (Node *)lsecond(oe->args);
    
    /* 检查参数类型: 第一个必须是Param, 第二个必须是Const */
    if (!IsA(arg1, Param) || !IsA(arg2, Const))
        return false;
    
    return true;
}
 
/*
 * 从表达式节点中提取无模式(schemaless)变量的远程列名
 * 参数:
 *   @node: 要处理的表达式节点
 *   @pslinfo: 无模式信息结构体指针
 * 功能说明:
 *   1. 检查是否启用无模式
 *   2. 验证节点是否为无模式变量提取操作(->>)
 *   3. 处理CoerceViaIO类型转换节点
 *   4. 从操作表达式中提取常量参数
 *   5. 将常量值转换为字符串形式返回
 * 注意事项:
 *   - 返回的字符串内存由调用者负责释放
 *   - 仅支持JSONB类型的无模式变量提取
 */
char *
tdengine_get_slvar(Expr *node, schemaless_info *pslinfo)
{
    /* 检查无模式是否启用 */
    if (!pslinfo->schemaless)
        return NULL;

    /* 检查是否为无模式变量提取操作 */
    if (tdengine_is_slvar_fetch((Node *)node, pslinfo))
    {
        OpExpr *oe;
        Const *cnst;

        /* 处理类型转换节点 */
        if (IsA(node, CoerceViaIO))
            node = (Expr *) (((CoerceViaIO *)node)->arg);

        /* 获取操作表达式和常量参数 */
        oe = (OpExpr *)node;
        cnst = lsecond_node(Const, oe->args);

        /* 将常量值转换为字符串并返回 */
        return TextDatumGetCString(cnst->constvalue);
    }

    return NULL;
}

 
 /*
  * 获取并初始化无模式(schemaless)处理所需的信息
  *
  * 参数说明:
  * @pslinfo 无模式信息结构体指针(输出参数)
  * @schemaless 是否启用无模式
  * @reloid 外部表OID
  *
  * 功能说明:
  * 1. 设置无模式标志位
  * 2. 如果启用无模式:
  *    a. 缓存JSONB类型OID(如果未设置)
  *    b. 查找并缓存JSONB提取操作符->>的OID(如果未设置)
  *    c. 验证外部表格式是否符合无模式要求
  *    d. 保存外部表OID
  */
 void tdengine_get_schemaless_info(schemaless_info *pslinfo, bool schemaless, Oid reloid)
 {
     /* 设置无模式标志 */
     pslinfo->schemaless = schemaless;
     
     /* 如果启用无模式 */
     if (schemaless)
     {
         /* 缓存JSONB类型OID(如果尚未设置) */
         if (pslinfo->slcol_type_oid == InvalidOid)
             pslinfo->slcol_type_oid = JSONBOID;
             
         /* 查找并缓存JSONB提取操作符->>的OID(如果尚未设置) */
         if (pslinfo->jsonb_op_oid == InvalidOid)
             pslinfo->jsonb_op_oid = LookupOperName(NULL, list_make1(makeString("->>")),
                                                       pslinfo->slcol_type_oid, TEXTOID, true, -1);
 
         /* 验证外部表格式是否符合无模式要求 */
         tdengine_validate_foreign_table_sc(reloid);
 
         /* 保存外部表OID供后续使用 */
         pslinfo->relid = reloid;
     }
 }

 /*
  * 递归遍历表达式树提取无模式(schemaless)变量
  *
  * 参数说明:
  * @node 当前处理的表达式节点
  * @context 包含提取上下文信息的结构体
  *
  * 返回值:
  * true - 继续遍历子节点
  * false - 停止遍历
  *
  * 功能说明:
  * 1. 检查节点是否为无模式变量提取操作(->>)
  * 2. 处理CoerceViaIO类型转换节点
  * 3. 根据extract_raw标志决定提取原始表达式还是列名
  * 4. 检查并避免重复提取相同位置的表达式
  * 5. 递归遍历子节点
  *
  * 处理流程:
  * 1. 空节点直接返回
  * 2. 检查是否为无模式变量提取
  * 3. 处理原始表达式或列名提取
  * 4. 递归处理子节点
  */
 static bool
 tdengine_slvars_walker(Node *node, pull_slvars_context *context)
 {
     /* 空节点直接返回 */
     if (node == NULL)
         return false;
 
     /* 检查是否为无模式变量提取操作 */
     if (tdengine_is_slvar_fetch(node, context->pslinfo))
     {
         /* 处理类型转换节点 */
         if (IsA(node, CoerceViaIO))
             node = (Node *)(((CoerceViaIO *)node)->arg);
 
         /* 提取原始表达式模式 */
         if (context->extract_raw)
         {
             /* 检查是否已存在相同表达式 */
             ListCell *temp;
             foreach (temp, context->columns)
             {
                 if (equal(lfirst(temp), node))
                 {
                     OpExpr *oe1 = (OpExpr *)lfirst(temp);
                     OpExpr *oe2 = (OpExpr *)node;
                     if (oe1->location == oe2->location)
                         return false;
                 }
             }
             /* 检查远程表达式中是否已存在 */
             foreach (temp, context->remote_exprs)
             {
                 if (equal(lfirst(temp), node))
                 {
                     OpExpr *oe1 = (OpExpr *)lfirst(temp);
                     OpExpr *oe2 = (OpExpr *)node;
                     if (oe1->location == oe2->location)
                         return false;
                 }
             }
             /* 添加新表达式 */
             context->columns = lappend(context->columns, node);
         }
         /* 提取列名模式 */ 
         else
         {
             /* 解析操作表达式获取变量和常量 */
             OpExpr *oe = (OpExpr *)node;
             Var *var = linitial_node(Var, oe->args);
             Const *cnst = lsecond_node(Const, oe->args);
 
             /* 检查变量是否属于目标关系 */
             if (var->varno == context->varno && var->varlevelsup == 0)
             {
                 /* 获取列名字符串 */
                 char *const_str = TextDatumGetCString(cnst->constvalue);
 
                 /* 检查列名是否已存在 */
                 ListCell *temp;
                 foreach (temp, context->columns)
                 {
                     char *colname = strVal(lfirst(temp));
                     Assert(colname != NULL);
 
                     if (strcmp(colname, const_str) == 0)
                     {
                         return false;
                     }
                 }
                 /* 添加新列名 */
                 context->columns = lappend(context->columns, makeString(const_str));
             }
         }
     }
 
     /* 确保不会遇到未计划的子查询 */
     Assert(!IsA(node, Query));
 
     /* 递归遍历子节点 */
     return expression_tree_walker(node, tdengine_slvars_walker,
                                   (void *) context);
 }
 
 /*
  * 提取无模式(schemaless)变量中的远程列名
  *
  * 参数说明:
  * @expr 要分析的表达式树
  * @varno 变量编号
  * @columns 初始列名列表
  * @extract_raw 是否提取原始表达式
  * @remote_exprs 远程表达式列表
  * @pslinfo 无模式信息结构体指针
  *
  * 返回值:
  * 包含所有提取列名的List结构
  *
  * 功能说明:
  * 1. 初始化上下文结构体
  * 2. 设置上下文参数
  * 3. 调用walker函数遍历表达式树
  * 4. 返回收集到的列名列表
  *
  * 注意事项:
  * 1. 该函数主要用于处理JSONB类型的无模式列
  * 2. 返回的列名列表可能包含重复项
  */
 List *
 tdengine_pull_slvars(Expr *expr, Index varno, List *columns, bool extract_raw, List *remote_exprs, schemaless_info *pslinfo)
 {
     pull_slvars_context context;
 
     /* 初始化上下文结构体 */
     memset(&context, 0, sizeof(pull_slvars_context));
 
     /* 设置上下文参数 */
     context.varno = varno;
     context.columns = columns;
     context.pslinfo = pslinfo;
     context.extract_raw = extract_raw;
     context.remote_exprs = remote_exprs;
 
     /* 遍历表达式树提取列名 */
     (void) tdengine_slvars_walker((Node *)expr, &context);
 
     /* 返回收集到的列名列表 */
     return context.columns;
 }
 
/*
 * tdengine_is_att_dropped: 检查表属性是否已被删除
 *
 * 参数:
 *   @relid: 表的关系OID
 *   @attnum: 属性编号
 *
 * 返回值:
 *   true - 属性已被删除
 *   false - 属性未被删除
 */
static bool
tdengine_is_att_dropped(Oid relid, AttrNumber attnum)
{
    HeapTuple   tp;

    /* 在系统缓存中查找属性信息 */
    tp = SearchSysCache2(ATTNUM,
                         ObjectIdGetDatum(relid), Int16GetDatum(attnum));
    if (HeapTupleIsValid(tp))
    {
        /* 获取属性元组结构 */
        Form_pg_attribute att_tup = (Form_pg_attribute) GETSTRUCT(tp);

        /* 检查attisdropped标志 */
        bool result = att_tup->attisdropped;

        /* 释放系统缓存 */
        ReleaseSysCache(tp);
        return result;
    }

    /* 未找到属性信息，返回false */
    return false;
}
 
 /*
  * 验证无模式(schemaless)下外部表的格式
  *
  * 参数:
  * @reloid 外部表的OID
  *
  * 功能说明:
  * 1. 验证表结构是否符合无模式要求
  * 2. 检查各列的数据类型和选项
  * 3. 处理可能存在的已删除列
  *
  * 表结构要求:
  * 1. 必须包含time列(时间戳类型)
  * 2. 可选包含time_text列(文本类型)
  * 3. 必须包含tags或fields列(JSONB类型)
  * 4. 其他列必须通过选项明确指定用途
  */
 static void
 tdengine_validate_foreign_table_sc(Oid reloid)
 {
     int attnum = 1;  /* 列索引从1开始 */
 
     /* 遍历所有列进行验证 */
     while (true)
     {
         /* 获取列名和类型 */
         char *attname = get_attname(reloid, attnum, true);
         Oid atttype = get_atttype(reloid, attnum);
         bool att_is_dropped = tdengine_is_att_dropped(reloid, attnum);
 
         /* 跳过已删除的列 */
         if (att_is_dropped)
         {
             attnum++;
             continue;
         }
 
         /* 没有更多列需要检查时退出循环 */
         if (attname == NULL || atttype == InvalidOid)
             break;
 
         /* 验证time列 */
         if (strcmp(attname, "time") == 0)
         {
             /* 检查数据类型是否为时间戳 */
             if (atttype != TIMESTAMPOID &&
                 atttype != TIMESTAMPTZOID)
             {
                 elog(ERROR, "tdengine fdw: invalid data type for time column");
             }
         }
         /* 验证time_text列 */
         else if (strcmp(attname, "time_text") == 0)
         {
             /* 检查数据类型是否为文本 */
             if (atttype != TEXTOID)
             {
                 elog(ERROR, "tdengine fdw: invalid data type for time_text column");
             }
         }
         /* 验证tags/fields列 */
         else if (strcmp(attname, "tags") == 0 || strcmp(attname, "fields") == 0)
         {
             List *options = NIL;
 
             /* 检查数据类型是否为JSONB */
             if (atttype != JSONBOID)
                 elog(ERROR, "tdengine fdw: invalid data type for tags/fields column");
 
             /* 检查列选项 */
             options = GetForeignColumnOptions(reloid, attnum);
             if (options != NIL)
             {
                 DefElem *def = (DefElem *)linitial(options);
 
                 /* 验证选项值 */
                 if (defGetBoolean(def) != true)
                     elog(ERROR, "tdengine fdw: invalid option value for tags/fields column");
             }
         }
         /* 验证其他列 */
         else
         {
             /* 处理时间相关列 */
             if (atttype == TIMESTAMPOID ||
                 atttype == TIMESTAMPTZOID ||
                 atttype == TEXTOID)
             {
                 /* 必须通过选项明确指定为time列 */
                 List *options = GetForeignColumnOptions(reloid, attnum);
                 if (options != NIL)
                 {
                     DefElem *def = (DefElem *)linitial(options);
                     if (strcmp(defGetString(def), "time") != 0)
                         elog(ERROR, "tdengine fdw: invalid option value for time/time_text column");
                 }
                 else
                     elog(ERROR, "tdengine fdw: invalid column name of time/time_text in schemaless mode");
             }
             /* 处理JSONB类型列 */
             else if (atttype == JSONBOID)
             {
                 /* 必须通过选项明确指定为tags/fields列 */
                 List *options = GetForeignColumnOptions(reloid, attnum);
                 if (options != NIL)
                 {
                     DefElem *def = (DefElem *)linitial(options);
                     if (defGetBoolean(def) != true)
                         elog(ERROR, "tdengine fdw: invalid option value for tags/fields column");
                 }
                 else
                     elog(ERROR, "tdengine fdw: invalid column name of tags/fields in schemaless mode");
             }
             /* 其他类型列不允许 */
             else
                 elog(ERROR, "tdengine fdw: invalid column in schemaless mode. Only time, time_text, tags and fields columns are accepted.");
         }
 
         attnum++;
     }
 }
 