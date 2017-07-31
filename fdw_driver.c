#include "postgres.h"

#include "hbase_fdw.h"

#include "optimizer/pathnode.h"
#include "optimizer/paths.h"
#include "optimizer/restrictinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "foreign/fdwapi.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "optimizer/planmain.h"
#include "foreign/foreign.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_class.h"
#include "utils/syscache.h"
#include "access/htup_details.h"
#include "utils/rel.h"
#include "utils/lsyscache.h"
#include "commands/defrem.h"
#include "utils/jsonb.h"

typedef struct HBaseFdwTableInfo {
	char *table_name;
	int num_columns;
	HBaseColumn *columns;

	List *remote_conds;
	List *local_conds;
} HBaseFdwTableInfo;

typedef struct HBaseFdwPrivateScanState {
	HBaseFdwTableInfo *table_info;
	List *filters;
	bool worker_started;

	FmgrInfo *param_flinfo;
	List *param_exprs;

	shm_mq_handle *mq_handle;
	dsm_segment *seg;
} HBaseFdwPrivateScanState;

static void
hbaseGetForeignRelSize(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Oid foreigntableid);
static void
hbaseGetForeignPaths(PlannerInfo *root,
					 RelOptInfo *baserel,
					 Oid foreigntableid);
static ForeignScan *
hbaseGetForeignPlan(PlannerInfo *root,
					RelOptInfo *baserel,
					Oid foreigntableid,
					ForeignPath *best_path,
					List *tlist,
					List *scan_clauses,
					Plan *outer_plan);
static void
hbaseBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *
hbaseIterateForeignScan(ForeignScanState *node);
static void
hbaseReScanForeignScan(ForeignScanState *node);
static void
hbaseEndForeignScan(ForeignScanState *node);

static HBaseColumn *
find_hbase_columns(Relation rel);

static bool
is_row_key_var(Node *node, HBaseFdwTableInfo *table_info, Bitmapset* relids);

static bool
is_row_key_equals(Node *node, HBaseFdwTableInfo *table_info, Bitmapset *relids);

void
setup_shared_memory(HBaseFdwPrivateScanState *pss);

static HBasePreparedFilter*
make_filter(Node *expr, HBaseFdwTableInfo *table_info, Bitmapset *relids);

static void
prepare_query_params(ForeignScanState *node);

static List*
create_finalized_filters(ForeignScanState *node);

/*
 * SQL functions
 */
PG_FUNCTION_INFO_V1(hbase_fdw_handler);

Datum
hbase_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *routine = makeNode(FdwRoutine);

	/* Functions for scanning foreign tables */
	routine->GetForeignRelSize = hbaseGetForeignRelSize;
	routine->GetForeignPaths = hbaseGetForeignPaths;
	routine->GetForeignPlan = hbaseGetForeignPlan;
	routine->BeginForeignScan = hbaseBeginForeignScan;
	routine->IterateForeignScan = hbaseIterateForeignScan;
	routine->ReScanForeignScan = hbaseReScanForeignScan;
	routine->EndForeignScan = hbaseEndForeignScan;

	PG_RETURN_POINTER(routine);
}

static bool
is_row_key_var(Node *node, HBaseFdwTableInfo *table_info, Bitmapset* relids)
{
	Var *var;

	if (nodeTag(node) != T_Var)
		return false;

	var = (Var*)node;

	// Check that we're not grabbing an outer variable
	// from a subquery.
	if (var->varlevelsup > 0)
		return false;

	// System columns are not part of what can be queried on.
	if (var->varattno <= 0)
		return false;

	if (!bms_is_member(var->varno, relids))
		return false;

	if (var->varattno > table_info->num_columns)
		return false;

	return table_info->columns[var->varattno - 1].row_key;
}

static bool
is_row_key_equals(Node *node, HBaseFdwTableInfo *table_info, Bitmapset *relids)
{
	OpExpr *oe;
	Node *left = NULL;
	Node *right = NULL;
	Node *var = NULL;
	Node *expr = NULL;

	if (nodeTag(node) != T_OpExpr)
		return false;

	oe = (OpExpr *) node;

	if (oe->opno != TextEqualOperator)
		return false;

	if (list_length(oe->args) != 2)
		return false;

	left = linitial(oe->args);
	right = lsecond(oe->args);

	if (is_row_key_var(left, table_info, relids))
	{
		var = left;
		expr = right;
	}
	else if (is_row_key_var(right, table_info, relids))
	{
		var = right;
		expr = left;
	}
	else
	{
		return false;
	}

	return (nodeTag(expr) == T_Param ||
			nodeTag(expr) == T_Const);
}

static bool
is_hbase_expr(Node *node, RelOptInfo *foreign_rel)
{
	HBaseFdwTableInfo *table_info = foreign_rel->fdw_private;
	Bitmapset *relids = foreign_rel->relids;

	if (is_row_key_equals(node, table_info, relids))
		return true;
	return false;
}

static char *get_table_name(ForeignTable *table)
{
	ListCell *lc;
	foreach (lc, table->options)
	{
		DefElem *elem = lfirst(lc);
		if (strcmp(elem->defname, "hbase_table") == 0)
		{
			return defGetString(elem);
		}
	}
	return NULL;
}

static HBaseColumn *
find_hbase_columns(Relation rel)
{
	int num_cols = RelationGetNumberOfAttributes(rel);
	HBaseColumn *cols = palloc0(sizeof(HBaseColumn) * num_cols);

	for (AttrNumber attnum = 0; attnum < num_cols; attnum++)
	{
		HBaseColumn *col = cols + attnum;
		List *col_opts = GetForeignColumnOptions(rel->rd_id, attnum + 1);
		ListCell *lc;
		foreach (lc, col_opts)
		{
			DefElem *elem = lfirst(lc);

			if (strcmp(elem->defname, "hbase_type") == 0)
			{
				char *type = defGetString(elem);
				if (strcmp(type, "row_key") == 0 ||
					strcmp(type, "rowkey") == 0 ||
					strcmp(type, "row") == 0)
					col->row_key = true;

				else if (strcmp(type, "family") == 0 ||
						strcmp(type, "column_family") == 0)
					col->family = true;
				else if (strcmp(type, "column") == 0)
					col->column = true;
				else
					elog(ERROR, "Unknown hbase_type: %s", type);
			}
			else if (strcmp(elem->defname, "family") == 0)
			{
				char *family = defGetString(elem);
				strncpy(col->family_name, family, HBASE_FDW_MAX_FAMILY_LEN);
				col->family_name[HBASE_FDW_MAX_FAMILY_LEN] = '\0';
			}
			else if (strcmp(elem->defname, "column") == 0 ||
					 strcmp(elem->defname, "qualifier") == 0)
			{
				char *qualifier = defGetString(elem);
				strncpy(col->qualifier, qualifier, HBASE_FDW_MAX_QUALIFIER_LEN);
				col->qualifier[HBASE_FDW_MAX_QUALIFIER_LEN] = '\0';
			}
			else
				elog(ERROR, "Unknown column option: %s", elem->defname);
		}

		if (col->row_key &&
			(col->family || col->column ||
			 col->qualifier[0] != '\0' || col->family_name[0] != '\0'))
			elog(ERROR, "Type row key, can not have family, column or other hbase_type");
		if (col->family &&
			(col->column || col->qualifier[0] != '\0'))
			elog(ERROR, "Type family can not have column or other hbase_type");
	}
	return cols;
}

static HBaseFdwTableInfo *
get_table_info(Oid foreigntableid)
{
	ForeignTable *foreign_table = GetForeignTable(foreigntableid);
	HBaseFdwTableInfo *table_info = palloc0(sizeof(HBaseFdwTableInfo));
	char *table_name;
	Relation rel = RelationIdGetRelation(foreigntableid);
	HBaseColumn *cols;
	int num_cols = RelationGetNumberOfAttributes(rel);

	table_name = get_table_name(foreign_table);
	if (table_name == NULL)
		table_name = RelationGetRelationName(rel);

	cols = find_hbase_columns(rel);

	table_info->table_name = table_name;
	table_info->num_columns = num_cols;
	table_info->columns = cols;
	table_info->remote_conds = NIL;
	table_info->local_conds = NIL;
	RelationClose(rel);

	return table_info;
}

#define DSM_SIZE 1048576

static void
hbaseGetForeignRelSize(PlannerInfo *root,
					   RelOptInfo *baserel,
					   Oid foreigntableid)
{
	List *remote_conds = NIL;
	List *local_conds = NIL;
	ListCell *lc;
	ForeignTable *foreign_table;
	HBaseFdwTableInfo *table_info = get_table_info(foreigntableid);

	baserel->fdw_private = table_info;
	foreign_table = GetForeignTable(foreigntableid);

	foreach (lc, baserel->baserestrictinfo)
	{
		RestrictInfo *ri = (RestrictInfo *) lfirst(lc);
		if (is_hbase_expr((Node*)ri->clause, baserel))
		{
			elog(LOG, "Was hbase expr");
			table_info->remote_conds = lappend(table_info->remote_conds, ri);
		}
		else
		{
			elog(LOG, "Was not hbase expr");
			table_info->local_conds = lappend(table_info->local_conds, ri);
		}
	}

	baserel->rows = 5.0;
}

static void
hbaseGetForeignPaths(PlannerInfo *root,
					 RelOptInfo *baserel,
					 Oid foreigntableid)
{
	ForeignPath *path;

	path = create_foreignscan_path(
		root,
		baserel,
		NULL,
		baserel->rows,
		1.0,
		100.0,
		NIL,
		NULL,
		NULL,
		NIL);
	add_path(baserel, (Path*) path);

}

static HBasePreparedFilter*
create_row_key_equals_filter(Node *node, HBaseFdwTableInfo *table_info, Bitmapset *relids)
{
	OpExpr *op = (OpExpr*)node;
	HBasePreparedFilter *filter = palloc0(sizeof(HBasePreparedFilter));
	Node *left = linitial(op->args);
	Node *right = lsecond(op->args);
	Node *expr = is_row_key_var(left, table_info, relids) ? right : left;

	filter->filter.filter_type = filter_type_row_key_equals;
	filter->params = list_make1(expr);
	filter->param_nums = NULL;
	return filter;
}

static HBasePreparedFilter*
make_filter(Node *expr,
			HBaseFdwTableInfo *table_info,
			Bitmapset *relids)
{
	if (is_row_key_equals(expr, table_info, relids))
		return create_row_key_equals_filter(expr, table_info, relids);
	elog(ERROR, "Failed to handle expression");
}


static ForeignScan *hbaseGetForeignPlan(PlannerInfo *root,
										RelOptInfo *baserel,
										Oid foreigntableid,
										ForeignPath *best_path,
										List *tlist,
										List *scan_clauses,
										Plan *outer_plan)
{
	List *local_exprs = NIL;
	List *remote_exprs = NIL;
	List *params_list = NIL;
	HBaseFdwTableInfo *table_info = baserel->fdw_private;
	List *hbase_filters = NIL;
	List *params = NIL;
	ListCell *lc;
	foreach(lc, scan_clauses)
	{
		RestrictInfo *rinfo = (RestrictInfo*) lfirst(lc);

		if (rinfo->pseudoconstant)
			continue;

		if (list_member_ptr(table_info->remote_conds, rinfo))
			remote_exprs = lappend(remote_exprs, rinfo->clause);
		else if (list_member_ptr(table_info->local_conds, rinfo))
			local_exprs = lappend(local_exprs, rinfo->clause);
	}

	foreach (lc, remote_exprs)
	{
		Node *node = lfirst(lc);
		HBasePreparedFilter *filter = make_filter(node, table_info, baserel->relids);
		hbase_filters = lappend(hbase_filters, filter);
	}

	foreach (lc, hbase_filters)
	{
		HBasePreparedFilter *filter = lfirst(lc);
		ListCell *filter_param;
		int num = 0;

		filter->param_nums = palloc0(sizeof(int) * list_length(filter->params));
		foreach (filter_param, filter->params)
		{
			Node *filter_param_node = lfirst(filter_param);
			ListCell *global_param;
			int pindex = 0;

			foreach (global_param, params)
			{
				Node *global_param_node = lfirst(global_param);
				pindex++;
				if (equal(global_param_node, filter_param_node))
					break;
			}

			if (global_param == NULL)
			{
				pindex++;
				params = lappend(params, filter_param_node);
			}
			filter->param_nums[num] = pindex;
		}
	}

	return make_foreignscan(
		tlist,
		local_exprs,
		baserel->relid,
		params,
		list_make1(hbase_filters),
		NIL,
		remote_exprs,
		outer_plan);
}

void
setup_shared_memory(HBaseFdwPrivateScanState *pss)
{
	shm_toc *toc;
	shm_mq *mq;
	dsm_segment *seg;
	shm_toc_estimator e;
	Size dsm_size;
	HBaseCommand *command;
	HBaseColumn *columns;
	HBaseFilter *out_filters;
	Size mq_size;
	ListCell *lc;
	size_t offset;
	HBaseFdwTableInfo *table_info = pss->table_info;
	List *filters = pss->filters;
	uint32 nr_filters = list_length(filters);

	shm_toc_initialize_estimator(&e);
	shm_toc_estimate_keys(&e, 1);
	shm_toc_estimate_chunk(&e, sizeof(HBaseCommand));
	shm_toc_estimate_keys(&e, 1);
	shm_toc_estimate_chunk(&e, sizeof(HBaseColumn) * table_info->num_columns);
	shm_toc_estimate_keys(&e, 1);
	shm_toc_estimate_chunk(&e, sizeof(HBaseFilter) * nr_filters);
	shm_toc_estimate_keys(&e, 1);
	shm_toc_estimate_chunk(&e, DSM_SIZE);
	dsm_size = shm_toc_estimate(&e);

	seg = dsm_create(dsm_size, 0);
	toc = shm_toc_create(
		HBASE_FDW_SHM_TOC_MAGIC,
		dsm_segment_address(seg),
		dsm_size);

	command = shm_toc_allocate(toc, sizeof(HBaseCommand));
	strncpy(command->table_name, table_info->table_name, HBASE_FDW_MAX_TABLE_NAME_LEN);
	command->table_name[HBASE_FDW_MAX_TABLE_NAME_LEN] = '\0';
	command->nr_columns = table_info->num_columns;
	command->nr_filters = nr_filters;
	shm_toc_insert(toc, 1, command);

	columns = shm_toc_allocate(toc, sizeof(HBaseColumn) * table_info->num_columns);
	memcpy(columns, table_info->columns, sizeof(*columns) * table_info->num_columns);
	shm_toc_insert(toc, 2, columns);

	out_filters = shm_toc_allocate(toc, sizeof(HBaseFilter) * nr_filters);
	shm_toc_insert(toc, 3, out_filters);

	mq_size = DSM_SIZE;
	mq = shm_toc_allocate(toc, mq_size);
	mq = shm_mq_create(mq, mq_size);
	shm_mq_set_receiver(mq, MyProc);
	shm_toc_insert(toc, 4, mq);

	pss->mq_handle = shm_mq_attach(mq, pss->seg, NULL);
	pss->seg = seg;
}

static void
prepare_query_params(ForeignScanState *node)
{
	HBaseFdwPrivateScanState *pss = node->fdw_state;
	ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
	List *exprs = fsplan->fdw_exprs;
	ListCell *lc;
	int num_params = list_length(exprs);
	int i;
	if (num_params == 0)
	{
		pss->param_flinfo = NULL;
		pss->param_exprs = NIL;
	}

	pss->param_flinfo = palloc0(sizeof(FmgrInfo) * num_params);
	pss->param_exprs  = (List*)ExecInitExpr((Expr*) exprs, &node->ss.ps);

	i = 0;
	foreach (lc, exprs)
	{
		Node *param_expr = (Node*) lfirst(lc);
		Oid typefnoid;
		bool isvarlena;

		getTypeOutputInfo(exprType(param_expr), &typefnoid, &isvarlena);
		fmgr_info(typefnoid, &pss->param_flinfo[i]);
		i++;
	}
}

static List*
create_finalized_filters(ForeignScanState *node)
{
	HBaseFdwPrivateScanState *pss = node->fdw_state;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	ListCell *lc;
	int i = 0;
	MemoryContext oldcontext;
	List *ret = NIL;
	char **param_values = NULL;
	int len = list_length(pss->param_exprs);

	elog(LOG, "RUNNING HERE");

	if (len == 0)
		return NIL;

	param_values = palloc0(len * sizeof(char*));

	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	foreach(lc, pss->param_exprs)
	{
		ExprState  *expr_state = (ExprState *) lfirst(lc);
		Datum		expr_value;
		bool		isNull;

		/* Evaluate the parameter expression */
		expr_value = ExecEvalExpr(expr_state, econtext, &isNull, NULL);

		/*
		 * Get string representation of each parameter value by invoking
		 * type-specific output function, unless the value is null.
		 */
		if (isNull)
			param_values[i] = NULL;
		else
			param_values[i] = OutputFunctionCall(&pss->param_flinfo[i], expr_value);

		i++;
	}
	MemoryContextSwitchTo(oldcontext);

	foreach (lc, pss->filters)
	{
		HBasePreparedFilter *prepared_filter = (HBasePreparedFilter *)lfirst(lc);
		HBaseFilter *filter = &prepared_filter->filter;

		switch(filter->filter_type)
		{
			case filter_type_row_key_equals:
			{
				if (prepared_filter->param_nums != NULL)
				{
					elog(LOG, "Params num: %d", prepared_filter->param_nums[0]);
					strncpy(filter->row_key_equals.row_key,
							param_values[prepared_filter->param_nums[0] - 1],
							HBASE_FDW_MAX_ROW_KEY_FILTER_LEN);
					filter->row_key_equals.row_key[HBASE_FDW_MAX_ROW_KEY_FILTER_LEN] = '\0';
				}
				break;
			}
			default:
				elog(ERROR, "Unknown filter type: %d", filter->filter_type);
		}
		ret = lappend(ret, filter);
	}

	pfree(param_values);
	return ret;
}

static void
start_external_worker(ForeignScanState *node)
{

	HBaseFdwPrivateScanState *pss = node->fdw_state;
	HBaseFdwTableInfo *table_info = pss->table_info;

	List *filters = create_finalized_filters(node);
	ListCell *lc;
	size_t offset = 0;
	shm_toc *toc;
	HBaseFilter *output_filters;
	toc = shm_toc_attach(HBASE_FDW_SHM_TOC_MAGIC, dsm_segment_address(pss->seg));

	if (toc == NULL)
		elog(ERROR, "Failed to reach TOC");

	output_filters = shm_toc_lookup(toc, 3);

	foreach (lc, filters)
	{
		HBaseFilter *filter = lfirst(lc);
		memcpy(output_filters + offset, filter, sizeof(HBaseFilter));
		offset += sizeof(HBaseFilter);
	}

	activate_worker(dsm_segment_handle(pss->seg));
	pss->worker_started = true;
}

static void
hbaseBeginForeignScan(ForeignScanState *node, int eflags)
{
	HBaseFdwPrivateScanState *pss;
	HBaseFdwTableInfo *table_info;
	Oid rel_id;
	ForeignScan *fsplan;
	List *filters;

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	fsplan = (ForeignScan*)node->ss.ps.plan;
	rel_id = RelationGetRelid(node->ss.ss_currentRelation);

	pss = node->fdw_state = palloc0(sizeof(*pss));
	pss->table_info = get_table_info(rel_id);
	pss->filters = linitial(fsplan->fdw_private);
	pss->mq_handle = NULL;
	pss->seg = NULL;
	pss->worker_started = false;
	pss->param_exprs = NIL;
	pss->param_flinfo = NULL;

	prepare_query_params(node);
	setup_shared_memory(pss);
}

static HeapTuple
handle_tuple(char *tuple_data, HBaseFdwTableInfo *table_info, TupleDesc desc)
{
	Datum *values = palloc0(sizeof(*values) * desc->natts);
	bool *nulls = palloc0(sizeof(bool) * desc->natts);
	size_t cur_tuple_offset = 0;
	size_t datum_len = *(int*)(tuple_data + cur_tuple_offset);
	HeapTuple tuple;
	int i = 0;

	memset(nulls, true, desc->natts);

	while (datum_len != 0) {
		if (datum_len == 4)
		{
			nulls[i] = true;
			values[i] = PointerGetDatum(NULL);
		}
		else
		{
			nulls[i] = false;
			values[i] = PointerGetDatum(tuple_data + cur_tuple_offset + 4);
		}
		i++;
		cur_tuple_offset += datum_len;
		datum_len = *(int*)(tuple_data + cur_tuple_offset);
	};

	tuple = heap_form_tuple(desc, values, nulls);

	HeapTupleHeaderSetXmax(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetXmin(tuple->t_data, InvalidTransactionId);
	HeapTupleHeaderSetCmin(tuple->t_data, InvalidTransactionId);

	return tuple;
}

static TupleTableSlot *
hbaseIterateForeignScan(ForeignScanState *node)
{
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	HBaseFdwPrivateScanState *pss = node->fdw_state;
	Size len;
	HBaseFdwMessage *message;
	shm_mq_result res;
	TupleDesc desc;

	if (!pss->worker_started)
		start_external_worker(node);

	desc = RelationGetDescr(node->ss.ss_currentRelation);
	res = shm_mq_receive(pss->mq_handle, &len, (void**)&message, false);
	if (res == SHM_MQ_DETACHED)
	{
		elog(ERROR, "Subprocess lost connection");
	}

	switch (message->msg_type)
	{
		case msg_type_end_of_stream:
			return ExecClearTuple(slot);
		case msg_type_tuple:
			{
				HeapTuple tuple = handle_tuple(message->data, pss->table_info, desc);
				ExecStoreTuple(tuple, slot, InvalidBuffer, false);
				return slot;
			}
		default:
			elog(ERROR, "Unknown message");
	}
}

static void
hbaseReScanForeignScan(ForeignScanState *node)
{
}

static void
hbaseEndForeignScan(ForeignScanState *node)
{
	HBaseFdwPrivateScanState *pss = node->fdw_state;
	/* if fsstate is NULL, we are in EXPLAIN; nothing to do */
	if (pss == NULL)
		return;

	dsm_detach(pss->seg);
}
