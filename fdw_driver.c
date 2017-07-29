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

static shm_mq_handle*
perform_query(HBaseFdwTableInfo *info);

static HBaseColumn *
find_hbase_columns(Relation rel);

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
is_hbase_expr(Node *node, RelOptInfo *foreign_rel)
{
	switch (nodeTag(node))
	{
		case T_Var:
			{
				Var *var = (Var*)node;
				if (bms_is_member(var->varno, foreign_rel->relids) &&
					var->varlevelsup == 0)
				{
					if (var->varattno < 0)
						return false;
					return true;
				}
				else
					return false;
			}
		case T_Const:
			return true;
		case T_Param:
			return true;
		case T_OpExpr:
		{
			OpExpr *oe = (OpExpr *) node;
			HeapTuple  opertup;
			Form_pg_operator operform;

			elog(LOG, "Looking for operator: %u",  oe->opno);
			opertup = SearchSysCache1(OPEROID,
									  ObjectIdGetDatum(oe->opno));

			if (!HeapTupleIsValid(opertup))
				elog(ERROR, "cache lookup failed for operator %u",
					 oe->opno);
			operform = (Form_pg_operator) GETSTRUCT(opertup);
			elog(LOG, "Found operator: %s", operform->oprname.data);
			ReleaseSysCache(opertup);
			return true;
		}
		default:
			return false;
	}
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
			table_info->remote_conds = lappend(table_info->remote_conds, ri);
		else
			table_info->local_conds = lappend(table_info->local_conds, ri);
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

static ForeignScan *hbaseGetForeignPlan(PlannerInfo *root,
										RelOptInfo *baserel,
										Oid foreigntableid,
										ForeignPath *best_path,
										List *tlist,
										List *scan_clauses,
										Plan *outer_plan)
{
	List *local_conds = NIL;
	List *local_exprs = NIL;
	List *remote_conds = NIL;
	List *remote_exprs = NIL;
	HBaseFdwTableInfo *table_info = baserel->fdw_private;

	ListCell *lc;
	foreach(lc, scan_clauses)
	{
		RestrictInfo *rinfo = (RestrictInfo*) lfirst(lc);

		if (rinfo->pseudoconstant)
			continue;

		if (list_member_ptr(table_info->remote_conds, rinfo))
		{
			remote_conds = lappend(remote_conds, rinfo);
			remote_exprs = lappend(remote_exprs, rinfo->clause);
		}
		else if (list_member_ptr(table_info->local_conds, rinfo))
			local_exprs = lappend(local_exprs, rinfo->clause);
	}
	return make_foreignscan(tlist,
							local_exprs,
							baserel->relid,
							NIL,
							NIL,
							NIL,
							remote_exprs,
							outer_plan);
}

static void
hbaseBeginForeignScan(ForeignScanState *node, int eflags)
{
	HBaseFdwPrivateScanState *pss;
	shm_mq *mq;

	if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
		return;

	pss = palloc0(sizeof(*pss));
	pss->table_info = get_table_info(RelationGetRelid(node->ss.ss_currentRelation));

	pss->seg = dsm_create(DSM_SIZE, 0);
	mq = shm_mq_create(dsm_segment_address(pss->seg), DSM_SIZE);
	shm_mq_set_receiver(mq, MyProc);
	pss->mq_handle = shm_mq_attach(mq, pss->seg, NULL);
	node->fdw_state = pss;

	activate_worker(
		pss->table_info->table_name,
		pss->table_info->columns,
		pss->table_info->num_columns,
		dsm_segment_handle(pss->seg)
	);
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

	TupleDesc desc = RelationGetDescr(node->ss.ss_currentRelation);
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
