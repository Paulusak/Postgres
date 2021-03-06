/*-------------------------------------------------------------------------
 *
 * windowfuncs.c
 *	  Standard window functions defined in SQL spec.
 *
 * Portions Copyright (c) 2000-2020, PostgreSQL Global Development Group
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/windowfuncs.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "utils/builtins.h"
#include "windowapi.h"

/*
 * ranking process information
 */
typedef struct rank_context
{
	int64		rank;			/* current rank */
} rank_context;

/*
 * ntile process information
 */
typedef struct
{
	int32		ntile;			/* current result */
	int64		rows_per_bucket;	/* row number of current bucket */
	int64		boundary;		/* how many rows should be in the bucket */
	int64		remainder;		/* (total rows) % (bucket num) */
} ntile_context;

static bool rank_up(WindowObject winobj);
static Datum leadlag_common(FunctionCallInfo fcinfo,
							bool forward, bool withoffset, bool withdefault);


/*
 * utility routine for *_rank functions.
 */
static bool
rank_up(WindowObject winobj)
{
	bool		up = false;		/* should rank increase? */
	int64		curpos = WinGetCurrentPosition(winobj);
	rank_context *context;

	context = (rank_context *)
		WinGetPartitionLocalMemory(winobj, sizeof(rank_context));

	if (context->rank == 0)
	{
		/* first call: rank of first row is always 1 */
		Assert(curpos == 0);
		context->rank = 1;
	}
	else
	{
		Assert(curpos > 0);
		/* do current and prior tuples match by ORDER BY clause? */
		if (!WinRowsArePeers(winobj, curpos - 1, curpos))
			up = true;
	}

	/* We can advance the mark, but only *after* access to prior row */
	WinSetMarkPosition(winobj, curpos);

	return up;
}


/*
 * row_number
 * just increment up from 1 until current partition finishes.
 */
Datum
window_row_number(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	int64		curpos = WinGetCurrentPosition(winobj);

	WinSetMarkPosition(winobj, curpos);
	PG_RETURN_INT64(curpos + 1);
}

/*
 * rank
 * Rank changes when key columns change.
 * The new rank number is the current row number.
 */
Datum
window_rank(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	rank_context *context;
	bool		up;

	up = rank_up(winobj);
	context = (rank_context *)
		WinGetPartitionLocalMemory(winobj, sizeof(rank_context));
	if (up)
		context->rank = WinGetCurrentPosition(winobj) + 1;

	PG_RETURN_INT64(context->rank);
}

/*
 * dense_rank
 * Rank increases by 1 when key columns change.
 */
Datum
window_dense_rank(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	rank_context *context;
	bool		up;

	up = rank_up(winobj);
	context = (rank_context *)
		WinGetPartitionLocalMemory(winobj, sizeof(rank_context));
	if (up)
		context->rank++;

	PG_RETURN_INT64(context->rank);
}

/*
 * percent_rank
 * return fraction between 0 and 1 inclusive,
 * which is described as (RK - 1) / (NR - 1), where RK is the current row's
 * rank and NR is the total number of rows, per spec.
 */
Datum
window_percent_rank(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	rank_context *context;
	bool		up;
	int64		totalrows = WinGetPartitionRowCount(winobj);

	Assert(totalrows > 0);

	up = rank_up(winobj);
	context = (rank_context *)
		WinGetPartitionLocalMemory(winobj, sizeof(rank_context));
	if (up)
		context->rank = WinGetCurrentPosition(winobj) + 1;

	/* return zero if there's only one row, per spec */
	if (totalrows <= 1)
		PG_RETURN_FLOAT8(0.0);

	PG_RETURN_FLOAT8((float8) (context->rank - 1) / (float8) (totalrows - 1));
}

/*
 * cume_dist
 * return fraction between 0 and 1 inclusive,
 * which is described as NP / NR, where NP is the number of rows preceding or
 * peers to the current row, and NR is the total number of rows, per spec.
 */
Datum
window_cume_dist(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	rank_context *context;
	bool		up;
	int64		totalrows = WinGetPartitionRowCount(winobj);

	Assert(totalrows > 0);

	up = rank_up(winobj);
	context = (rank_context *)
		WinGetPartitionLocalMemory(winobj, sizeof(rank_context));
	if (up || context->rank == 1)
	{
		/*
		 * The current row is not peer to prior row or is just the first, so
		 * count up the number of rows that are peer to the current.
		 */
		int64		row;

		context->rank = WinGetCurrentPosition(winobj) + 1;

		/*
		 * start from current + 1
		 */
		for (row = context->rank; row < totalrows; row++)
		{
			if (!WinRowsArePeers(winobj, row - 1, row))
				break;
			context->rank++;
		}
	}

	PG_RETURN_FLOAT8((float8) context->rank / (float8) totalrows);
}

/*
 * ntile
 * compute an exact numeric value with scale 0 (zero),
 * ranging from 1 (one) to n, per spec.
 */
Datum
window_ntile(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	ntile_context *context;

	context = (ntile_context *)
		WinGetPartitionLocalMemory(winobj, sizeof(ntile_context));

	if (context->ntile == 0)
	{
		/* first call */
		int64		total;
		int32		nbuckets;
		bool		isnull;

		total = WinGetPartitionRowCount(winobj);
		nbuckets = DatumGetInt32(WinGetFuncArgCurrent(winobj, 0, &isnull));

		/*
		 * per spec: If NT is the null value, then the result is the null
		 * value.
		 */
		if (isnull)
			PG_RETURN_NULL();

		/*
		 * per spec: If NT is less than or equal to 0 (zero), then an
		 * exception condition is raised.
		 */
		if (nbuckets <= 0)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_ARGUMENT_FOR_NTILE),
					 errmsg("argument of ntile must be greater than zero")));

		context->ntile = 1;
		context->rows_per_bucket = 0;
		context->boundary = total / nbuckets;
		if (context->boundary <= 0)
			context->boundary = 1;
		else
		{
			/*
			 * If the total number is not divisible, add 1 row to leading
			 * buckets.
			 */
			context->remainder = total % nbuckets;
			if (context->remainder != 0)
				context->boundary++;
		}
	}

	context->rows_per_bucket++;
	if (context->boundary < context->rows_per_bucket)
	{
		/* ntile up */
		if (context->remainder != 0 && context->ntile == context->remainder)
		{
			context->remainder = 0;
			context->boundary -= 1;
		}
		context->ntile += 1;
		context->rows_per_bucket = 1;
	}

	PG_RETURN_INT32(context->ntile);
}

/*
 * leadlag_common
 * common operation of lead() and lag()
 * For lead() forward is true, whereas for lag() it is false.
 * withoffset indicates we have an offset second argument.
 * withdefault indicates we have a default third argument.
 */
static Datum
leadlag_common(FunctionCallInfo fcinfo,
			   bool forward, bool withoffset, bool withdefault)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	int32		offset;
	bool		const_offset;
	Datum		result;
	bool		isnull;
	bool		isout;

	if (withoffset)
	{
		offset = DatumGetInt32(WinGetFuncArgCurrent(winobj, 1, &isnull));
		if (isnull)
			PG_RETURN_NULL();
		const_offset = get_fn_expr_arg_stable(fcinfo->flinfo, 1);
	}
	else
	{
		offset = 1;
		const_offset = true;
	}

	result = WinGetFuncArgInPartition(winobj, 0,
									  (forward ? offset : -offset),
									  WINDOW_SEEK_CURRENT,
									  const_offset,
									  &isnull, &isout);

	if (isout)
	{
		/*
		 * target row is out of the partition; supply default value if
		 * provided.  otherwise it'll stay NULL
		 */
		if (withdefault)
			result = WinGetFuncArgCurrent(winobj, 2, &isnull);
	}

	if (isnull)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(result);
}

/*
 * lag
 * returns the value of VE evaluated on a row that is 1
 * row before the current row within a partition,
 * per spec.
 */
Datum
window_lag(PG_FUNCTION_ARGS)
{
	return leadlag_common(fcinfo, false, false, false);
}

/*
 * lag_with_offset
 * returns the value of VE evaluated on a row that is OFFSET
 * rows before the current row within a partition,
 * per spec.
 */
Datum
window_lag_with_offset(PG_FUNCTION_ARGS)
{
	return leadlag_common(fcinfo, false, true, false);
}

/*
 * lag_with_offset_and_default
 * same as lag_with_offset but accepts default value
 * as its third argument.
 */
Datum
window_lag_with_offset_and_default(PG_FUNCTION_ARGS)
{
	return leadlag_common(fcinfo, false, true, true);
}

/*
 * lead
 * returns the value of VE evaluated on a row that is 1
 * row after the current row within a partition,
 * per spec.
 */
Datum
window_lead(PG_FUNCTION_ARGS)
{
	return leadlag_common(fcinfo, true, false, false);
}

/*
 * lead_with_offset
 * returns the value of VE evaluated on a row that is OFFSET
 * number of rows after the current row within a partition,
 * per spec.
 */
Datum
window_lead_with_offset(PG_FUNCTION_ARGS)
{
	return leadlag_common(fcinfo, true, true, false);
}

/*
 * lead_with_offset_and_default
 * same as lead_with_offset but accepts default value
 * as its third argument.
 */
Datum
window_lead_with_offset_and_default(PG_FUNCTION_ARGS)
{
	return leadlag_common(fcinfo, true, true, true);
}

/*
 * first_value
 * return the value of VE evaluated on the first row of the
 * window frame, per spec.
 */
Datum
window_first_value(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	Datum		result;
	bool		isnull;

	result = WinGetFuncArgInFrame(winobj, 0,
								  0, WINDOW_SEEK_HEAD, true,
								  &isnull, NULL);
	if (isnull)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(result);
}

/*
 * last_value
 * return the value of VE evaluated on the last row of the
 * window frame, per spec.
 */
Datum
window_last_value(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	Datum		result;
	bool		isnull;

	result = WinGetFuncArgInFrame(winobj, 0,
								  0, WINDOW_SEEK_TAIL, true,
								  &isnull, NULL);
	if (isnull)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(result);
}

/*
 * nth_value
 * return the value of VE evaluated on the n-th row from the first
 * row of the window frame, per spec.
 */
Datum
window_nth_value(PG_FUNCTION_ARGS)
{
	WindowObject winobj = PG_WINDOW_OBJECT();
	bool		const_offset;
	Datum		result;
	bool		isnull;
	int32		nth;

	nth = DatumGetInt32(WinGetFuncArgCurrent(winobj, 1, &isnull));
	if (isnull)
		PG_RETURN_NULL();
	const_offset = get_fn_expr_arg_stable(fcinfo->flinfo, 1);

	if (nth <= 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE),
				 errmsg("argument of nth_value must be greater than zero")));

	result = WinGetFuncArgInFrame(winobj, 0,
								  nth - 1, WINDOW_SEEK_HEAD, const_offset,
								  &isnull, NULL);
	if (isnull)
		PG_RETURN_NULL();

	PG_RETURN_DATUM(result);
}

/* SPECHT */

/*
 * Struktura slouzici k uchovani informace v win_get_partition_local_memory a pomocnych funkcich
 */
typedef struct window_memory_context
{
	float8	calculated_value;
} window_memory_context;

/*
 * Obalkova funkce pro WinGetPartitionLocalMemory, aby mohla byt pouzita z PL/pgSQL
 */
Datum win_get_partition_local_memory(PG_FUNCTION_ARGS)
{
	WindowObject winobj = (WindowObject) PG_GETARG_POINTER(0);	/* Struktura uchovavajici dulezite informace k window funkci */
	if(WindowObjectIsValid(winobj)){	/* Funkce z windowapi.h, ktera zkouma validitu WindowObject objektu*/
		window_memory_context	*context;	/* Ukazatel na strukturu uchovavajici hodnotu ulozenou pri predchozi iteraci v soucasne partition */
		context = (window_memory_context *) WinGetPartitionLocalMemory(winobj, sizeof(window_memory_context)); /* Z pameti se vrati ukazatel na strukturu */
		PG_RETURN_FLOAT8(context->calculated_value);
	}
	else
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("WindowObject function argument empty or corrupted")));	/* WindowObject obsahoval nevalidni hodnotu */
}

/*
 * Pomocna funkce pro win_get_partition_local_memory - zkouma, zda je ve strukture z partition pameti ulozena informace. Vraci true/false.
 */
Datum win_is_context_in_local_memory(PG_FUNCTION_ARGS)
{
	WindowObject winobj = (WindowObject) PG_GETARG_POINTER(0);	/* Struktura uchovavajici dulezite informace k window funkci */
	if(WindowObjectIsValid(winobj)){	/* Funkce z windowapi.h, ktera zkouma validitu WindowObject objektu*/
		window_memory_context	*context;	/* Ukazatel na strukturu uchovavajici hodnotu ulozenou pri predchozi iteraci v soucasne partition */
		context = (window_memory_context *) WinGetPartitionLocalMemory(winobj, sizeof(window_memory_context));	/* Z pameti se vrati ukazatel na strukturu */
		if(!context->calculated_value)
			PG_RETURN_BOOL(false);
		PG_RETURN_BOOL(true);
	}
	else
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("WindowObject function argument empty or corrupted")));	/* WindowObject obsahoval nevalidni hodnotu */
	
}

/*
 * Pomocna funkce pro win_get_partition_local_memory - ulozi do struktury vypocitanou hodnotu pro pozdejsi pouziti.
 */
Datum win_set_partition_local_memory(PG_FUNCTION_ARGS)
{
	WindowObject winobj = (WindowObject) PG_GETARG_POINTER(0);	/* Struktura uchovavajici dulezite informace k window funkci */
	float8 res = PG_GETARG_FLOAT8(1);	/* 
										 * Struktura, ktera drzi hodnotu v pameti, obsahuje float8. Pro obecne pouziti by bylo 
										 * potreba vytvorit v kazde funkci rozcestnik podle datoveho typu na vstupu. Pro demonstraci
										 * funkcnosti v ramci prace je staticky pouzit datovy typ float8.
										 */
	if(WindowObjectIsValid(winobj)){	/* Funkce z windowapi.h, ktera zkouma validitu WindowObject objektu */
		window_memory_context	*context;	/* Ukazatel na strukturu uchovavajici hodnotu ulozenou pri predchozi iteraci v soucasne partition */
		context = (window_memory_context *) WinGetPartitionLocalMemory(winobj, sizeof(window_memory_context));	/* Z pameti se vrati ukazatel na strukturu */
		context->calculated_value = res;	
		PG_RETURN_NULL();
	}
	else
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("WindowObject function argument empty or corrupted")));	/* WindowObject obsahoval nevalidni hodnotu */
}

/*
 * Obalkova funkce pro WinGetCurrentPosition - vraci aktualni pozici v ramci partition
 */
Datum win_get_current_position(PG_FUNCTION_ARGS)
{
	WindowObject winobj = (WindowObject) PG_GETARG_POINTER(0);	/* Struktura uchovavajici dulezite informace k window funkci */
	if(WindowObjectIsValid(winobj)){	/* Funkce z windowapi.h, ktera zkouma validitu WindowObject objektu */
		int64 curpos = WinGetCurrentPosition(winobj);
		PG_RETURN_INT64(curpos);
	}
	else
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("WindowObject function argument empty or corrupted")));	/* WindowObject obsahoval nevalidni hodnotu */
}

/*
 * Obalkova funkce pro WinSetMarkPosition - nastavuje novou pozici v ramci partition 
 */
Datum win_set_mark_position(PG_FUNCTION_ARGS)
{
	WindowObject winobj = (WindowObject) PG_GETARG_POINTER(0);	/* Struktura uchovavajici dulezite informace k window funkci */
	if(WindowObjectIsValid(winobj)){	/* Funkce z windowapi.h, ktera zkouma validitu WindowObject objektu */
		int64 markpos = PG_GETARG_INT64(1);
		WinSetMarkPosition(winobj, markpos);
		PG_RETURN_NULL();
	}
	else
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("WindowObject function argument empty or corrupted")));	/* WindowObject obsahoval nevalidni hodnotu */
}

/*
 * Obalkova funkce pro WinGetPartitionRowCount - vraci pocet radku v ramci soucasne partition
 */
Datum win_get_partition_row_count(PG_FUNCTION_ARGS)
{
	WindowObject winobj = (WindowObject) PG_GETARG_POINTER(0);	/* Struktura uchovavajici dulezite informace k window funkci */
	if(WindowObjectIsValid(winobj)){	/* Funkce z windowapi.h, ktera zkouma validitu WindowObject objektu */
		int64 curpos = WinGetPartitionRowCount(winobj);
		PG_RETURN_INT64(curpos);
	}
	else
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("WindowObject function argument empty or corrupted")));	/* WindowObject obsahoval nevalidni hodnotu */
}

/*
 * Obalkova funkce pro WinRowsArePeers - porovnava radky podle absolutni pozice v partition, zda jsou si rovne podle ORDER BY klauzule
 */
Datum win_rows_are_peers(PG_FUNCTION_ARGS)
{
	WindowObject winobj = (WindowObject) PG_GETARG_POINTER(0);	/* Struktura uchovavajici dulezite informace k window funkci */
	int64 pos1 = PG_GETARG_INT64(1);	/* Pozice prvniho radku */
	int64 pos2 = PG_GETARG_INT64(2);	/* Pozice druheho radku */
	if(WindowObjectIsValid(winobj)){	/* Funkce z windowapi.h, ktera zkouma validitu WindowObject objektu */
		bool ret = WinRowsArePeers(winobj, pos1, pos2);
		PG_RETURN_BOOL(ret);
	}
	else
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("WindowObject function argument empty or corrupted")));	/* WindowObject obsahoval nevalidni hodnotu */
}

/*
 * Obalkova funkce pro WinGetFuncArgInPartition - vraci x-ty funkcni argument (argno) v soucasne partition, 
 * specifikovano podle pozice (relpos) a podle ceho se pozice pocita (seektype - viz windowapi.h)
 */
Datum win_get_func_arg_in_partition (PG_FUNCTION_ARGS)
{
	WindowObject winobj = (WindowObject) PG_GETARG_POINTER(0);	/* Struktura uchovavajici dulezite informace k window funkci */
	int argno = PG_GETARG_INT32(1); 
	int relpos = PG_GETARG_INT32(2);
	int seektype = PG_GETARG_INT32(3);
	bool set_mark = PG_GETARG_BOOL(4);
	bool isnull;
	bool isout;
	if(WindowObjectIsValid(winobj)){	/* Funkce z windowapi.h, ktera zkouma validitu WindowObject objektu */
		Datum ret = WinGetFuncArgInPartition(winobj, argno, relpos, seektype, set_mark, &isnull, &isout);
		if(isout) /* Hledani se dostalo mimo soucasnou partition */
			elog(WARNING, "Row out of the partition");
		if (isnull)	/* Hodnota argumentu je NULL */
			PG_RETURN_NULL();
		PG_RETURN_DATUM(ret);
	}
	else
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("WindowObject function argument empty or corrupted")));	/* WindowObject obsahoval nevalidni hodnotu */
}

/*
 * Obalkova funkce pro WinGetFuncArgInFrame - vraci x-ty funkcni argument (argno) v celem frame, 
 * specifikovano podle pozice (relpos) a podle ceho se pozice pocita (seektype - viz windowapi.h - nemelo by byt WINDOW_SEEK_CURRENT)
 */
Datum win_get_func_arg_in_frame (PG_FUNCTION_ARGS)
{
	WindowObject winobj = (WindowObject) PG_GETARG_POINTER(0);	/* Struktura uchovavajici dulezite informace k window funkci */
	int argno = PG_GETARG_INT32(1);
	int relpos = PG_GETARG_INT32(2);
	int seektype = PG_GETARG_INT32(3);
	bool set_mark = PG_GETARG_BOOL(4);
	bool isnull;
	bool isout;
	if(WindowObjectIsValid(winobj)){	/* Funkce z windowapi.h, ktera zkouma validitu WindowObject objektu */
		Datum ret = WinGetFuncArgInFrame(winobj, argno, relpos, seektype, set_mark, &isnull, &isout);
		if(isout)	/* Hledani se dostalo mimo frame */
			elog(WARNING, "Row out of the frame");
		if (isnull)	/* Hodnota argumentu je NULL */
			PG_RETURN_NULL();
		PG_RETURN_DATUM(ret);
	}
	else
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("WindowObject function argument empty or corrupted")));	/* WindowObject obsahoval nevalidni hodnotu */
}

/*
 * Obalkova funkce pro WinGetFuncArgCurrent - vraci x-ty funkcni argument (argno) na aktualni pozici
 */
Datum win_get_func_arg_current (PG_FUNCTION_ARGS)
{
	WindowObject winobj = (WindowObject) PG_GETARG_POINTER(0);	/* Struktura uchovavajici dulezite informace k window funkci */
	int argno = PG_GETARG_INT32(1);
	bool isnull;
	if(WindowObjectIsValid(winobj)){	/* Funkce z windowapi.h, ktera zkouma validitu WindowObject objektu */
		Datum ret = WinGetFuncArgCurrent(winobj, argno, &isnull);
		if (isnull)	/* Hodnota argumentu je NULL */
			PG_RETURN_NULL();	
		PG_RETURN_DATUM(ret);
	}
	else
		ereport(ERROR, (errcode(ERRCODE_DATA_EXCEPTION), errmsg("WindowObject function argument empty or corrupted")));	/* WindowObject obsahoval nevalidni hodnotu */
}

/*
 * Funkce slouzici pro vytvoreni vlastniho datoveho typu window_object
 * Je nutne specifikovat dve funkce - 1) in (na vstupu dostane cstring retezec a vrati objekt)
 * Pro ucely prace je funkce prazdna a vrati staticky stejne hodnoty, protoze se nevyuzije jeji ucel
 */
Datum window_object_in(PG_FUNCTION_ARGS)
{
	PG_RETURN_NULL();
}

/*
 * Funkce slouzici pro vytvoreni vlastniho datoveho typu window_object
 * Je nutne specifikovat dve funkce - 2) out (na vstupu dostane objekt a vrati cstring retezec)
 * Pro ucely prace je funkce prazdna a vrati staticky stejne hodnoty, protoze se nevyuzije jeji ucel
 */
Datum window_object_out(PG_FUNCTION_ARGS)
{
	PG_RETURN_CSTRING("TEST");
}

/* SPECHT */