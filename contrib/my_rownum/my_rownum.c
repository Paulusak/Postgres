#include "postgres.h"

#include "catalog/pg_collation.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/varlena.h"
#include <ctype.h>
#include <float.h>
#include <math.h>
#include <limits.h>

#include "catalog/pg_type.h"
#include "common/int.h"
#include "common/shortest_dec.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "utils/array.h"
#include "utils/float.h"
#include "utils/fmgrprotos.h"
#include "utils/sortsupport.h"
#include "utils/timestamp.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test);

//vase ukazka z kavarny
Datum test(PG_FUNCTION_ARGS)
{
	int	   first = PG_GETARG_INT32(0);
	int	   second = PG_GETARG_INT32(1);
	int		result;
	result = first + second;
	PG_RETURN_INT32(result);
}

/* Retezeni stringu --------------------------------------------------------------------------*/

PG_FUNCTION_INFO_V1(myconcat);

//reseni viz text_catenate ve src/backend/utils/adt/varlena.c
//kdyz jsem prepracovaval tuto fci, stejne me to navedlo na tento zpusob, tak aspon zkusim zakomentovat postup, zda jsem ho spravne pochopil
Datum myconcat (PG_FUNCTION_ARGS)
{
	text	   *left = PG_GETARG_TEXT_PP(0); //prvni argument fce jako text
	text	   *right = PG_GETARG_TEXT_PP(1); //druhy argument fce jako text
	text		*result;
	int 	   len, len1, len2;
	char	   *ptr;
	len1 = VARSIZE_ANY_EXHDR(left);	// ulozim si delku prvniho retezce bez hlavicky (hlavicka udava celkovou delku) 
	len2 = VARSIZE_ANY_EXHDR(right); // to stejne s druhym retezcem
	len = len1 + len2 + VARHDRSZ; // spocitam si delku, kterou budu potrebovat jako kombinaci obou vstupu a velikosti hlavicky u varleny
	result = (text *) palloc(len); // alokuji misto o teto delce a urcuji, ze je to text
	SET_VARSIZE(result, len); //nastavi hlavicku v result na celkovou delku
	ptr = VARDATA(result); //ukazatel na zacatek retezce bez hlavicky
	if (len1 > 0)
		memcpy(ptr, VARDATA_ANY(left), len1); // kopiruji len1 bitu z left(bez hlavicky) do ptr
	if (len2 > 0)
		memcpy(ptr + len1, VARDATA_ANY(right), len2); //stejne ale ptr je posunuty o len1
	PG_RETURN_TEXT_P(result); // predam vysledek
	
}

/* Vlastni agregacni fce - avg --------------------------------------------------------------------------*/


PG_FUNCTION_INFO_V1(accumulator); /*fce pridava mezivysledky*/

Datum
accumulator(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8		newval = PG_GETARG_FLOAT8(1);
	float8	   *transvalues;
	float8		counter, total;

	if (ARR_NDIM(transarray) != 1 || ARR_DIMS(transarray)[0] != 2 || ARR_HASNULL(transarray) || ARR_ELEMTYPE(transarray) != FLOAT8OID)
		elog(ERROR, "accumulator: expected 2-element float8 array");
	transvalues = (float8 *) ARR_DATA_PTR(transarray);
	counter = transvalues[0];
	total = transvalues[1];

	counter += 1.0;
	total += newval;
	transvalues[0] = counter;
	transvalues[1] = total;
	PG_RETURN_ARRAYTYPE_P(transarray);
}

PG_FUNCTION_INFO_V1(finalcalc); /*fce se vyhodnoti na konci*/

Datum
finalcalc(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transvalues;
	float8	counter,total;

	if (ARR_NDIM(transarray) != 1 || ARR_DIMS(transarray)[0] != 2 || ARR_HASNULL(transarray) || ARR_ELEMTYPE(transarray) != FLOAT8OID)
		elog(ERROR, "finalcalc: expected 2-element float8 array");
	transvalues = (float8 *) ARR_DATA_PTR(transarray);
	counter = transvalues[0];
	total = transvalues[1];
	if (counter == 0.0)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(total / counter);
}



