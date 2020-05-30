#include "postgres.h"

#include "catalog/pg_collation.h"
#include "utils/builtins.h"
#include "utils/formatting.h"
#include "utils/varlena.h"
#include <ctype.h>
#include <float.h>
#include <math.h>
#include <limits.h>
#include "windowapi.h"

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

PG_MODULE_MAGIC; // makro se stara o kompatibilitu knihoven

/* ------------------------------ Vlastni window PL/pgSQL vyuzivajici C API  ----------------------------------------------------- */

/* Makro zprostredkovava dostupnost funkce v SQL  */
PG_FUNCTION_INFO_V1(test);
PG_FUNCTION_INFO_V1(custom_rownum);
PG_FUNCTION_INFO_V1(custom_window_max);

/* Struktura, do ktera slouzi jako uloziste pro window funkci */
typedef struct window_memory_context
{
	int	calculated_value;
} window_memory_context;

Datum custom_window_max(PG_FUNCTION_ARGS)
{
	WindowObject win_obj = PG_WINDOW_OBJECT(); // ziskani window objektu do promenne
	int		partition_count = WinGetPartitionRowCount(win_obj); // pocet radek v aktualni partition
	window_memory_context * saved = (window_memory_context * ) WinGetPartitionLocalMemory(win_obj, sizeof(window_memory_context)); // z pameti se ziska ulozena informace v forme struktury vyse
	if(saved->calculated_value == 0){ // v pameti nebyla ulozena hodnota
		int max_value = 0;
		bool is_set = false;
		for (int i = 0; i < partition_count; i++) // iteruje se pres radky v partition
		{
			bool isnull;
			bool isout;
			int tmp_value = DatumGetInt32(WinGetFuncArgInPartition(win_obj, 0, i, 1, false, &isnull, &isout)); // do promenne se ulozi prvni (cislo 0) funkcni argument aktualniho radku
			if(isout) // radek je mimo frame
				elog(WARNING, "Row out of the frame");
			else if(!isnull){ // nastala hodnota NULL?
				if(!is_set){ // funkcionalita max funkce - je aktualni hodnota vetsi nez docasna? Ano - zmenim docasnou na tuto, Ne - nic se nedeje
						max_value = tmp_value;
						is_set = true; // v prvni iteraci je hodnota automaticky max
					}
					else if(tmp_value > max_value)
						max_value = tmp_value;	
				}
		}
		saved->calculated_value = max_value; // ulozim vysledek na pozdejsi vyuziti
	}
		return saved->calculated_value;	//vratim vysledek
}

/* ------------------------------ Vlastni window PL/pgSQL vyuzivajici C API  ----------------------------------------------------- */



/* ------------------------------ Retezeni stringu (funkce z ukazky vytvoreni extenze) ----------------------------------------------------- */

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

/* ------------------------------ Retezeni stringu (viz ukazka vytvoreni extenze) -----------------------------------------------------*/


/* --------------------------- Funkce k vlastni agregacni funkci myAVG (viz thesis_extension--1.0.sql) -----------------------------------------------*/


PG_FUNCTION_INFO_V1(accumulator); 
PG_FUNCTION_INFO_V1(finalcalc); 

/* funkce pridava mezivysledky */
Datum accumulator(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8		newval = PG_GETARG_FLOAT8(1);
	float8	   *transvalues;
	float8		counter, total;

	/* Zjistuje se, zda ma pole spravny pocet dimenzu, neni NULL a je spravneho datoveho typu */
	if (ARR_NDIM(transarray) != 1 || ARR_DIMS(transarray)[0] != 2 || ARR_HASNULL(transarray) || ARR_ELEMTYPE(transarray) != FLOAT8OID)
		elog(ERROR, "accumulator: expected 2-element float8 array");
	transvalues = (float8 *) ARR_DATA_PTR(transarray);
	counter = transvalues[0];
	total = transvalues[1];
	/* Do pole ulozim mezisoucet a pocet prvku celkem */
	counter += 1.0;
	total += newval;
	transvalues[0] = counter;
	transvalues[1] = total;
	PG_RETURN_ARRAYTYPE_P(transarray); // vracim pole
}


/* finalni funkce deli mezisoucet celkovym poctem hodnot a diky tomu vraci prumer*/
Datum
finalcalc(PG_FUNCTION_ARGS)
{
	ArrayType  *transarray = PG_GETARG_ARRAYTYPE_P(0);
	float8	   *transvalues;
	float8	counter,total;

	/* Zjistuje se, zda ma pole spravny pocet dimenzu, neni NULL a je spravneho datoveho typu */
	if (ARR_NDIM(transarray) != 1 || ARR_DIMS(transarray)[0] != 2 || ARR_HASNULL(transarray) || ARR_ELEMTYPE(transarray) != FLOAT8OID)
		elog(ERROR, "finalcalc: expected 2-element float8 array");
	transvalues = (float8 *) ARR_DATA_PTR(transarray);
	counter = transvalues[0];
	total = transvalues[1];
	if (counter == 0.0) // zadny radek na vstupu - vracim NULL (zaroven se tak zamezuje deleni nulou)
		PG_RETURN_NULL();

	PG_RETURN_FLOAT8(total / counter);
}

/* --------------------------- Funkce k vlastni agregacni funkci myAVG (viz thesis_extension--1.0.sql) -----------------------------------------------*/

