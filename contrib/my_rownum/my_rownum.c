#include "postgres.h"

#include "fmgr.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

PG_MODULE_MAGIC;

static HTAB *hashTable = NULL;

/*
 * Poznamky - pouzivejte tabelatory o sirce 4 mezery, nikoliv mezery.
 * chybi signatura V1 funkce PG_FUNCTION_INFO_V1(my_test);
 * Musi se definovat hash entry, kde prvni polozkou je klic a druhou obsah
 * hash_search vraci ukazatel na hash entry (kterou se alokovala v kontextu prirazenem hash tabulce)
 */

typedef struct hashEntry
{
	char		key[10];		/* max velikost klice, pokud se neuvede HASH_BLOBS, tak se porovnava jako retezec */
	int			value;
} hashEntry;

PG_FUNCTION_INFO_V1(my_test);
 
Datum
my_test(PG_FUNCTION_ARGS)
{
	bool		found;
	char		key[10];
	hashEntry  *hentry;

	if (hashTable == NULL)
	{
		MemoryContext private_context;
		HASHCTL		ctl;

		private_context = AllocSetContextCreate(TopMemoryContext ,
												"TEST- MEMORY",
												ALLOCSET_DEFAULT_MINSIZE,
												ALLOCSET_DEFAULT_INITSIZE,
												ALLOCSET_DEFAULT_MAXSIZE);

		MemSet(&ctl, 0, sizeof(ctl));
		ctl.keysize = 10 * sizeof(char);
		ctl.entrysize = sizeof(hashEntry);
		ctl.hcxt = private_context;

		hashTable = hash_create("TESTHASH", 20, &ctl,
								HASH_ELEM | HASH_CONTEXT);
	}

	strcpy(key, "TEST");
	hentry = (hashEntry *) hash_search(hashTable, key, HASH_ENTER, &found);

	if(!found)
		hentry->value = 0;
	else
		hentry->value++;

	PG_RETURN_INT32(hentry->value);
}
