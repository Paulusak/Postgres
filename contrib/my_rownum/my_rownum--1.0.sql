/* contrib/my_rownum/my_rownum--1.0.sql */

/* Akumulator pro funkci myAVG - za kazdy radek na vstupu se provede definovana operace */
CREATE FUNCTION accumulator(anyarray, float8)
RETURNS anyarray -- anyarray je pole o libovolnem obsahu
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE; 
/* 
 * IMMUTABLE - nemeni databazi a se stejnymi argumenty vraci vzdy stejny vysledek
 * STRICT - pokud je argument NULL - vrati automaticky NULL
 * PARALLEL SAFE - funkci lze pustit v paralelnim modu bez omezeni
 */

/* Funkce, ktera se vykona po provedeni funkce accumulator pro vsechny radky ze vstupu */
CREATE FUNCTION finalcalc(anyarray)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

/* Syntaxe pro vytvoreni vlastni agregacni funkce */
CREATE AGGREGATE myAVG(float8)  (
    sfunc = accumulator, -- fce pracuje s kazdym radkem, dostava tmp hodnoty
    stype = float8[],	-- pole floatu je ocekavany vstup
    finalfunc = finalcalc, -- vykona se na konci
    initcond = '{0,0}' -- pocatecni hodnoty
);

/* Implementace zakladni window funkce (row_number) - ukazka toho, ze lze v PL/pgSQL vytvorit vlastni window funkci */
CREATE FUNCTION my_rownum() 
RETURNS int8 
AS $$
DECLARE
curpos int8;
BEGIN
    curpos := win_get_current_position(winobj); /* Do promenne se ulozi aktualni pozice */
    PERFORM win_set_mark_position(winobj, curpos); /* Posune se pozice */
    RETURN curpos + 1;  /* Jelikoz pozice zacina od 0, pripocita se 1 */
END
$$ LANGUAGE plpgsql WINDOW;

/* Implementace vlastni agregacni funkce (avg), ktera funguje jako analyticka (window) funkce v PL/pgSQL */
CREATE FUNCTION my_window_avg(int)
RETURNS float8
AS $$
DECLARE
partition_count int := 0;
cnt int := 0;
sum float8 := 0;
result float8 := 0;
precalculated bool := false;
BEGIN
    partition_count := win_get_partition_row_count(winobj); /* Ziska se pocet radku v aktualni partition */
    precalculated := win_is_context_in_local_memory(winobj); /* Musi se vysledek pocitat, ci jiz byl pro aktualni partition vypocitan a ulozen? */
    IF(precalculated) THEN
        result := win_get_partition_local_memory(winobj); /* Vysledek se vyzvedne z pameti */
        return result;
    ELSE
        FOR cnt IN 0 .. partition_count - 1
        LOOP
            sum := sum + win_get_func_arg_in_partition(winobj, 0, cnt, 1, false);   /* Pripocitavaji se hodnoty do celkove sumy */
        END LOOP; 
        result := sum / partition_count::float8; /* Vysledek je typu float8, proto je nutne partition_count pretypovat */
        PERFORM win_set_partition_local_memory(winobj, result); /* Do lokalni pameti se ulozi vysledek pro pozdejsi pouziti */
        RETURN result;
    END IF;
END
$$ LANGUAGE plpgsql WINDOW;