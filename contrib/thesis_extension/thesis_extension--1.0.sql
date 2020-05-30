/* contrib/thesis_extension/thesis_extension--1.0.sql */

/* ---------- Agregacni funkce myAVG pouzivana v testovani rychlosti zpracovani dotazu (zdrojove kody v C)--------- */

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

/* ---------- Agregacni funkce MyAVG pouzivana v testovani rychlosti zpracovani dotazu --------- */



/* ---------- Agregacni funkce customSUM pouzivana v kapitole Agregacni a window funkce (zdrojove kody v plpgsql) --------- */

/* Akumulator - tmp je mezivysledek, new_value je nova hodnota, kterou pripocita k tmp a vrati novy mezivysledek */
create function custom_accumulator(tmp int, new_value int)
returns int
immutable
language plpgsql
as $$
declare
  new_tmp int;
begin
  new_tmp := tmp + new_value;
  return new_tmp;
end;
$$;

/* Finalni funkce pouze vrati konecny vysledek z akumulatoru, protoze se jedna o sumu hodnot, tedy se nemuseji delat dalsi operace */
create function final_function(tmp int)
returns int
immutable
strict
language plpgsql
as $$
begin
  return tmp;
end;
$$;

/* Vlastni agregacni funkce customSUM pocita soucet hodnot ze vstupu */
CREATE AGGREGATE customSUM(int)  (
    sfunc = custom_accumulator, -- fce pracuje s kazdym radkem, dostava tmp hodnoty
    stype = int,	-- integer je ocekavany vstup
    finalfunc = final_function, -- vykona se na konci
    initcond = '0' -- pocatecni hodnoty
);

/* ---------- Agregacni funkce customSUM pouzivana v kapitole Agregacni a window funkce (zdrojove kody v plpgsql) --------- */



/* ---------- Implementace zakladni window funkce (row_number) - ukazka toho, ze lze v PL/pgSQL vytvorit vlastni window funkci ---------- */

CREATE FUNCTION my_rownum() 
RETURNS int8 
AS $$
DECLARE
curpos int8;
BEGIN
    curpos := win_get_current_position(winobj); /* Do promenne se ulozi aktualni pozice (vsimnete si auto promenne winobj, do ktere je dosazeno v interpretu) */
    PERFORM win_set_mark_position(winobj, curpos); /* Posune se pozice */
    RETURN curpos + 1;  /* Jelikoz pozice zacina od 0, pripocita se 1 */
END
$$ LANGUAGE plpgsql WINDOW;

/* ---------- Implementace zakladni window funkce (row_number) - ukazka toho, ze lze v PL/pgSQL vytvorit vlastni window funkci ---------- */



/* ---------- Implementace window funkce first_value v PL/pgSQL (pouziva se v testovani spravneho vystupu) ---------- */
CREATE FUNCTION my_window_first_value(anyelement) /* pseudotyp, ktery muze byt libovolneho datoveho typu */
RETURNS anyelement
AS $$
DECLARE
    result ALIAS FOR $0; /* promenna result je datoveho typu jako pseudotyp na vstupu (pri pouziti pseudotypu se v parametru $0 nachazi datovy typ podle realneho vstupu) */
BEGIN
    result := win_get_func_arg_in_frame(winobj, 0, 0, 1, true); /* Zavola se obalkova funkce z windowfuncs.c */
    return result;
END
$$ LANGUAGE plpgsql WINDOW;

/* ---------- Implementace window funkce first_value v PL/pgSQL (pouziva se v testovani spravneho vystupu) ---------- */


/* --------- Implementace vlastni agregacni funkce (avg), ktera funguje jako analyticka (window) funkce v PL/pgSQL --------- */
/* Pouziva se v testovani rychlosti zpracovani dotazu */
CREATE FUNCTION my_window_avg(int)
RETURNS float8
AS $$
DECLARE
partition_count int := 0;
null_count int := 0;
cnt int := 0;
sum float8 := 0;
result float8 := 0;
precalculated bool := false;
BEGIN
    precalculated := win_is_context_in_local_memory(winobj); /* Musi se vysledek pocitat, ci jiz byl pro aktualni partition vypocitan a ulozen? */
    IF(precalculated) THEN
        result := win_get_partition_local_memory(winobj); /* Vysledek se vyzvedne z pameti */
    ELSE
        partition_count := win_get_partition_row_count(winobj); /* Ziska se pocet radku v aktualni partition */
        FOR cnt IN 0 .. partition_count - 1
        LOOP
            DECLARE 
                tmp float8;
            BEGIN
                tmp := COALESCE(win_get_func_arg_in_partition(winobj, 0, cnt, 1, false), -1); /* Kontrola NULL hodnoty */
                IF (tmp = -1) THEN
                    null_count := null_count + 1;
                ELSE
                    sum := sum + tmp; /* Pripocitavaji se hodnoty do celkove sumy */
                END IF;        
            END;
        END LOOP; 
        partition_count := partition_count - null_count;
        IF (partition_count <> 0) THEN
            result := sum / partition_count::float8; /* Vysledek je typu float8, proto je nutne partition_count pretypovat */
        ELSE
            result := NULL;
        END IF;
        PERFORM win_set_partition_local_memory(winobj, result); /* Do lokalni pameti se ulozi vysledek pro pozdejsi pouziti */
    END IF;
    RETURN result;
END
$$ LANGUAGE plpgsql WINDOW CALLED ON NULL INPUT;

/* --------- Implementace vlastni agregacni funkce (avg), ktera funguje jako analyticka (window) funkce v PL/pgSQL --------- */


/* --------- Vlastni window PL/pgSQL vyuzivajici C API (definice v thesis_extension--1.0.sql) --------- */

CREATE FUNCTION custom_window_max(int)
RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C WINDOW;

/* --------- Vlastni window PL/pgSQL vyuzivajici C API (definice v thesis_extension--1.0.sql) --------- */