/* contrib/my_rownum/my_rownum--1.0.sql */

CREATE FUNCTION accumulator(anyarray, float8)
RETURNS anyarray -- anyarray je pole o libovolnem obsahu
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE FUNCTION finalcalc(anyarray)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT PARALLEL SAFE;

CREATE AGGREGATE myAVG(float8)  (
    sfunc = accumulator, -- fce pracuje s kazdym radkem, dostava tmp hodnoty
    stype = float8[],	-- pole floatu je ocekavany vstup
    finalfunc = finalcalc, -- vykona se na konci
    initcond = '{0,0}' -- pocatecni hodnoty
);

CREATE FUNCTION my_rownum() 
RETURNS int 
AS $$
BEGIN
RETURN my_window_row_number(winobj);
END
$$ LANGUAGE plpgsql WINDOW;

CREATE FUNCTION my_rownum2() 
RETURNS int8 
AS $$
DECLARE
curpos int8;
BEGIN
    curpos := win_get_current_position(winobj);
    perform win_set_mark_position(winobj, curpos);
    RETURN curpos + 1;
END
$$ LANGUAGE plpgsql WINDOW;

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
    partition_count := win_get_partition_row_count(winobj);
    IF(precalculated) THEN
        result := win_get_partition_local_memory(winobj);
        return result;
    ELSE
        LOOP
            exit when cnt = partition_count;
            sum := sum + win_get_func_arg_in_partition(winobj, 0, cnt, 1, false);
            cnt := cnt + 1;
        END LOOP; 
        result := sum / cnt::float8;
        perform win_set_partition_local_memory(winobj, result);
        return result;
    END IF;
END
$$ LANGUAGE plpgsql WINDOW;