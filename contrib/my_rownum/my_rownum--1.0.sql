/* contrib/my_rownum/my_rownum--1.0.sql */

CREATE FUNCTION my_test()
RETURNS int
AS 'MODULE_PATHNAME'
LANGUAGE C;

CREATE FUNCTION my_rownum() 
RETURNS int 
AS $$
BEGIN
RETURN my_window_row_number(winobj);
END
$$ LANGUAGE plpgsql WINDOW;

CREATE FUNCTION my_rank() 
RETURNS int 
AS $$
BEGIN
RETURN my_window_rank(winobj);
END
$$ LANGUAGE plpgsql WINDOW;
