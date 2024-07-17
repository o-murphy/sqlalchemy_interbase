
SET TERM ^ ;

-- Abs Procedure
CREATE PROCEDURE Abs (
    input_value DOUBLE PRECISION
) RETURNS (
    result_value DOUBLE PRECISION
) AS
BEGIN
    IF (input_value < 0) THEN
        result_value = -input_value;
    ELSE
        result_value = input_value;
    SUSPEND;
END^

-- Ceil Procedure
CREATE PROCEDURE Ceil (
    input_value DOUBLE PRECISION
) RETURNS (
    result_value INTEGER
) AS
BEGIN
    result_value = CAST(input_value AS INTEGER);
    IF (result_value < input_value) THEN
        result_value = result_value + 1;
    SUSPEND;
END^

-- Floor Procedure
CREATE PROCEDURE Floor (
    input_value DOUBLE PRECISION
) RETURNS (
    result_value INTEGER
) AS
BEGIN
    result_value = CAST(input_value AS INTEGER);
    IF (result_value > input_value) THEN
        result_value = result_value - 1;
    SUSPEND;
END^

-- MaxNum Procedure
CREATE PROCEDURE MaxNum (
    f1 DOUBLE PRECISION,
    f2 DOUBLE PRECISION
) RETURNS (
    max_value DOUBLE PRECISION
) AS
BEGIN
    IF (f1 >= f2) THEN
        max_value = f1;
    ELSE
        max_value = f2;
    SUSPEND;
END^

-- MinNum Procedure
CREATE PROCEDURE MinNum (
    f1 DOUBLE PRECISION,
    f2 DOUBLE PRECISION
) RETURNS (
    min_value DOUBLE PRECISION
) AS
BEGIN
    IF (f1 <= f2) THEN
        min_value = f1;
    ELSE
        min_value = f2;
    SUSPEND;
END^

-- Power Procedure
CREATE PROCEDURE Power (
    base_value DOUBLE PRECISION,
    exponent_value DOUBLE PRECISION
) RETURNS (
    result_value DOUBLE PRECISION
) AS
DECLARE VARIABLE i INTEGER;
BEGIN
    IF (exponent_value = 0.0) THEN
        result_value = 1.0;
    ELSE IF (exponent_value > 0.0) THEN BEGIN
        result_value = 1.0;
        i = ABS(CAST(exponent_value AS INTEGER));
        WHILE (i > 0) DO BEGIN
            result_value = result_value * base_value;
            i = i - 1;
        END
    END ELSE BEGIN
        result_value = 1.0;
        i = ABS(CAST(exponent_value AS INTEGER));
        WHILE (i > 0) DO BEGIN
            result_value = result_value / base_value;
            i = i - 1;
        END
    END
    SUSPEND;
END^

-- CREATE PROCEDURE Round (
--     f DOUBLE PRECISION,
--     i INTEGER
-- ) RETURNS (
--     rounded_value DOUBLE PRECISION
-- ) AS
-- DECLARE VARIABLE factor DOUBLE PRECISION;
-- DECLARE VARIABLE shift DOUBLE PRECISION;
-- BEGIN
--     factor = CAST(1 AS DOUBLE PRECISION);
--     shift = CAST(0.5 AS DOUBLE PRECISION);
--
--     IF (f < 0) THEN
--         shift = -shift;
--
--     WHILE (i > 0) DO BEGIN
--         factor = factor * 10;
--         i = i - 1;
--     END
--
--     rounded_value = CAST((f * factor + shift) AS INTEGER) / factor;
--     SUSPEND;
-- END^

-- CREATE PROCEDURE Round (
--     f DOUBLE PRECISION,
--     i INTEGER
-- ) RETURNS (
--     rounded_value DOUBLE PRECISION
-- ) AS
-- DECLARE VARIABLE factor DOUBLE PRECISION;
-- DECLARE VARIABLE power INTEGER;
-- BEGIN
--     power = i;
--     factor = 1.0;
--
--     WHILE (power > 0) DO BEGIN
--         factor = factor * 10.0;
--         power = power - 1;
--     END
--
--     IF (f >= 0) THEN
--         rounded_value = CAST((f * factor + 0.5) AS DOUBLE PRECISION) / factor;
--     ELSE
--         rounded_value = CAST((f * factor - 0.5) AS DOUBLE PRECISION) / factor;
--
--     SUSPEND;
-- END^

SET TERM ; ^

-- http://freeadhocudf.org/ftp/adhoc20101206/install/UDF_Windows/InterBase/