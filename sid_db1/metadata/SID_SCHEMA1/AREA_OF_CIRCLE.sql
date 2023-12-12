CREATE OR REPLACE FUNCTION "AREA_OF_CIRCLE"("RADIUS" FLOAT)
RETURNS FLOAT
LANGUAGE SQL
AS '
    pi() * area_of_circle_dep() * area_of_circle_dep()
  ';