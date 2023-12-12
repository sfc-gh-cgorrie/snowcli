CREATE OR REPLACE FUNCTION "SAY_HELLO"("P" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'library.say_hello'
IMPORTS = ('@STAGE_DB.PUBLIC.MY_STAGE/library.py')
COMMENT='Says hello'
;