CREATE OR REPLACE PROCEDURE "ADDTWO"("I" NUMBER(38,0))
RETURNS NUMBER(38,0)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'addtwo_py'
EXECUTE AS OWNER
AS '
def addtwo_py(session, i):
  return session.call(''sid_db1.sid_schema1.addnine'', i) + 1 
';