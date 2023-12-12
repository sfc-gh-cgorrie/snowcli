create or replace TABLE USERS (
	ID NUMBER(38,0) autoincrement start 1 increment 1 order,
	NAME VARCHAR(100) NOT NULL,
	ACTIVE BOOLEAN DEFAULT TRUE
);