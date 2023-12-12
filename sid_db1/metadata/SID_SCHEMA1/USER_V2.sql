create or replace view USER_V2(
	ID,
	NAME,
	ACTIVE
) as select * from sid_db1.sid_schema1.users;