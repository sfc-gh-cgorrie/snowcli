create or replace view USER_V3(
	ID,
	NAME,
	ACTIVE
) as select * from user_v2;