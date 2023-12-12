create or replace view USER_V4(
	ID,
	NAME,
	ACTIVE
) as (
    select * from user_v3
    union
    select * from user_v2
    );