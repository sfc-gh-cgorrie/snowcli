
from snowflake.snowpark.session import Session

def get_pi(session: Session) -> float:
    return session.sql("select pi() as ACTUALLY_PI").collect()[0]["ACTUALLY_PI"]

def say_hello(p: str) -> str:
    return f"Hello, {p}!"
