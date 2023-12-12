
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col

session = get_active_session()

st.header("Welcome to streamlit")

st.subheader("Users")
users_df = session.table("sid_schema1.users").collect()
st.dataframe(users_df)

st.subheader("Let's say hello")
your_name = st.text_input("What is your name?")
if st.button("Say hello!"):
    st.text(session.sql("select sid_schema1.say_hello(?) as ABC", params=[your_name]).collect()[0]["ABC"])

st.subheader("Check out a circle thing")
pi = session.call("sid_schema1.get_pi")
st.text(f"Turns out, pi is actually {pi}")
