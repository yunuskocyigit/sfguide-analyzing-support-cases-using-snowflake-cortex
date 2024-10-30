from snowflake.snowpark import Session
import os
import streamlit as st

@st.cache_resource
def connect_to_snowflake():
    try:
        username = os.environ["DATAOPS_SNOWFLAKE_USER"]
        password = os.environ["DATAOPS_SNOWFLAKE_PASSWORD"]
        account = os.environ["DATAOPS_SNOWFLAKE_ACCOUNT"]
        database_name = os.environ["DATAOPS_DATABASE"]
        role = str(os.getenv("DATAOPS_CATALOG_SOLUTION_PREFIX") + "_ADMIN")
        warehouse = str(os.getenv("DATAOPS_CATALOG_SOLUTION_PREFIX") + "_DATA_APP_WH")
        schema = "SPCS"
        
    except KeyError:
        raise Exception("Could not find one or more required environment variables")
    
    return  Session.builder.configs(
            {
                "account": account,
                "user": username,
                "password": password,
                "role": role,
                "warehouse": warehouse,
                "database": database_name,
                "schema": "SUPPORT",
            }
        ).create()
