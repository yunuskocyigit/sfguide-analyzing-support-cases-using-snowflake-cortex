import streamlit as st  # Import python packages
import json
import snowflake.snowpark.functions as F
from datetime import timedelta, datetime as dt
from langchain_text_splitters import CharacterTextSplitter
import tiktoken
import pickle
import os

from typing import Any, Dict, List, Optional
import time
import json

from snowflake.cortex import Complete
from langchain_core.callbacks.manager import CallbackManagerForLLMRun
from langchain_core.language_models.llms import LLM

from langchain_core.prompts import PromptTemplate
from langchain.chains import StuffDocumentsChain, MapReduceDocumentsChain, ReduceDocumentsChain, LLMChain
from langchain.cache import InMemoryCache
from langchain.globals import set_llm_cache

from snowflake.snowpark.types import StructType, StructField, StringType, DateType, TimestampType, VariantType
import datetime
from common.app_tools import connect_to_snowflake


st.title(":telephone_receiver: Support Case Summary (powered by Cortex LLMs)")

# Establishing session
session = connect_to_snowflake()
current_database = session.get_current_database()
print(current_database)
current_schema = "SUPPORT"


def get_tables():
    return session.sql(
        f"""SHOW TABLES LIKE '%SUM%' IN SCHEMA {current_database}.{current_schema}"""
    ).collect()


def get_analysis(table):
    return session.table(f"{current_database}.{current_schema}.{table}").to_pandas()


tables_list = get_tables()
# convert tables_list list to an array of strings
tables_list = [table[1] for table in tables_list]
table = st.selectbox("Select a table", tables_list)
if table:
    analysis_pd = get_analysis(table)
    # find the most recent record in the analysis_pd pandas dataframe, which can be found by the one record with the most recent DATETIME column
    most_recent_record = analysis_pd.loc[analysis_pd["DATETIME"].idxmax()]

    # create a streamlit markdown for the column value of output_text
    st.markdown(most_recent_record["OUTPUT_TEXT"])

    with st.expander("Intermediate Summaries"):
        # for each value in the json array for the column "INTERMEDIATE_STEPS", create a streamlit (ideally collapsible) markdown for the value
        json_steps = json.loads(most_recent_record["INTERMEDIATE_STEPS"])
        for step in json_steps:
            st.markdown(step)