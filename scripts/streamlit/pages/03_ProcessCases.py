import os
from common.process_cases import process_cases
from common.app_tools import connect_to_snowflake

import streamlit as st  # Import python packages
from datetime import datetime, timedelta

from snowflake.snowpark.functions import col, max



map_prompt = """Given the following support cases for Snowflake, return a summary of each case.
Include details on the category of the issue, the errors or symptoms the customer noticed,
and any basic details about what the customer was looking to accomplish.
If multiple cases exist in the same category, you can group them together.
The summary will be used to understand overall case trends and causes that the team can 
use to prioritize fixes and improvements.
    
### Cases ###

{cases}"""

reduce_prompt = """Given the following set of summaries for support case reports opened for Snowflake, 
distill it into a final, consolidated and detailed summary of trends and top pain points or blockers customers have been hitting.
Prioritize issue categories that show up in multiple summaries as they are likely to be the most impactful.
Include a description of the issue, the symptoms the customer noticed, what they were trying to do, and what led them to open the case.
Ideally provide the response in a list of 3 to 8 areas, each that has a paragrapha or two of detail about the area or trend.

### Case Chunk Summaries ###

{summaries}"""

if "initialized" not in st.session_state:
    st.session_state.initialized = True
    st.session_state.running = False

session = connect_to_snowflake()


### Sidebar

# sidebar to choose how many weeks back to filter
weeks = st.sidebar.slider("Filter by weeks back", 1, 26, 12)
# show text in sidebar of what the starting date would be with selected filter by calculating datetime.now() - timedelta(weeks=weeks)
start_date = datetime.now() - timedelta(weeks=weeks)
formatted_date = start_date.strftime("%Y-%m-%d")
st.sidebar.write(f"Cases starting at: {formatted_date}")

#latest_case_date = session.table("SUPPORT_CASES").select(max(col("DATE_CREATED"))).collect()[0][0]

concurrency = st.sidebar.slider("Mapping Concurrency", 1, 10, 5)

with st.sidebar.expander("Prompts"):
    map_prompt_input = st.text_area(
        label="Map Prompt", value=map_prompt, key="map_prompt"
    )

    reduce_prompt_input = st.text_area(
        label="Reduce Prompt", value=reduce_prompt, key="reduce_prompt"
    )

create_cortext = st.sidebar.toggle("Create Cortex Search", value=True)

### Main

prefix = st.text_input("Prefix for summary", "ALL")


case_categories = (
    session.table("SUPPORT_CASES")
    .select(col("CATEGORY"))
    .distinct()
    .to_pandas()
    .sort_values("CATEGORY")
)

categories = st.multiselect(
    "Select Categories",
    case_categories,
    disabled=st.session_state.running,
    default=case_categories["CATEGORY"],
)
if categories:
    st.expander("Preview cases").dataframe(
        session.table("SUPPORT_CASES")
        .select(
            col("DATE_CREATED"),
            col("CASE_TITLE"),
            col("CATEGORY"),
        )
        .filter(col("CATEGORY").isin(categories))
        .filter(col("DATE_CREATED") > start_date)
        .limit(1000)
        .to_pandas()
    )

    if st.button("Process cases", disabled=st.session_state.running):
        st.session_state.running = True
        st.rerun()
    
    if "total_tokens" in st.session_state and not st.session_state.running:
        total_tokens = st.session_state.total_tokens
        elapsed_time = st.session_state.get("elapsed_time", timedelta(0))
        credits_required = (total_tokens / 1000000) * 5.10
        with st.expander("Cost Estimate", expanded=True):
            elapsed_time_str = (datetime.min + elapsed_time).strftime("%H:%M:%S")
            st.write(f"Elapsed time: {elapsed_time_str}")
            st.write(f"Total tokens used: {total_tokens}")
            st.write(f"Credits required: {credits_required:.2f}")
            st.write(f"Estimated cost: ${(credits_required*2):.2f}")

if st.session_state.running:
    with st.spinner("Processing cases. This may take a few minutes..."):
        progress_bar = st.progress(0, "Processing cases...")
        start_time = datetime.now()

        try:
            total_tokens = process_cases(
                session,
                weeks,
                categories,
                prefix,
                create_cortext,
                progress_bar,
                concurrency,
            )
            st.success("Processing complete. Check Summary tab.")
        except Exception as e:
            st.error(f"Error processing cases: {str(e)}")

        # end timer
        end_time = datetime.now()
        elapsed_time = end_time - start_time

        st.session_state.running = False
        st.session_state.total_tokens = total_tokens
        st.session_state.elapsed_time = elapsed_time
        progress_bar.empty()
        st.rerun()