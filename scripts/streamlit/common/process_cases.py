import threading
from snowflake.snowpark.functions import concat, lit, col, max
from langchain_text_splitters import CharacterTextSplitter
import tiktoken
import os
from .cortex_llm import CortexLLM, ProgressCallback
from langchain.chains import (
    StuffDocumentsChain,
    MapReduceDocumentsChain,
    ReduceDocumentsChain,
    LLMChain,
)

from langchain_core.prompts import PromptTemplate
from langchain.cache import InMemoryCache
from langchain.globals import set_llm_cache
from snowflake.snowpark.types import (
    StructType,
    StructField,
    StringType,
    DateType,
    TimestampType,
    VariantType,
)

from queue import Empty, Queue

from datetime import datetime, timedelta

set_llm_cache(InMemoryCache())


def process_cases(
    session,
    weeks_back,
    categories,
    prefix,
    cortex_search,
    progress_bar,
    concurrency,
    model="mistral-large",
):
    session = session
    latest_case_date = session.table("SUPPORT_CASES").select(max(col("DATE_CREATED"))).collect()[0][0]

    support_tickets = session.table("SUPPORT_CASES").filter(col("CATEGORY").isin(categories)).filter(col("DATE_CREATED") > latest_case_date - timedelta(weeks=weeks_back))
       

    cases_df = support_tickets.select(
        concat(
            lit("##### \nCASE TITLE: "),
            col("CASE_TITLE"),
            lit("\n\nCASE DESCRIPTION: "),
            col("CASE_DESCRIPTION"),
            lit("\n\nCASE STATUS: "),
            col("STATUS"),
            lit("\n\nLAST COMMENT: "),
            col("LAST_UPDATE"),
        ).alias("CASE_STRING")
    )

    # num_tokens = cases_df.select(
    #     F.array_size(F.split(col("CASE_STRING"), lit(" "))).alias("num_tokens")
    # )
    # num_tokens.agg(F.sum(col("num_tokens")).alias("total_words")).show()

    pandas_df = cases_df.to_pandas()
    if pandas_df.empty:
        raise ValueError("No data found for the given filters.")

    # Convert the Pandas DataFrame to a single appended string
    appended_string = pandas_df.apply(
        lambda x: " ".join(x.astype(str)), axis=1
    ).str.cat(sep=" ")

    def num_tokens_from_string(string: str) -> int:
        encoding = tiktoken.get_encoding("cl100k_base")
        num_tokens = len(encoding.encode(string))
        return num_tokens

    text_splitter = CharacterTextSplitter().from_tiktoken_encoder(
        "cl100k_base",
        separator="#####",
        chunk_size=20000,
        chunk_overlap=4000,
        is_separator_regex=False,
    )
    texts = text_splitter.create_documents([appended_string])

    map_template = PromptTemplate.from_template(
        """
                                                Given the following support cases for an order, return a summary of each case.
                                                Include details on the category of the issue, the errors or symptoms the customer noticed,
                                                and any basic details about what the customer was looking to accomplish.
                                                If multiple cases exist in the same category, you can group them together.
                                                The summary will be used to understand overall case trends and causes that the team can 
                                                use to prioritize fixes and improvements.
                                                    
                                                ### Cases ###
                                                
                                                {cases}
                                                    """
    )

    reduce_template = PromptTemplate.from_template(
        """
                                                    Given the following set of summaries for support case reports opened for an order , 
                                                    distill it into a final, consolidated and detailed summary of trends and top pain points or blockers customers have been hitting.
                                                    Prioritize issue categories that show up in multiple summaries as they are likely to be the most impactful.
                                                    Include a description of the issue, the symptoms the customer noticed, what they were trying to do, and what led them to open the case.
                                                
                                                    ### Case Chunk Summaries ###
                                                
                                                    {summaries} 
                                                """
    )

    llm = CortexLLM(
        model=model,
        max_retries=2,
        retry_delay=0,
        session=session,
        concurrency=concurrency,
    )

    progress_queue = Queue()
    result_queue = Queue()

    def background_task(chain, texts, handler, result_queue):
        result = chain.invoke(texts, {"callbacks": [handler]})
        result_queue.put(result)

    handler = ProgressCallback(len(texts), progress_queue)
    map_chain = LLMChain(llm=llm, prompt=map_template, callbacks=[handler])
    reduce_chain = LLMChain(llm=llm, prompt=reduce_template, callbacks=[handler])

    combine_docs_chain = StuffDocumentsChain(
        llm_chain=reduce_chain, document_variable_name="summaries"
    )

    reduce_documents_chain = ReduceDocumentsChain(
        combine_documents_chain=combine_docs_chain,
        collapse_documents_chain=combine_docs_chain,
        token_max=28000,
    )

    map_reduce_chain = MapReduceDocumentsChain(
        llm_chain=map_chain,
        reduce_documents_chain=reduce_documents_chain,
        document_variable_name="cases",
        return_intermediate_steps=True,
    )

    progress_bar.progress(0, text=f"Processing cases... (Total Chunks: {len(texts)})")
    thread = threading.Thread(
        target=background_task, args=(map_reduce_chain, texts, handler, result_queue)
    )

    thread.start()

    # Main Streamlit loop to handle progress updates
    while thread.is_alive() or not progress_queue.empty() or not result_queue.empty():
        try:
            task = progress_queue.get(timeout=1)
            if task[0] == "update":
                finished, total = task[2], task[3]
                if finished >= total - 2:
                    text = "Summarizing chunks...."
                else:
                    text = f"Processing cases... (Chunks finished: {finished} | Total chunks: {total - 2})"
                progress_value = min(finished / total, 0.95)
                # print(
                #     "progress_value, start, finish, total: ",
                #     progress_value,
                #     task[1],
                #     task[2],
                #     task[3],
                # )
                progress_bar.progress(progress_value, text=text)
        except Empty:
            pass
        try:
            result = result_queue.get_nowait()
            break
        except Empty:
            pass

    # Ensure thread has finished
    thread.join()

    schema = StructType(
        [
            StructField("datetime", TimestampType()),
            StructField("day", DateType()),
            StructField("output_text", StringType()),
            StructField("intermediate_steps", VariantType()),
        ]
    )

    current_datetime = datetime.now()
    current_date = current_datetime.date()

    data = [
        (
            current_datetime,
            current_date,
            result["output_text"],
            result["intermediate_steps"],
        )
    ]
    df = session.create_dataframe(data, schema=schema)

    df.write.save_as_table(f"{prefix}_SUMMARIES", mode="append")

    if cortex_search:
        support_pd = support_tickets.with_column(
            "INDEX_TEXT",
            concat(
                lit("##### \nCASE TITLE: "),
                col("CASE_TITLE"),
                lit("\n\nCASE DESCRIPTION: "),
                col("CASE_DESCRIPTION"),
                lit("\n\nCASE STATUS: "),
                col("STATUS"),
                lit("\n\nLAST COMMENT: "),
                col("LAST_UPDATE"),
            ),
        )

        support_pd.write.save_as_table(f"{prefix}_CASES", mode="append")

        session.sql(
            f"""
        CREATE OR REPLACE CORTEX SEARCH SERVICE {prefix}_CORTEX_SEARCH
                    ON INDEX_TEXT
                    WAREHOUSE = {str(os.getenv("DATAOPS_PREFIX") + "_DATA_APP_WH")}
                    TARGET_LAG = '1 day'
                    AS (
                        SELECT INDEX_TEXT, DATE_CREATED, CASE_TITLE, CASE_ID FROM {prefix}_CASES
                    )
        """
        ).collect()
    return llm.total_tokens
