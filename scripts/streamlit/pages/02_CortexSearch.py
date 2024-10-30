import os
import streamlit as st  # Import python packages
from snowflake.core import Root
from snowflake.cortex import Complete
from common.app_tools import connect_to_snowflake
import altair as alt

COLUMNS = ["INDEX_TEXT", "DATE_CREATED","CASE_ID", "CASE_TITLE"]

MODELS = [
    "mistral-large",
    "snowflake-arctic",
    "llama3-70b",
    "llama3-8b",
]

# Establishing session
session = connect_to_snowflake()
root = Root(session)
st.title(":balloon: Support Cases Chatbot with Snowflake Cortex")

def init_config_options():
    service_show = session.sql(
        "SHOW CORTEX SEARCH SERVICES IN SCHEMA SUPPORT"
    ).collect()
    SERVICES = [service[1] for service in service_show]
    st.sidebar.selectbox(
        "Select cortex search service:",
        SERVICES,
        key="cortex_search_service",
    )
    
    st.sidebar.button("Clear conversation", key="clear_conversation")
    st.sidebar.toggle("Debug", key="debug", value=False)
    st.sidebar.toggle("Use chat history", key="use_chat_history", value=False)

    with st.sidebar.expander("Advanced options"):
        st.selectbox("Select model:", MODELS, key="model_name")
        st.number_input(
            "Select number of context chunks",
            value=5,
            key="num_retrieved_chunks",
            min_value=1,
            max_value=20,
        )
        st.number_input(
            "Select number of messages to use in chat history",
            value=5,
            key="num_chat_messages",
            min_value=1,
            max_value=20,
        )

    st.sidebar.expander("Session State").write(st.session_state)


def init_messages():
    if st.session_state.clear_conversation or "messages" not in st.session_state:
        st.session_state.messages = []

def query_cortex_search_service(query):
    db, schema = session.get_current_database(), session.get_current_schema()
    references = []

    cortex_search_service = (
        root.databases[db]
        .schemas[schema]
        .cortex_search_services[st.session_state.cortex_search_service]
    )

    context_documents = cortex_search_service.search(
        query, COLUMNS, limit=st.session_state.num_retrieved_chunks
    )
    results = context_documents.results
    # print(results)
    context_str = ""
    case_set = set()
    for i, r in enumerate(results):
        # check to make sure r has a property INDEX_TEXT
        if "INDEX_TEXT" not in r:
            continue
        context_str += f"Context support case {i+1}: {r['INDEX_TEXT']} \n" + "\n"
        case_id = r["CASE_ID"]
        if case_id not in case_set:
            references.append(
                {
                    "case_created_at": case_id,
                    "case_id": r["CASE_ID"],
                    "subject": r["CASE_TITLE"],
                }
            )
            case_set.add(case_id)  # Add the case to the set to mark it as added

    if st.session_state.debug:
        st.sidebar.text_area("Context cases", context_str, height=500)

    return context_str, references



def get_chat_history():
    start_index = max(
        0, len(st.session_state.messages) - st.session_state.num_chat_messages
    )
    return st.session_state.messages[start_index : len(st.session_state.messages) - 1]


def complete(model, prompt):
    response = Complete(model, prompt).replace("$", "\$")
    response = "\n".join([line.lstrip() for line in response.split("\n")])
    return response


def make_chat_history_summary(chat_history, question):
    # To get the right context, use the LLM to first summarize the previous conversation
    # This will be used to get embeddings and find similar chunks in the docs for context
    prompt = f"""
        [INST]
        Based on the chat history below and the question, generate a query that extend the question
        with the chat history provided. The query should be in natural language. 
        Answer with only the query. Do not add any explanation.

        <chat_history>
        {chat_history}
        </chat_history>
        <question>
        {question}
        </question>
        [/INST]
    """

    summary = complete(st.session_state.model_name, prompt)

    if st.session_state.debug:
        st.sidebar.text_area(
            "Chat history summary", summary.replace("$", "\$"), height=150
        )

    return summary


def create_prompt(user_question):
    if st.session_state.use_chat_history:
        chat_history = get_chat_history()
        if chat_history != []:
            question_summary = make_chat_history_summary(chat_history, user_question)
            prompt_context, references = query_cortex_search_service(question_summary)
        else:
            prompt_context, references = query_cortex_search_service(user_question)
    else:
        prompt_context, references = query_cortex_search_service(user_question)
        chat_history = ""

    prompt = f"""
            [INST]
            You are a helpful AI chat assistant with RAG capabilities. When a user asks you a question,
            you will also be given context provided between <context> and </context> tags. Use that context
            with the user's chat history provided in the between <chat_history> and </chat_history> tags
            to provide a summary that addresses the user's question. Ensure the answer is coherent, concise,
            and directly relevant to the user's question.

            If the user asks a generic question which cannot be answered with the given context or chat_history,
            just say "I don't know the answer to that question.
            
            Don't saying things like "according to the provided context".
            
            <chat_history>
            {chat_history}
            </chat_history>
            <context>          
            {prompt_context}
            </context>
            <question>  
            {user_question}
            </question>
            [/INST]
            Answer:
           """
    return prompt, references


def generate_response(question):
    prompt, references = create_prompt(question)
    response = complete(st.session_state.model_name, prompt)
    # append response with markdown link to the case
    if len(references) > 0:
        response += "\n\n##### References:"
        for i, ref in enumerate(references):
            response += f"\n\n{ref['case_id']} - **{ref['subject']}**"

    return response


init_config_options()
init_messages()

icons = {
    "assistant": "🤖", 
     "user": "👤"
}
# Display chat messages from history on app rerun
for message in st.session_state.messages:
    with st.chat_message(message["role"], avatar=icons[message["role"]]):
        st.markdown(message["content"])

if question := st.chat_input("What do you want to know about Snowflake Cases?"):
    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": question})
    # Display user message in chat message container
    with st.chat_message("user", avatar=icons["user"]):
        st.markdown(question.replace("$", "\$"))

    # Display assistant response in chat message container
    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        question = question.replace("'", "")
        with st.spinner("Thinking..."):
            generated_response = generate_response(question)
            message_placeholder.write(generated_response)

    st.session_state.messages.append(
        {"role": "assistant", "content": generated_response}
    )