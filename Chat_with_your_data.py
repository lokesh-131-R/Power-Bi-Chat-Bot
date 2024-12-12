import json
import pandas as pd
import streamlit as st
import snowflake.connector
import os
import openai
from langchain_openai import ChatOpenAI
from langchain_experimental.agents import create_pandas_dataframe_agent

# Snowflake connection details
SNOWFLAKE_USER = "dsx_dashboards_powerbi_service_account"
SNOWFLAKE_PASSWORD = "B!_PowerBI2024"
SNOWFLAKE_ACCOUNT = "c2gpartners.us-east-1"
SNOWFLAKE_DATABASE = "DSX_DASHBOARDS"
SNOWFLAKE_SCHEMA = "HUBSPOT_REPORTING"
SNOWFLAKE_WAREHOUSE = "POWERHOUSE"

# Establish connection to Snowflake
conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER,
    password=SNOWFLAKE_PASSWORD,
    account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WAREHOUSE,
)

# SQL query to execute
query = """
SELECT DISTINCT_CAPABILITY AS "Number of capability",
       SNAPSHOT_DATE AS "Date of data pulled",
       PORTFOLIO_LEAD AS "Team",
       BD_LEAD AS "Deal Owner",
       PARTNER_SOURCE_TYPE AS "Partner Source Type",
       OPPORTUNITY_CREATE_DATE AS "Date",
       EXTRACT(MONTH FROM OPPORTUNITY_CREATE_DATE) AS "Opportunity Month",
       EXTRACT(YEAR FROM OPPORTUNITY_CREATE_DATE) AS "Opportunity Year",
       OPPORTUNITY_ID,
       OPPORTUNITY_NAME AS "Deal Name",
       EXPECTED_PROJECT_DURATION_IN_MONTHS AS "Expected Project duration",
       REVENUE_TYPE_ID,
       PIPELINE,
       DEAL_TYPE_ID,
       CASE 
           WHEN OPPORTUNITY_STAGE_ID_DESC = 'STAGE 0' THEN '0-NEW'
           WHEN OPPORTUNITY_STAGE_ID_DESC = 'STAGE 1' THEN '1-Connected to Meeting'
           WHEN OPPORTUNITY_STAGE_ID_DESC = 'STAGE 2' THEN '2-Needs Expressed'
           WHEN OPPORTUNITY_STAGE_ID_DESC = 'STAGE 3' THEN '3-Qualified Opportunity'
           WHEN OPPORTUNITY_STAGE_ID_DESC = 'STAGE 4' THEN '4-Proposal Presented'
           WHEN OPPORTUNITY_STAGE_ID_DESC = 'STAGE 5' THEN '5-Verbal Agreement'
           WHEN OPPORTUNITY_STAGE_ID_DESC = 'STAGE 6' THEN '6-Contracted'
           ELSE OPPORTUNITY_STAGE_ID_DESC
       END AS "Stage type",
       AMOUNT,
       TCV,
       MRR,
       ICV
FROM DSX_DASHBOARDS.HUBSPOT_REPORTING.VW_DEALS_LINE_ITEMS_DATA
WHERE SNAPSHOT_DATETIME = 
        (
            SELECT MAX(SNAPSHOT_DATETIME)
            FROM DSX_DASHBOARDS.HUBSPOT_REPORTING.VW_DEALS_LINE_ITEMS_DATA
        )
"""

df_Snowflack = pd.read_sql(query, conn)

# Set up Streamlit page configuration
st.set_page_config(layout='wide')

# Authentication system
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

def authenticate(username, password):
    return username == "Blend360" and password == "Blend@360"

if not st.session_state["authenticated"]:
    st.title("Login")
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    if st.button("Login"):
        if authenticate(username, password):
            st.session_state["authenticated"] = True
            st.success("Login successful!")
        else:
            st.error("Invalid username or password.")
    st.stop()

# Streamlit UI for refreshing the page
st.button("refresh")

# Header for the chat interface
st.header("Power BI Smart Bot")

columns = st.columns([8, 2])

# Load Excel and JSON data directly from predefined local paths
json_path = r"DataModelSchema.json"
excel_path = r"Data DictionaryChat bot.xlsx"

json_data = pd.read_json(json_path, encoding='utf-16')
df = pd.DataFrame()

# Process JSON data
table_1 = list(json_data["model"]['tables'])
for i in range(len(table_1)):
    table = table_1[i]
    if 'measures' in table:
        df = pd.concat([df, pd.DataFrame(table['measures'])], ignore_index=True)
Measure_Table = df[["name", "expression"]]
Measure_Table = Measure_Table.rename(columns={"expression": "DAX", "name": "Dax Name"})

df_1 = pd.DataFrame(columns=['Table Name', 'Column Name'])
tables = json_data["model"]['tables']
for table in tables:
    if 'columns' in table:
        for column in table['columns']:
            df_1 = pd.concat([df_1, pd.DataFrame({'Table Name': [table['name']], 'Column Name': [column['name']], 'Data Type': [column['dataType']]})], ignore_index=True)

# Process Excel data
xls_data = pd.read_excel(excel_path)

# Set up OpenAI API key
if "OPENAI_API_KEY" not in st.session_state:
    api_key = st.text_input("Upload your API Key", type="password")
    if api_key:
        os.environ["OPENAI_API_KEY"] = api_key
        openai.api_key = os.environ["OPENAI_API_KEY"]

llm = ChatOpenAI(model="gpt-4", temperature=0.1, max_tokens=5000)

# Create agents for each dataframe
agents = {
    "Power Bi Calculation": create_pandas_dataframe_agent(df=Measure_Table, llm=llm, allow_dangerous_code=True, handle_parsing_errors=True),
    "Power Bi Table": create_pandas_dataframe_agent(df=df_1, llm=llm, allow_dangerous_code=True, handle_parsing_errors=True),
    "Data": create_pandas_dataframe_agent(df=df_Snowflack, llm=llm, allow_dangerous_code=True, handle_parsing_errors=True),
    "Dictionary": create_pandas_dataframe_agent(df=xls_data, llm=llm, allow_dangerous_code=True, handle_parsing_errors=True)
}

# Chat interface
if "messages" not in st.session_state:
    st.session_state.messages = []

with columns[1]:
    with st.container(border=True):
        response_container = st.container(height=450)
        selected_df = st.selectbox("Select Topic to chat with", list(agents.keys()))
        if prompt := st.chat_input("Ask your question here"):
            st.session_state.messages.append({"role": "user", "content": prompt})
            conversation_history = "\n".join([msg["content"] for msg in st.session_state.messages])

            try:
                agent = agents[selected_df]
                response = agent.invoke(conversation_history)
                
                # Extract the response or fallback to string
                response_content = (
                    response.get("output", "") 
                    if isinstance(response, dict) else str(response)
                )
                if not response_content.strip():
                    response_content = "No valid response received."

            except Exception as e:
                response_content = f"Error occurred: {str(e)}"

            st.session_state.messages.append({"role": "assistant", "content": response_content})
            with response_container:
                for message in st.session_state.messages:
                    with st.chat_message(message["role"]):
                        st.markdown(message["content"])

        if st.button('Clear Chat'):
            st.session_state.messages = []

with columns[0]:
    with st.container(border=True):
        if url := st.text_input("Place your Power BI URL"):
            st.markdown(
                f'<iframe width="850" height="600" src="{url}" frameborder="0" allowFullScreen="true"></iframe>',
                unsafe_allow_html=True
            )
