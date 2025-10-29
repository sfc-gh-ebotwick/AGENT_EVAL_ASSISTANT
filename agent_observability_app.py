import streamlit as st
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.snowpark import Session
import os
from dotenv import load_dotenv
from typing import Optional

# Load environment variables
load_dotenv()

# Page config
st.set_page_config(
    page_title="AI Observability Events Query",
    page_icon="🔍",
    layout="wide"
)

# Initialize session state
if 'connection' not in st.session_state:
    st.session_state.connection = None

def get_snowflake_connection():
    """Get or create Snowflake connection"""
    if st.session_state.connection is None:
        try:
            session = Session.get_active_session()
            st.session_state.connection = session
            # st.success("✅ Connected to Snowflake via get_active_session()")
        except Exception as e:
            try: 
                st.session_state.connection = snowflake.connector.connect(
                    user=os.getenv("SNOWFLAKE_USER"),
                    password=os.getenv("SNOWFLAKE_PASSWORD"),
                    account=os.getenv("SNOWFLAKE_ACCOUNT"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
                    database=os.getenv("SNOWFLAKE_DATABASE", "SNOWFLAKE"),
                    schema=os.getenv("SNOWFLAKE_SCHEMA", "LOCAL"),
                    role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
                )
                # st.success("✅ Connected to Snowflake")
            except Exception as connection_failed:
                st.error(f"❌ Connection failed: {connection_failed}")
                return None
    return st.session_state.connection

def build_query(agent_name: Optional[str] = None, thread_id: Optional[str] = None) -> str:
    """Build the query with optional filters for AGENT_NAME and THREAD_ID"""
    
    # Base query
    query = """
WITH RESULTS AS (
    SELECT 
        TIMESTAMP AS TS,
        RECORD_ATTRIBUTES:"snow.ai.observability.object.name" AS AGENT_NAME,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.thread_id" AS THREAD_ID,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.parent_message_id" AS PARENT_MESSAGE_ID,
        COALESCE(
            PARENT_MESSAGE_ID,
            LAST_VALUE (PARENT_MESSAGE_ID) IGNORE NULLS OVER (
              PARTITION BY AGENT_NAME,
              THREAD_ID
              ORDER BY
                TIMESTAMP ROWS BETWEEN UNBOUNDED PRECEDING
                AND CURRENT ROW
            )
          ) AS PARENT_MESSAGE,

        CONCAT(THREAD_ID, '-', PARENT_MESSAGE) AS THREAD_MESSAGE_ID,

        VALUE:"snow.ai.observability.request_body"."messages"[0]."content"[0]."text" AS INPUT_QUERY,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.planning.thinking_response" AS AGENT_PLANNING,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_analyst.sql_query" AS GENERATED_SQL,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.sql_execution.result" AS SQL_RESULT,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_search.results" AS CORTEX_SEARCH_RESULT,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.custom_tool.results" AS CUSTOM_TOOL_RESULT,
        RECORD_ATTRIBUTES:"ai.observability.record_root.output" AS AGENT_RESPONSE, 
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.planning.model" AS REASONING_MODEL, 
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.planning.tool.name" AS AVAILABLE_TOOLS, 
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.planning.tool_selection.name" AS TOOL_SELECTION,
        CASE
            WHEN VALUE:"positive"='true' THEN 1
            WHEN VALUE:"positive"='false' THEN 0
            ELSE NULL
            END AS USER_FEEDBACK,
        VALUE:"feedback_message" AS USER_FEEDBACK_MESSAGE,

        RECORD:"name" as OPERATION,
        *
    FROM SNOWFLAKE.LOCAL.AI_OBSERVABILITY_EVENTS 
    WHERE SCOPE:name = 'snow.cortex.agent'
    AND OPERATION != 'Agent'
    """
    
    # Add optional filters
    filters = []
    if agent_name:
        filters.append(f"AND AGENT_NAME = '{agent_name}'")
    if thread_id:
        filters.append(f"AND THREAD_ID = '{thread_id}'")
    
    query += " ".join(filters)
    query += """
    ORDER BY THREAD_ID, TIMESTAMP, START_TIMESTAMP ASC
)
SELECT 
    THREAD_MESSAGE_ID,
    MIN(TIMESTAMP) AS TS,
    MIN(AGENT_NAME) AS AGENT_NAME,
    ARRAY_AGG(INPUT_QUERY) WITHIN GROUP (ORDER BY TIMESTAMP ASC) AS INPUT_QUERIES,
    ARRAY_AGG(AGENT_RESPONSE) WITHIN GROUP (ORDER BY TIMESTAMP ASC) AS AGENT_RESPONSE,
    ARRAY_AGG(AGENT_PLANNING) WITHIN GROUP (ORDER BY TIMESTAMP ASC) AS AGENT_PLANNING,
    ARRAY_AGG(TOOL_SELECTION) WITHIN GROUP (ORDER BY TIMESTAMP ASC) AS TOOL_CALLS,
    ARRAY_AGG(COALESCE(GENERATED_SQL, SQL_RESULT, CORTEX_SEARCH_RESULT, CUSTOM_TOOL_RESULT))  
    WITHIN GROUP (ORDER BY TIMESTAMP ASC) AS TOOL_RESULTS
FROM RESULTS
GROUP BY THREAD_MESSAGE_ID
ORDER BY TS ASC
"""
    
    return query

def execute_query(session, query: str) -> pd.DataFrame:
    """Execute query and return results as pandas DataFrame"""
    try:
        data = session.sql(query)
        
        # Convert to DataFrame
        df = data.to_pandas()
        return df
    except Exception as e:
        st.error(f"Query execution failed: {e}")
        raise

def write_to_table(session, df: pd.DataFrame, table_name: str, database: Optional[str] = None, 
                   schema: Optional[str] = None, overwrite: bool = False) -> bool:
    """Write DataFrame to Snowflake table"""
    try:
        # Parse table name (could be DATABASE.SCHEMA.TABLE or TABLE)
        target_table = table_name.upper()
        
        success = session.write_pandas(df, target_table, auto_create_table = True, overwrite=overwrite)
        
        return success
    except Exception as e:
        st.error(f"Failed to write to table: {e}")
        return False

# Main UI
st.title("🔍 AI Observability Events Query Tool")
st.markdown("Query and analyze AI observability events from Snowflake")

# Sidebar for connection and filters
with st.sidebar:
    st.header("Configuration")
    session = get_snowflake_connection()
    
    if st.session_state.connection:
        st.success("✅ Connected")
    
    st.divider()
    
    # Filters
    st.header("Filters")
    agent_name = st.text_input("AGENT_NAME (optional)", value="")
    thread_id = st.text_input("THREAD_ID (optional)", value="")
    
    st.markdown("**Note:** Leave filters empty to see all results")

# Main content area
if st.session_state.connection:
    # Query section
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.subheader("Query Results")
    
    with col2:
        if st.button("🔄 Run Query", type="primary"):
            with st.spinner("Executing query..."):
                session = st.session_state.connection
                query = build_query(
                    agent_name=agent_name.strip() if agent_name else None,
                    thread_id=thread_id.strip() if thread_id else None
                )
                
                try:
                    df = execute_query(session, query)
                    st.session_state.query_results = df
                    st.session_state.query_executed = True
                    st.rerun()
                except Exception as e:
                    st.error(f"Error: {e}")
    
    # Display results
    if 'query_executed' in st.session_state and st.session_state.query_executed:
        df = st.session_state.query_results
        
        st.metric("Records Returned", len(df))
        
        # Display DataFrame
        st.dataframe(df, use_container_width=True, height=400)
        
        # Download as CSV
        csv = df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv,
            file_name=f"observability_events_{agent_name or 'all'}_{thread_id or 'all'}.csv",
            mime="text/csv"
        )
        
        st.divider()
        
        # Write to table section
        st.subheader("💾 Write Results to Snowflake Table")
        
        col1, col2, col3 = st.columns([3, 1, 1])
        
        with col1:
            table_name = st.text_input(
                "Table Name (Use DATABASE.SCHEMA.TABLE_NAME format",
                value=f"AGENT_EVAL_DB.{agent_name or 'EVAL_SETS'}.EVAL_DATASET_v0",
                placeholder=f"AGENT_EVAL_DB.{agent_name or 'EVAL_SETS'}.EVAL_DATASET_v0",
                help="Enter full path (DATABASE.SCHEMA.TABLE)"
            )
        
        with col2:
            overwrite = st.checkbox("Overwrite table", value=False, help="If checked, will drop and recreate the table")
        
        st.write("")  # Spacing
        st.write("")  # Spacing
        if st.button("📤 Write to Table", type="primary"):
            if table_name.strip():
                with st.spinner("Writing data to table..."):
                    success = write_to_table(session, df, table_name.strip(), overwrite=overwrite)
                    if success:
                        st.success(f"✅ Successfully wrote {len(df)} rows to {table_name}")
                    else:
                        st.error("❌ Failed to write data to table")
            else:
                st.warning("⚠️ Please enter a table name")
        
        # Show query for reference
        with st.expander("📋 View Generated SQL Query"):
            query = build_query(
                agent_name=agent_name.strip() if agent_name else None,
                thread_id=thread_id.strip() if thread_id else None
            )
            st.code(query, language="sql")
else:
    st.info("👈 Please connect to Snowflake using the sidebar")
    
    # Show connection requirements
    with st.expander("📝 Connection Requirements"):
        st.markdown("""
        Required environment variables:
        - `SNOWFLAKE_ACCOUNT` - Your Snowflake account identifier
        - `SNOWFLAKE_USER` - Your Snowflake username
        - `SNOWFLAKE_PASSWORD` - Your Snowflake password
        
        Optional environment variables (with defaults):
        - `SNOWFLAKE_WAREHOUSE` (default: COMPUTE_WH)
        - `SNOWFLAKE_DATABASE` (default: SNOWFLAKE)
        - `SNOWFLAKE_SCHEMA` (default: LOCAL)
        - `SNOWFLAKE_ROLE` (default: ACCOUNTADMIN)
        """)

# Footer
st.divider()
st.markdown(
    "<small>AI Observability Events Query Tool | Query results are limited to optimize performance</small>",
    unsafe_allow_html=True
)

