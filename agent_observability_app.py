import streamlit as st
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.snowpark import Session
import os
from dotenv import load_dotenv
from typing import Optional
import ast
import json

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
                # Create Snowpark Session using connection parameters
                connection_parameters = {
                    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                    "user": os.getenv("SNOWFLAKE_USER"),
                    "password": os.getenv("SNOWFLAKE_PASSWORD"),
                    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
                    "database": os.getenv("SNOWFLAKE_DATABASE", "SNOWFLAKE"),
                    "schema": os.getenv("SNOWFLAKE_SCHEMA", "LOCAL"),
                    "role": os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN")
                }
                
                st.session_state.connection = Session.builder.configs(connection_parameters).create()
                
                # st.success("✅ Connected to Snowflake")
            except Exception as connection_failed:
                st.error(f"❌ Connection failed: {connection_failed}")
                return None
    return st.session_state.connection

def build_query(agent_name: Optional[str] = None, record_id: Optional[str] = None, user_feedback: Optional[str] = None) -> str:
    """Build the query with optional filters for AGENT_NAME and THREAD_ID"""
    
    # Base query
    query = """
WITH RESULTS AS (SELECT 
    TIMESTAMP AS TS,
    RECORD_ATTRIBUTES:"snow.ai.observability.object.name" AS AGENT_NAME,
    RECORD_ATTRIBUTES:"ai.observability.record_id" AS RECORD_ID, 
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.thread_id" AS THREAD_ID,
    RECORD_ATTRIBUTES:"ai.observability.record_root.input" AS INPUT_QUERY,
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.planning.thinking_response" AS AGENT_PLANNING,
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_analyst.sql_query" AS GENERATED_SQL,
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.sql_execution.result" AS SQL_RESULT,
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_search.results" AS CORTEX_SEARCH_RESULT,
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.custom_tool.results" AS CUSTOM_TOOL_RESULT,
    RECORD_ATTRIBUTES:"ai.observability.record_root.output" AS AGENT_RESPONSE, 
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.planning.model" AS REASONING_MODEL, 
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.planning.tool.name" AS AVAILABLE_TOOLS, 
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.planning.tool_selection.name" AS TOOL_SELECTION,
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.cortex_search.name" AS CSS_NAME,

    RECORD:"name" as TOOL_CALL,

    RECORD_ATTRIBUTES:"snow.ai.observability.agent.planning.tool_selection.type" AS TOOL_TYPE,
    CASE 
        WHEN RECORD_ATTRIBUTES:"snow.ai.observability.agent.tool.id" IS NOT NULL     
        AND RECORD:"name" NOT IN ('SqlExecution', 'SqlExecution_CortexAnalyst','CortexChartToolImpl-data_to_chart')
 
        THEN OBJECT_CONSTRUCT (
            'tool_name',
            TOOL_CALL,
            'tool_output',
            OBJECT_CONSTRUCT(
            'SQL',
            GENERATED_SQL,
            'search results',
            CORTEX_SEARCH_RESULT,
            'CUSTOM_TOOL_RESULT',
            CUSTOM_TOOL_RESULT
            ))
        ELSE NULL
        END AS TOOL_ARRAY,

    CASE
        WHEN VALUE:"positive"='true' THEN 1
        WHEN VALUE:"positive"='false'THEN 0
        ELSE NULL
        END AS USER_FEEDBACK,
    VALUE:"feedback_message" AS USER_FEEDBACK_MESSAGE,
    RECORD:"name" as OPERATION
    
    FROM SNOWFLAKE.LOCAL.AI_OBSERVABILITY_EVENTS 
    WHERE SCOPE:name = 'snow.cortex.agent'
    AND OPERATION !='Agent'
    """
    
    # Add optional filters
    filters = []
    if agent_name:
        filters.append(f"AND AGENT_NAME = '{agent_name}'")
    if record_id:
        filters.append(f"AND RECORD_ID = '{record_id}'")
    
    query += " ".join(filters)
    query += """
    ORDER BY THREAD_ID, TS, START_TIMESTAMP ASC)

    SELECT 
        RECORD_ID,
        MIN(TS) AS START_TS,
        MAX(TS) AS END_TS,
        DATEDIFF(SECOND, START_TS, END_TS)::FLOAT AS LATENCY, 
        MIN(AGENT_NAME) AS AGENT_NAME,
        MIN(INPUT_QUERY) AS INPUT_QUERY,
        MIN(AGENT_RESPONSE) AS AGENT_RESPONSE,
        MIN(AGENT_PLANNING) AS AGENT_PLANNING,
        ARRAY_AGG(TOOL_ARRAY) WITHIN GROUP (ORDER BY TS ASC) AS TOOL_ARRAY,
        MIN(USER_FEEDBACK) AS USER_FEEDBACKS,
        MIN(USER_FEEDBACK_MESSAGE) AS USER_FEEDBACK_MESSAGES
    
        FROM RESULTS    
        GROUP BY RECORD_ID"""
    feedback_filter = []
    if user_feedback == 'Positive Feedback Only':
        feedback_filter.append(f" HAVING USER_FEEDBACKS = 1")
    elif user_feedback == 'Negative Feedback Only':
        feedback_filter.append(f" HAVING USER_FEEDBACKS = 0")
    elif user_feedback == 'Any Feedback':
        feedback_filter.append(f" HAVING USER_FEEDBACKS IS NOT NULL")
    query += " ".join(feedback_filter)
    
    query += """
    ORDER BY START_TS DESC;

    """
    
    return query

# def convert_tools_list_to_sequence_dict(tool_list):
#     """
#     Convert a list of tool names into a list of dictionaries with tool names and sequence numbers
    
#     Args:
#         tool_list (list): List of tool names
        
#     Returns:
#         list: List of dictionaries containing tool_name and tool_sequence
#     """
#     return [
#         {
#             'tool_name': ast.literal_eval(tool)[0],
#             'tool_sequence': str(idx + 1)
#         }
#         for idx, tool in enumerate(tool_list)
#     ]

def add_tool_sequence(tool_list):

    for idx, tool in enumerate(tool_list):
        tool.update({'tool_sequence': idx+1})
        
    new_order = ['tool_sequence', 'tool_name', 'tool_output']

    # return a new list where each dict's keys appear in new_order
    final_tool_list = [
        {k: tool[k] for k in new_order if k in tool}
        for tool in tool_list
    ]
    return final_tool_list

def execute_query_and_postprocess(session, query: str) -> pd.DataFrame:
    """Execute query and return results as pandas DataFrame
        Perform some operations in pandas to clean up data."""
    try:
        data = session.sql(query)
        
        # Convert to DataFrame
        df = data.to_pandas()

        #Pandas post processing section

        #Drop duplicate queries
        df.drop_duplicates(subset=['AGENT_NAME', 'INPUT_QUERY'], inplace=True)
        
        #Create tool selection sequence
        df['TOOL_CALLING']  = df.TOOL_ARRAY.apply(lambda x: add_tool_sequence(ast.literal_eval(x)))
        df['EXPECTED_TOOLS'] = df.apply(lambda x: {'ground_truth_invocations': x.TOOL_CALLING, 'ground_truth_output': x.AGENT_RESPONSE} ,axis=1)

        # df['TOOL_CALLING_READABLE'] = df.explode('TOOL_CALLING')
        final_df = df[['RECORD_ID', 'START_TS', 'AGENT_NAME',
              'INPUT_QUERY', 'AGENT_RESPONSE', 'TOOL_CALLING','EXPECTED_TOOLS', 
               'LATENCY','USER_FEEDBACKS', 'USER_FEEDBACK_MESSAGES']]

        return final_df
    except Exception as e:
        st.error(f"Query execution failed: {e}")
        raise

def write_to_table(session, df: pd.DataFrame, table_name: str, database: Optional[str] = None, 
                   schema: Optional[str] = None, overwrite: bool = False) -> bool:
    """Write DataFrame to Snowflake table"""
    try:
        # Parse table name (could be DATABASE.SCHEMA.TABLE or TABLE)
        target_table = table_name.upper()
        
        # Handle both Snowpark Session and connector connection
        if hasattr(session, 'write_pandas'):
            # Snowpark Session
            success = session.write_pandas(df, target_table, auto_create_table=True, overwrite=overwrite)
        else:
            # Connector connection - use write_pandas from snowflake.connector.pandas_tools
            success = write_pandas(session, df, target_table, auto_create_table=True, overwrite=overwrite)
        
        return success
    except Exception as e:
        st.error(f"Failed to write to table: {e}")
        return False

def load_from_table(session, table_name: str) -> pd.DataFrame:
    """Load data from Snowflake table"""
    try:
        # Parse table name (could be DATABASE.SCHEMA.TABLE or TABLE)
        target_table = table_name.upper()
        
        query = f"SELECT * FROM {target_table}"
        
        # Handle both Snowpark Session and connector connection
        if hasattr(session, 'sql'):
            # Snowpark Session
            data = session.sql(query)
            df = data.to_pandas()
        else:
            # Connector connection
            df = pd.read_sql(query, session)
        
        return df
    except Exception as e:
        st.error(f"Failed to load from table: {e}")
        return pd.DataFrame()

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
    record_id = st.text_input("RECORD_ID (optional)", value="")
    user_feedback = st.selectbox(
                        "User Feedback:",
                        ("Positive Feedback Only", "Negative Feedback Only", "Any Feedback"),
                        index=None,
                        placeholder= "Select Option")
    
    st.markdown("**Note:** Leave filters empty to see all results")

# Main content area
if st.session_state.connection:
    # Initialize session state for editing
    if 'editing_df' not in st.session_state:
        st.session_state.editing_df = None
    
    # Create tabs
    tab1, tab2 = st.tabs(["📊 Query & View", "✏️ Edit Dataset"])
    
    with tab1:
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
                        record_id=record_id.strip() if record_id else None,
                        user_feedback=user_feedback
                    )
                    
                    try:
                        df = execute_query_and_postprocess(session, query)
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
            st.dataframe(session.create_dataframe(df), use_container_width=True, height=400)
            
            # Download as CSV
            csv = df.to_csv(index=False)
            st.download_button(
                label="📥 Download CSV",
                data=csv,
                file_name=f"observability_events_{agent_name or 'all'}_{record_id or 'all'}.csv",
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
                    help="Enter full path (DATABASE.SCHEMA.TABLE)",
                    key="query_table_name"
                )
            
            with col2:
                overwrite = st.checkbox("Overwrite table", value=False, help="If checked, will drop and recreate the table", key="query_overwrite")
            
            st.write("")  # Spacing
            st.write("")  # Spacing
            if st.button("📤 Write to Table", type="primary", key="query_write_button"):
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
                    record_id=record_id.strip() if record_id else None,
                    user_feedback=user_feedback
                )
                st.code(query, language="sql")
    
    with tab2:
        st.subheader("✏️ Edit Dataset")
        st.markdown("Load a dataset from query results or a Snowflake table, then edit and save your changes.")
        
        session = st.session_state.connection
        
        # Data source selection
        data_source = st.radio(
            "Data Source:",
            ["Load from Query Results", "Load from Snowflake Table"],
            horizontal=True,
            help="Choose to edit query results or load from an existing table"
        )
        
        # Table name input for loading (show before button for better UX)
        load_table_name = ""
        if data_source == "Load from Snowflake Table":
            load_table_name = st.text_input(
                "Table Name (Use DATABASE.SCHEMA.TABLE_NAME format)",
                value="",
                placeholder="AGENT_EVAL_DB.SCHEMA.EVAL_DATASET_v0",
                help="Enter full path (DATABASE.SCHEMA.TABLE)",
                key="load_table_name"
            )
        
        # Load button
        if st.button("🔄 Load Data", type="primary"):
            if data_source == "Load from Query Results":
                if 'query_executed' in st.session_state and st.session_state.query_executed:
                    st.session_state.editing_df = st.session_state.query_results.copy()
                    st.success(f"✅ Loaded {len(st.session_state.editing_df)} rows from query results")
                    st.rerun()
                else:
                    st.warning("⚠️ Please run a query first in the 'Query & View' tab")
            else:
                # Load from table
                if load_table_name.strip():
                    with st.spinner("Loading data from table..."):
                        df = load_from_table(session, load_table_name.strip())
                        if not df.empty:
                            st.session_state.editing_df = df
                            st.success(f"✅ Loaded {len(df)} rows from {load_table_name}")
                            st.rerun()
                        else:
                            st.error("❌ No data loaded. Please check the table name.")
                else:
                    st.warning("⚠️ Please enter a table name")
        
        st.divider()
        
        # Display editable dataframe
        if st.session_state.editing_df is not None:
            df = st.session_state.editing_df
            
            st.metric("Records in Dataset", len(df))
            
            # Add new row button
            if st.button("➕ Add New Row", type="secondary"):
                # Create a new row with None/NaN values based on column dtypes
                new_row_dict = {}
                for col in df.columns:
                    if df[col].dtype == 'object':
                        new_row_dict[col] = None
                    elif pd.api.types.is_numeric_dtype(df[col]):
                        new_row_dict[col] = pd.NA
                    else:
                        new_row_dict[col] = None
                new_row = pd.DataFrame([new_row_dict])
                st.session_state.editing_df = pd.concat([df, new_row], ignore_index=True)
                st.rerun()
            
            # Display editable dataframe
            st.markdown("**Edit the dataset below. Double-click cells to edit values. You can also delete rows by selecting them.**")
            edited_df = st.data_editor(
                df,
                use_container_width=True,
                height=400,
                num_rows="dynamic",
                key="data_editor"
            )
            
            # Update session state with edited data (data_editor automatically tracks changes)
            st.session_state.editing_df = edited_df
            
            st.divider()
            
            # Save section
            st.subheader("💾 Save Changes to Snowflake Table")
            
            col1, col2, col3 = st.columns([3, 1, 1])
            
            with col1:
                save_table_name = st.text_input(
                    "Table Name (Use DATABASE.SCHEMA.TABLE_NAME format)",
                    value=f"AGENT_EVAL_DB.{agent_name or 'EVAL_SETS'}.EVAL_DATASET_v0",
                    placeholder=f"AGENT_EVAL_DB.{agent_name or 'EVAL_SETS'}.EVAL_DATASET_v0",
                    help="Enter full path (DATABASE.SCHEMA.TABLE)",
                    key="save_table_name"
                )
            
            with col2:
                save_overwrite = st.checkbox("Overwrite table", value=False, help="If checked, will drop and recreate the table", key="save_overwrite")
            
            st.write("")  # Spacing
            st.write("")  # Spacing
            
            col1, col2 = st.columns([1, 4])
            with col1:
                if st.button("💾 Save Changes", type="primary"):
                    if save_table_name.strip():
                        with st.spinner("Saving changes to table..."):
                            success = write_to_table(session, edited_df, save_table_name.strip(), overwrite=save_overwrite)
                            if success:
                                st.success(f"✅ Successfully saved {len(edited_df)} rows to {save_table_name}")
                                st.session_state.editing_df = edited_df.copy()
                            else:
                                st.error("❌ Failed to save data to table")
                    else:
                        st.warning("⚠️ Please enter a table name")
            
            with col2:
                # Download edited CSV
                csv = edited_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download Edited CSV",
                    data=csv,
                    file_name=f"edited_dataset_{save_table_name.replace('.', '_') if save_table_name else 'dataset'}.csv",
                    mime="text/csv"
                )
        else:
            st.info("👈 Load data using the options above to start editing")
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

