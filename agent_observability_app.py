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

def build_query(agent_name: Optional[str] = None, thread_id: Optional[str] = None, user_feedback: Optional[str] = None) -> str:
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
    AND THREAD_ID IS NOT NULL
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
    WITHIN GROUP (ORDER BY TIMESTAMP ASC) AS TOOL_RESULTS,
    ARRAY_AGG(USER_FEEDBACK) AS USER_FEEDBACKS,
    ARRAY_AGG(USER_FEEDBACK_MESSAGE) AS USER_FEEDBACK_MESSAGES
    
FROM RESULTS
GROUP BY THREAD_MESSAGE_ID
"""
    feedback_filter = []
    if user_feedback == 'Positive Feedback Only':
        feedback_filter.append(f"HAVING USER_FEEDBACKS[0] = 1")
    elif user_feedback == 'Negative Feedback Only':
        feedback_filter.append(f"HAVING USER_FEEDBACKS[0] = 0")
    elif user_feedback == 'Any Feedback':
        feedback_filter.append(f"HAVING USER_FEEDBACKS[0] IS NOT NULL")
    query += " ".join(feedback_filter)
    
    query += """
    ORDER BY TS ASC
    """
    
    return query

def convert_tools_list_to_sequence_dict(tool_list):
    """
    Convert a list of tool names into a list of dictionaries with tool names and sequence numbers
    
    Args:
        tool_list (list): List of tool names
        
    Returns:
        list: List of dictionaries containing tool_name and tool_sequence
    """
    return [
        {
            'tool_name': ast.literal_eval(tool)[0],
            'tool_sequence': str(idx + 1)
        }
        for idx, tool in enumerate(tool_list)
    ]

def execute_query_and_postprocess(session, query: str) -> pd.DataFrame:
    """Execute query and return results as pandas DataFrame
        Perform some operations in pandas to clean up data."""
    try:
        data = session.sql(query)
        
        # Convert to DataFrame
        df = data.to_pandas()

        #Pandas post processing section

        #Drop duplicate queries
        df.drop_duplicates(subset=['AGENT_NAME', 'INPUT_QUERIES'], inplace=True)
        
        #Create tool selection sequence
        df['TOOL_SELECTION'] = df.TOOL_CALLS.apply(lambda x: json.dumps(convert_tools_list_to_sequence_dict(ast.literal_eval(x))))
        
        return df
    except Exception as e:
        st.error(f"Query execution failed: {e}")
        raise

def write_to_table(session, df: pd.DataFrame, table_name: str, database: Optional[str] = None, 
                   schema: Optional[str] = None, overwrite: bool = False) -> bool:
    """Write DataFrame to Snowflake table using SQL INSERT to avoid stage creation issues"""
    try:
        # Parse table name (could be DATABASE.SCHEMA.TABLE or TABLE)
        target_table = table_name.upper()
        
        # Handle both Snowpark Session and connector connection
        if hasattr(session, 'sql'):
            # Snowpark Session - use SQL INSERT with VALUES to avoid stage creation issues
            # Handle overwrite
            if overwrite:
                try:
                    session.sql(f"DROP TABLE IF EXISTS {target_table}").collect()
                except:
                    pass  # Table might not exist, continue
            
            # Create table if it doesn't exist
            columns = []
            for col in df.columns:
                # Infer SQL type from pandas dtype
                if pd.api.types.is_integer_dtype(df[col]):
                    sql_type = "NUMBER"
                elif pd.api.types.is_float_dtype(df[col]):
                    sql_type = "FLOAT"
                elif pd.api.types.is_bool_dtype(df[col]):
                    sql_type = "BOOLEAN"
                elif pd.api.types.is_datetime64_any_dtype(df[col]):
                    sql_type = "TIMESTAMP_NTZ"
                else:
                    sql_type = "VARIANT"  # For complex types like arrays/JSON
                columns.append(f'"{col}" {sql_type}')
            
            create_table_sql = f"CREATE TABLE IF NOT EXISTS {target_table} ({', '.join(columns)})"
            session.sql(create_table_sql).collect()
            
            # Insert data using direct SQL INSERT with VALUES to avoid stage creation
            if len(df) > 0:
                columns_str = ', '.join([f'"{col}"' for col in df.columns])
                
                # Insert in batches to avoid SQL statement length limits
                batch_size = 100
                for i in range(0, len(df), batch_size):
                    batch_df = df.iloc[i:i+batch_size]
                    values_list = []
                    
                    for _, row in batch_df.iterrows():
                        row_values = []
                        for val in row:
                            if pd.isna(val):
                                row_values.append("NULL")
                            elif isinstance(val, (list, dict)):
                                # Convert to JSON string for VARIANT columns
                                json_str = json.dumps(val).replace("'", "''")
                                row_values.append(f"PARSE_JSON('{json_str}')")
                            elif isinstance(val, str):
                                # Escape single quotes in strings
                                val_str = str(val).replace("'", "''")
                                row_values.append(f"'{val_str}'")
                            elif isinstance(val, (int, float)):
                                row_values.append(str(val))
                            else:
                                # For other types, convert to string and escape
                                val_str = str(val).replace("'", "''")
                                row_values.append(f"'{val_str}'")
                        
                        values_list.append(f"({', '.join(row_values)})")
                    
                    # Execute INSERT statement
                    insert_sql = f"INSERT INTO {target_table} ({columns_str}) VALUES {', '.join(values_list)}"
                    session.sql(insert_sql).collect()
            
            return True
        else:
            # Connector connection - use SQL INSERT with parameterized queries
            cursor = session.cursor()
            
            try:
                # Handle overwrite
                if overwrite:
                    cursor.execute(f"DROP TABLE IF EXISTS {target_table}")
                
                # Create table if it doesn't exist
                # Get column types from DataFrame
                columns = []
                for col in df.columns:
                    # Infer SQL type from pandas dtype
                    if pd.api.types.is_integer_dtype(df[col]):
                        sql_type = "NUMBER"
                    elif pd.api.types.is_float_dtype(df[col]):
                        sql_type = "FLOAT"
                    elif pd.api.types.is_bool_dtype(df[col]):
                        sql_type = "BOOLEAN"
                    elif pd.api.types.is_datetime64_any_dtype(df[col]):
                        sql_type = "TIMESTAMP_NTZ"
                    else:
                        sql_type = "VARIANT"  # For complex types like arrays/JSON
                    columns.append(f'"{col}" {sql_type}')
                
                create_table_sql = f"CREATE TABLE IF NOT EXISTS {target_table} ({', '.join(columns)})"
                cursor.execute(create_table_sql)
                
                # Insert data using executemany for better performance
                if len(df) > 0:
                    # Prepare data for insertion
                    placeholders = ', '.join(['?' for _ in df.columns])
                    columns_str = ', '.join([f'"{col}"' for col in df.columns])
                    insert_sql = f"INSERT INTO {target_table} ({columns_str}) VALUES ({placeholders})"
                    
                    # Convert DataFrame rows to list of tuples, handling complex types
                    rows_to_insert = []
                    for _, row in df.iterrows():
                        row_values = []
                        for val in row:
                            if pd.isna(val):
                                row_values.append(None)
                            elif isinstance(val, (list, dict)):
                                # Convert to JSON string for VARIANT columns
                                row_values.append(json.dumps(val))
                            else:
                                row_values.append(val)
                        rows_to_insert.append(tuple(row_values))
                    
                    # Execute in batches
                    batch_size = 1000
                    for i in range(0, len(rows_to_insert), batch_size):
                        batch = rows_to_insert[i:i+batch_size]
                        cursor.executemany(insert_sql, batch)
                
                cursor.close()
                return True
                
            except Exception as insert_error:
                cursor.close()
                raise insert_error
                
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
    thread_id = st.text_input("THREAD_ID (optional)", value="")
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
                        thread_id=thread_id.strip() if thread_id else None,
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
                    thread_id=thread_id.strip() if thread_id else None,
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

