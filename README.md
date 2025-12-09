# AI Evaluation Dataset Builder for Cortex Agents

A Streamlit application to build evaluation datasets for (**Snowflake Cortex Agent Evaluations**)[https://docs.snowflake.com/LIMITEDACCESS/cortex-agent-evaluations] (Private Preview). This tool helps you create, manage, and export evaluation datasets by combining agent observability logs with manually created test cases.

## Overview

This tool streamlines the process of building high-quality evaluation datasets for testing and improving your Cortex Agents. It supports the new Cortex Agent Evaluations capability in Snowflake, allowing you to:

1. **Load existing agent logs** from observability events
2. **Manually create test cases** with expected tool invocations and responses
3. **Edit and refine** evaluation records with a form-based interface
4. **Export datasets** to Snowflake tables in the proper format for agent evaluation

## Key Features

### 📥 Load from Agent Logs
- Query agent observability events from Snowflake Intelligence
- Filter by agent name, record ID, or user feedback
- Automatically extract queries, responses, and tool calling sequences
- Preview and verify loaded data before proceeding

### ➕ Manual Record Creation
- Form-based interface for creating custom evaluation records
- Define input queries and expected agent responses
- Configure expected tool invocations with proper sequencing
- Support for multiple tool types: SQL, Cortex Search, Custom tools
- Add multiple records incrementally to your dataset

### ✏️ Review & Edit
- Browse all records in your dataset
- Form-based editing (not raw dataframe editor!)
- Edit queries, responses, and tool configurations
- Delete unwanted records
- Robust null value handling

### 📤 Export Dataset
- Export to Snowflake with proper schema:
  - `INPUT_QUERY` as VARCHAR
  - `EXPECTED_TOOLS` as VARIANT (following expected schema with `ground_truth_invocations` and `ground_truth_output` keys)
- Append or overwrite modes
- Download as CSV for local use
- Verification of record counts after export

## Setup

### Option 1: Run in Streamlit in Snowflake

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Environment Variables

Create a `.env` file in the `agent_eval_assistant` directory or set environment variables:

```bash
SNOWFLAKE_ACCOUNT=your_account.us-east-1
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_WAREHOUSE=COMPUTE_WH  # Optional, defaults to COMPUTE_WH
SNOWFLAKE_DATABASE=SNOWFLAKE    # Optional, defaults to SNOWFLAKE
SNOWFLAKE_SCHEMA=LOCAL          # Optional, defaults to LOCAL
SNOWFLAKE_ROLE=ACCOUNTADMIN     # Optional, defaults to ACCOUNTADMIN
```

### 3. Run the Application

```bash
streamlit run agent_observability_app.py
```

The app will open in your browser at `http://localhost:8501`

## Usage

1. **Connect to Snowflake**: Click "Connect to Snowflake" in the sidebar
2. **Set Filters** (optional):
   - Enter an `AGENT_NAME` to filter by specific agent
   - Enter a `THREAD_ID` to filter by specific thread
   - Specify `USER_FEEDBACK` values to filter by user feedback
   - Leave both empty to see all results
3. **Run Query**: Click the "Run Query" button to execute
4. **View Results**: Results are displayed in an interactive DataFrame
5. **Download CSV**: Use the "Download CSV" button to save results locally
6. **Write to Table** (optional):
   - Enter a table name in format `DATABASE.SCHEMA.TABLE_NAME` or just `TABLE_NAME`
   - Click "Write to Table" to save results to Snowflake

## Query Details

The application runs a complex query that:
- Aggregates observability events by thread and message ID
- Extracts agent planning, tool calls, SQL queries, and responses
- Groups related events together for easier analysis
- Supports filtering by agent name and thread ID

## Notes

- Query results are displayed in an interactive table
- When writing to Snowflake, the app will auto-create tables if they don't exist
- The connection is maintained in session state for better performance

