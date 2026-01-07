
import streamlit as st
import pandas as pd
import os
from databricks_connector import DatabricksConnector
from spark_processor_databricks import SparkProcessorDatabricks
import config

st.set_page_config(
    page_title="Cloud Data Processing",
    page_icon="☁️",
    layout="wide"
)

if 'code_generated' not in st.session_state:
    st.session_state.code_generated = False
if 'selected_dataset' not in st.session_state:
    st.session_state.selected_dataset = config.DATASET_PATH

st.title("Cloud-Based Data Processing Service")
st.markdown("### Powered by Databricks and Apache Spark")
st.markdown("---")

with st.sidebar:
    st.title("Project Information")
    st.markdown("---")

    st.subheader("Team Members")
    st.write("- 𝐍𝐨𝐨𝐫 𝐀𝐥𝐣𝐨𝐮𝐫𝐚𝐧𝐢")
    st.write("- Hayat Zandah")


    st.markdown("---")

    st.subheader("Course Details")
    st.write("Cloud and Distributed Systems")
    st.write("SICT 4313")

    st.markdown("---")

    st.subheader("Connection Status")
    connector = DatabricksConnector(config.DATABRICKS_HOST, config.DATABRICKS_TOKEN)
    if connector.test_connection():
        st.success("Connected")
    else:
        st.error("Connection Failed")

    st.markdown("---")
    st.caption("Islamic University of Gaza - 2025")

st.header("Step 1: Select Dataset")

dataset_choice = st.radio(
    "Choose your dataset:",
    ["Databricks Sample Dataset (Flights)", "Upload Your Own CSV"],
    index=0
)

if dataset_choice == "Databricks Sample Dataset (Flights)":
    st.success("Using Flight Delays Dataset")

    col1, col2 = st.columns(2)
    with col1:
        st.metric("Dataset Size", "1.6M rows")
        st.metric("Columns", "5")
    with col2:
        st.metric("Type", "CSV")
        st.metric("Location", "DBFS")



    st.session_state.selected_dataset = "/databricks-datasets/flights/departuredelays.csv"
    dataset_ready = True

else:
    uploaded_file = st.file_uploader("Upload CSV file", type=['csv'])

    if uploaded_file:
        temp_path = config.get_local_path(uploaded_file.name)
        with open(temp_path, 'wb') as f:
            f.write(uploaded_file.getbuffer())

        st.success(f"Uploaded: {uploaded_file.name}")

        df_preview = pd.read_csv(temp_path, nrows=5)
        st.dataframe(df_preview)

        st.warning("Upload this file to Databricks DBFS first")
        dataset_ready = True
    else:
        st.info("Upload a CSV file to continue")
        dataset_ready = False

if dataset_ready:


    st.markdown("---")

    if st.button("Generate Complete Code", type="primary", width='stretch'):
        with st.spinner("Generating code..."):
            processor = SparkProcessorDatabricks(st.session_state.selected_dataset)
            complete_code = processor.generate_complete_code()
            st.session_state.generated_code = complete_code
            st.session_state.code_generated = True

        st.success("Code generated successfully")
        st.balloons()

    if st.session_state.code_generated:
        st.markdown("---")
        st.header("Step 4: Your Complete Spark Code")

        st.success("Ready to use - Copy and paste into Databricks")

        with st.expander("How to Use This Code", expanded=False):
            st.markdown("""
            ### Quick Steps:

            1. Open Databricks
               - Go to: https://dbc-957aa73b-d7e0.cloud.databricks.com

            2. Create New Notebook
               - Workspace > Your Name > New > Notebook
               - Name: Cloud_Project
               - Language: Python

            3. Copy and Paste
               - Copy the code below
               - Paste into the notebook cell

            4. Run
               - Press Shift + Enter
               - Wait for completion (2-5 minutes)

            5. Download Results
               - Go to: Data > Browse DBFS > FileStore
               - Download all files with the timestamp
               - Files include: text report, CSV tables, performance chart

            6. Calculate Performance
               - Use the performance table for your report
               - Include charts in documentation
               - Discuss scalability findings
            """)

        st.subheader("Complete Code")
        st.code(st.session_state.generated_code, language='python')

        col1, col2, col3 = st.columns([1, 1, 1])

        with col2:
            st.download_button(
                label="Download Code",
                data=st.session_state.generated_code,
                file_name="spark_complete_analysis.py",
                mime="text/plain",
                width='stretch'
            )

        st.markdown("---")
        st.header("Step 5: Record Your Results")

        st.write("After running the code, you will get:")

        results_template = pd.DataFrame({
            'Workers': [1, 2, 4, 8],
            'Execution Time (s)': ['Actual', 'Projected', 'Projected', 'Projected'],
            'Speedup': ['1.0', 'Calculated', 'Calculated', 'Calculated'],
            'Efficiency (%)': ['100', 'Calculated', 'Calculated', 'Calculated']
        })

        st.dataframe(results_template, width='stretch')

        st.info("""
        How to calculate:
        - Speedup = Time(1 worker) / Time(N workers)
        - Efficiency = (Speedup / N workers) x 100%

        Example:
        - Time with 1 worker = 120s
        - Time with 4 workers = 35s (projected)
        - Speedup = 120 / 35 = 3.43
        - Efficiency = (3.43 / 4) x 100% = 85.7%
        """)

        st.markdown("---")
        st.header("Next Steps")

        col1, col2 = st.columns(2)

        with col1:
            st.subheader("For the Project:")
            st.write("""
            - Run code on Databricks
            - Download all result files
            - Take screenshots
            - Create performance graphs
            - Document findings
            """)

        with col2:
            st.subheader("For the Report:")
            st.write("""
            - Include methodology
            - Add performance tables
            - Show scalability analysis
            - Discuss results
            - Add GitHub link
            """)

        st.markdown("---")
        st.subheader("Quick Links")

        col1, col2, col3 = st.columns(3)

        with col1:
            st.link_button(
                "Open Databricks",
                "https://dbc-957aa73b-d7e0.cloud.databricks.com",
                width='stretch'
            )

        with col2:
            if st.button("Generate New Code", width='stretch'):
                st.session_state.code_generated = False
                st.rerun()

        with col3:
            st.link_button(
                "Databricks Docs",
                "https://docs.databricks.com",
                width='stretch'
            )

st.markdown("---")
st.markdown("""

""", unsafe_allow_html=True)