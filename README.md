# Cloud-Based Distributed Data Processing Service

This project is a cloud-based distributed data processing service that allows users to upload or select datasets, generate PySpark code, and perform large-scale data analytics and machine learning using Apache Spark on Databricks.

The system provides a user-friendly web interface built with Streamlit, while all distributed processing and machine learning tasks are executed on Databricks using Apache Spark.

---

## Features

- User-friendly web interface (Streamlit)
- Dataset selection (Databricks sample dataset or CSV upload)
- Automatic generation of PySpark code
- Descriptive statistics on large datasets
- Multiple distributed machine learning jobs using Spark MLlib
- Performance analysis with speedup and efficiency (Amdahl’s Law)
- Cloud-based execution using Databricks

---

## Technologies Used

- Python
- Streamlit
- Apache Spark (PySpark)
- Databricks (Cloud Platform)
- Pandas (for reporting and visualization support)

---

## System Architecture

The system follows a modular architecture:

1. **Streamlit Web Interface**
   - Runs as a cloud-deployed web application
   - Allows users to select or upload datasets
   - Generates PySpark code based on user selections

2. **Databricks + Apache Spark**
   - Executes the generated PySpark code
   - Performs distributed data processing and machine learning
   - Displays analytics and performance results

> Note: The Streamlit interface generates code only. All distributed processing is performed on Databricks.

---

## How to Use

1. Open the web application: https://cloud-data-processing-service1-jycpet8dd3z8zgnfruttjf.streamlit.app/

2. Select a dataset:
- Use a Databricks sample dataset, or
- Upload your own CSV file

3. Generate the PySpark code from the web interface.

4. Open Databricks and create a new Python notebook.

5. Paste the generated PySpark code into the notebook.

6. Run the notebook to:
- Compute descriptive statistics
- Execute machine learning jobs
- Analyze performance and scalability

---

## Machine Learning Jobs

The system performs the following distributed machine learning tasks using Spark MLlib:

- Linear Regression
- KMeans Clustering
- Time Series Aggregation
- Classification (Logistic Regression)

---

## Performance Analysis

- Execution time is measured on a single worker.
- Speedup and efficiency for 2, 4, and 8 workers are estimated using Amdahl’s Law.
- Results demonstrate scalability behavior in distributed systems.

---

## Deployment

- The web interface is deployed using **Streamlit Community Cloud**.
- Distributed processing is executed on **Databricks Serverless**.

---

## Repository Structure

├── app.py
├── config.py
├── databricks_connector.py
├── README.md
├── requirements.txt
├── spark_processor_databricks.py


---

## Demo & Links

- 🌐 Web Application: https://cloud-data-processing-service1-jycpet8dd3z8zgnfruttjf.streamlit.app/
- 📦 GitHub Repository: https://github.com/naro04/cloud-data-processing-service1.git
- 🎥 Video Demonstration: https://youtu.be/qJQTzoiwOlo

---

## Author

- Name: Noor Aljourani , Hayat Zendah
- Course: Cloud and Distributed Systems
- University: Islamic University of Gaza
