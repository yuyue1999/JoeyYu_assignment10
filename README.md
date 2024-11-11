![CI](https://github.com/yuyue1999/JoeyYu_assignment10/actions/workflows/CI.yml/badge.svg)
# PySpark Data Processing Project

## Project Overview

This project demonstrates data processing using PySpark, focusing on SQL queries and data transformations. We analyze a dataset on store sales and customer satisfaction, performing transformations and SQL-based filtering to derive insights. This project is implemented on Databricks Community Edition, allowing a free PySpark environment, or can be run locally with Spark.

## Setup Instructions

### Option 1: Using Databricks Community Edition

1. Sign up at [Databricks Community Edition](https://community.cloud.databricks.com/) (select **Personal Use**).
2. Once logged in, create a new notebook and upload the provided Python script or copy and paste the code into the notebook.
3. Run each cell sequentially to load the dataset, perform transformations, and view results.

### Option 2: Running Spark Locally

1. Install Apache Spark and ensure it’s correctly configured with your Python environment.
2. Run the provided Python script (`python your_script_name.py`) in your terminal.

### Option 3: Using GitHub Codespaces

1. Fork the [Python Ruff Template](https://github.com/nogibjj/python-ruff-template) to your GitHub account and start a Codespace.
2. Verify that Spark is installed by running `spark-shell` or `pyspark` in the terminal.
3. Upload the provided Python script to the Codespace and execute it.

## Project Requirements

This project performs the following:

1. **Data Loading**: Loads a CSV dataset with store information, including monthly sales and customer satisfaction ratings.
2. **Spark SQL Query**: Filters stores with a customer satisfaction rating above 80.
3. **Data Transformations**:
   - **Sales Category**: Classifies stores based on monthly sales (Low, Medium, High).
   - **Customer Satisfaction Category**: Classifies stores based on satisfaction levels (Poor, Good, Excellent).
   
### Code Overview

- **`init_spark`**: Initializes a Spark session with specified memory allocation.
- **`read_csv`**: Loads the CSV file into a Spark DataFrame.
- **`spark_sql_query`**: Filters for stores with customer satisfaction above 80 using Spark SQL.
- **`transform`**: Adds `sales_category` and `satisfaction_category` columns to classify stores based on sales and satisfaction ratings.

### Grading Criteria

1. **Data Processing Functionality** (20 points): Successfully loads and processes data from CSV.
2. **Use of Spark SQL and Transformations** (20 points): Performs SQL filtering and data transformations as specified.
3. **CI/CD Pipeline** (10 points): Ensures code integrity and ease of deployment.
4. **README.md** (10 points): Clear and comprehensive documentation.

## Deliverables

- **PySpark Script**: The Python script containing the data processing code.
- **Output Data or Summary Report**: A summary of results in markdown or PDF format.

## Running Tests

To validate the code functionality, you can use `pytest` with test cases defined for initializing Spark, reading CSV, applying SQL filters, and verifying transformations.

```bash
python -m pytest test.py
```

This command will run all tests and display results, ensuring the code performs as expected.

## Submission

Submit the project as a public GitHub repository with the following:

1. **PySpark script**.
2. **Output data or a summary report** (e.g., `output_summary.md`).
3. **CI/CD Pipeline Configuration** (if applicable).
4. **README.md** (this document).

## Example Data and Output

### Example Dataset

The dataset should include columns similar to:

```csv
store_id,store_name,location,monthly_sales,customer_satisfaction
1,Store A,New York,50000,85
2,Store B,Los Angeles,45000,90
...
```

### Example Output

An example output from the SQL query:

```
+----------+------------+-------------+
|store_name|    location|monthly_sales|
+----------+------------+-------------+
|   Store A|    New York|        50000|
|   Store B| Los Angeles|        45000|
...
```
