from lib import init_spark, read_csv, spark_sql_query, transform
from pyspark.sql import SparkSession, Row


def test_init_spark():
    spark = init_spark(app_name="PySpark Data Processing")
    assert isinstance(spark, SparkSession), "Test failed."
    print("Spark initiation test passed successfully.")


def test_read_csv():
    spark = init_spark(app_name="PySpark Data Processing")
    csv_file_path = "store.csv"
    df = read_csv(spark, csv_file_path)
    print(df)
    assert df.count() > 0, "Test failed."
    print("CSV file reading test passed successfully.")


def test_spark_sql_query():
    # create SparkSession for testing
    spark = SparkSession.builder.appName("Spark SQL Query Test").getOrCreate()
    csv_file_path = "store.csv"
    df = read_csv(spark, csv_file_path)
    result_df = spark_sql_query(spark, df)

    # expected df with stores that have customer satisfaction > 80
    expected_data = [
        Row(store_name="Store A", location="New York", monthly_sales=50000),
        Row(store_name="Store B", location="Los Angeles", monthly_sales=45000),
        Row(store_name="Store D", location="Houston", monthly_sales=52000),
        Row(store_name="Store E", location="Phoenix", monthly_sales=48000),
        Row(store_name="Store F", location="Philadelphia", monthly_sales=55000),
        Row(store_name="Store H", location="San Diego", monthly_sales=31000),
        Row(store_name="Store J", location="San Jose", monthly_sales=47000),
    ]

    expected_df = spark.createDataFrame(expected_data)

    # compare
    assert result_df.collect() == expected_df.collect(), "Test failed."
    print("Spark SQL query test passed successfully.")


def test_transform():
    # create SparkSession for testing
    spark = SparkSession.builder.appName("Transform Test").getOrCreate()

    # Sample data for testing
    sample_data = [
        Row(monthly_sales=25000, customer_satisfaction=65),
        Row(monthly_sales=35000, customer_satisfaction=82),
        Row(monthly_sales=55000, customer_satisfaction=92),
    ]
    df = spark.createDataFrame(sample_data)

    # call function
    result_df = transform(df)

    # Get categories for sales and satisfaction
    sales_categories = [row["sales_category"] for row in result_df.collect()]
    satisfaction_categories = [row["satisfaction_category"] for row in result_df.collect()]

    expected_sales_categories = ["Low", "Medium", "High"]
    expected_satisfaction_categories = ["Poor", "Good", "Excellent"]

    assert sales_categories == expected_sales_categories, "Sales category test failed!"
    assert satisfaction_categories == expected_satisfaction_categories, "Satisfaction category test failed!"

    print("Transform test passed successfully.")

    spark.stop()


if __name__ == "__main__":
    test_init_spark()
    test_read_csv()
    test_spark_sql_query()
    test_transform()
