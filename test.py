from lib import init_spark, read_csv, spark_sql_query, transform
from pyspark.sql import SparkSession, Row


def test_init_spark():
    spark = init_spark(app_name="PySpark Data Processing")
    assert isinstance(spark, SparkSession), "Test failed."
    print("Spark initiation test passed successfully.")


def test_read_csv():
    spark = init_spark(app_name="PySpark Data Processing")
    csv_file_path = "student_performance.csv"
    df = read_csv(spark, csv_file_path)
    print(df)
    assert df.count() > 0, "Test failed."
    print("CSV file reading test passed successfully.")


def test_spark_sql_query():
    # create SparkSession for testing
    spark = SparkSession.builder.appName("Spark SQL Query Test").getOrCreate()
    csv_file_path = "student_performance.csv"
    df = read_csv(spark, csv_file_path)
    result_df = spark_sql_query(spark, df)

    # expected df
    expected_data = [
        Row(name="Sarah", attendance_rate=90),
        Row(name="Michael", attendance_rate=92),
        Row(name="Emma", attendance_rate=88),
        Row(name="Olivia", attendance_rate=95),
        Row(name="Isabella", attendance_rate=91),
    ]

    expected_df = spark.createDataFrame(expected_data)

    # compare
    assert result_df.collect() == expected_df.collect(), "Test failed."
    print("Spark SQL query test passed successfully.")


def test_transform():
    # create SparkSession for testing
    spark = SparkSession.builder.appName("Add Attendance Category Test").getOrCreate()

    # Sample data for testing
    sample_data = [
        Row(attendance_rate=40),
        Row(attendance_rate=60),
        Row(attendance_rate=90),
    ]
    df = spark.createDataFrame(sample_data)

    # call function
    result_df = transform(df)

    categories = [row["attendance_category"] for row in result_df.collect()]
    expected_categories = ["Low", "Medium", "High"]

    assert categories == expected_categories, "Test failed!"

    print("Transform test passed successfully.")

    spark.stop()


if __name__ == "__main__":
    test_init_spark()
    test_read_csv()
    test_spark_sql_query()
    test_transform()
