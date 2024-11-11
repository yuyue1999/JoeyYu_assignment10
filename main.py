from lib import init_spark, read_csv, spark_sql_query, transform


if __name__ == "__main__":
    spark = init_spark(app_name="PySpark Data Processing")

    csv_file_path = "student_performance.csv"
    df = read_csv(spark, csv_file_path)

    # show original
    print("Original Data:")
    df.show()

    # show queried data
    print("Data After Spark SQL Query:")
    spark_sql_query(spark, df)

    # show data after categorized
    df_with_category = transform(df)
    print("Data After Adding Attendance Category:")
    df_with_category.show()
