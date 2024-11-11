from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F


def init_spark(app_name: str, memory: str = "2g") -> SparkSession:
    session = (
        SparkSession.builder.appName(app_name)
        .config("spark.executor.memory", memory)
        .getOrCreate()
    )
    return session


def read_csv(session: SparkSession, file_path: str) -> DataFrame:
    data_file = session.read.csv(file_path, header=True, inferSchema=True)
    return data_file


def spark_sql_query(spark: SparkSession, data: DataFrame):
    data.createOrReplaceTempView("sales_data")
    result = spark.sql(
        """
        SELECT store_name, location, monthly_sales
        FROM sales_data
        WHERE customer_satisfaction > 80
        """
    )
    result.show()
    return result


def transform(data: DataFrame) -> DataFrame:
    sales_conditions = [
        (F.col("monthly_sales") < 30000, "Low"),
        ((F.col("monthly_sales") >= 30000) & (F.col("monthly_sales") < 50000), "Medium"),
        (F.col("monthly_sales") >= 50000, "High"),
    ]

    satisfaction_conditions = [
        (F.col("customer_satisfaction") < 75, "Poor"),
        ((F.col("customer_satisfaction") >= 75) & (F.col("customer_satisfaction") < 90), "Good"),
        (F.col("customer_satisfaction") >= 90, "Excellent"),
    ]

    data = data.withColumn(
        "sales_category",
        F.when(sales_conditions[0][0], sales_conditions[0][1])
        .when(sales_conditions[1][0], sales_conditions[1][1])
        .otherwise(sales_conditions[2][1]),
    )

    data = data.withColumn(
        "satisfaction_category",
        F.when(satisfaction_conditions[0][0], satisfaction_conditions[0][1])
        .when(satisfaction_conditions[1][0], satisfaction_conditions[1][1])
        .otherwise(satisfaction_conditions[2][1]),
    )

    return data
