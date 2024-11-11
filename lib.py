from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F


def init_spark(app_name: str, memory: str = "2g") -> SparkSession:
    # init spark session
    session = (
        SparkSession.builder.appName(app_name)
        .config("session.executor.memory", memory)
        .getOrCreate()
    )
    return session


def read_csv(session: SparkSession, file_path: str) -> DataFrame:
    # read csv
    data_file = session.read.csv(file_path, header=True, inferSchema=True)
    return data_file


def spark_sql_query(spark: SparkSession, data: DataFrame):
    # create a temporary view for querying
    data.createOrReplaceTempView("performance")

    # execute query
    result = spark.sql(
        """
        SELECT name, attendance_rate
        FROM performance
        WHERE final_grade > 80
    """
    )
    result.show()
    return result


def transform(data: DataFrame) -> DataFrame:
    conditions = [
        (F.col("attendance_rate") < 50, "Low"),
        ((F.col("attendance_rate") >= 50) & (F.col("attendance_rate") < 80), "Medium"),
        (F.col("attendance_rate") >= 80, "High"),
    ]

    return data.withColumn(
        "attendance_category",
        F.when(conditions[0][0], conditions[0][1])
        .when(conditions[1][0], conditions[1][1])
        .otherwise(conditions[2][1]),
    )
