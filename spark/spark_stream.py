import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, trim, lower, initcap,
    to_timestamp, regexp_replace
)
from pyspark.sql.types import StructType, StructField, StringType


# ===============================
# SPARK SESSION
# ===============================
def create_spark_connection():
    spark = (
        SparkSession.builder
        .appName("KafkaToPostgres")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


# ===============================
# READ FROM KAFKA
# ===============================
def read_from_kafka(spark):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:29092")
        .option("subscribe", "users_created")
        .option("startingOffsets", "earliest")
        .load()
    )


# ===============================
# PARSE JSON
# ===============================
def parse_kafka_value(kafka_df):
    schema = StructType([
        StructField("first_name", StringType()),
        StructField("last_name", StringType()),
        StructField("gender", StringType()),
        StructField("address", StringType()),
        StructField("postal_code", StringType()),
        StructField("email", StringType()),
        StructField("username", StringType()),
        StructField("registered_date", StringType()),
        StructField("phone", StringType()),
        StructField("picture", StringType())
    ])

    return (
        kafka_df
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(from_json(col("json_value"), schema).alias("data"))
        .select("data.*")
    )


# ===============================
# CLEAN DATA
# ===============================
def clean_user_data(df):
    clean_df = (
        df
        .withColumn("first_name", initcap(trim(col("first_name"))))
        .withColumn("last_name", initcap(trim(col("last_name"))))
        .withColumn("gender", lower(trim(col("gender"))))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("username", trim(col("username")))
        .withColumn("address", trim(col("address")))
        .withColumn("postal_code", trim(col("postal_code")))
        .withColumn("phone", regexp_replace(col("phone"), "[^0-9]", ""))
        .withColumn("registered_date", to_timestamp(col("registered_date")))
    )

    # Validation
    clean_df = (
        clean_df
        .filter(col("email").isNotNull())
        .filter(col("first_name").isNotNull())
        .filter(col("email").rlike("^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+$"))
    )

    # Deduplicate trong batch (OK)
    return clean_df.dropDuplicates(["email"])


# ===============================
# WRITE TO POSTGRES
# ===============================
def write_to_postgres(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        print(f"Batch {batch_id} empty - skip")
        return

    print(f"Writing batch {batch_id}")

    (
        batch_df.write
        .format("jdbc")
        .mode("append")
        .option("url", "jdbc:postgresql://postgres:5432/airflow")
        .option("dbtable", "created_users")
        .option("user", "airflow")
        .option("password", "airflow")
        .option("driver", "org.postgresql.Driver")
        .save()
    )

    print(f"✅ Batch {batch_id} written")


# ===============================
# MAIN
# ===============================
if __name__ == "__main__":
    spark = create_spark_connection()

    kafka_df = read_from_kafka(spark)
    parsed_df = parse_kafka_value(kafka_df)
    clean_df = clean_user_data(parsed_df)

    query = (
        clean_df.writeStream
        .foreachBatch(write_to_postgres)
        .outputMode("append")
        .option("checkpointLocation", "/opt/spark/checkpoints/users_created")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()
