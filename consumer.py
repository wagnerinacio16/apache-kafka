
from pyspark.sql import SparkSession
from pyspark.sql import functions as f

def main():
    spark = (
        SparkSession.builder
        .appName("ecommerce-orders-streaming-consumer")
        .getOrCreate()
    )

    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "ecommerce.orders.events")
        .option("kafka.group.id", "spark_orders_consumer")
        .option("startingOffsets", "earliest")
        .load()
    )

    df_raw = df_kafka.select(
        f.col("key").cast("string").alias("key"),
        f.col("value").cast("string").alias("json_value"),
        f.col("topic"),
        f.col("partition"),
        f.col("offset"),
        f.col("timestamp")
    )

    query = (
        df_raw.writeStream
        .format("console")
        .option("checkpointLocation", "./checkpoints/ecommerce_orders")
        .outputMode("append")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()