package ru.ecomon62.stream2

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}

object Main extends App {

  import org.apache.spark.sql.SparkSession


  val spark = SparkSession.builder()
    .master("local")
    .appName("ecomon62-stream")
    .getOrCreate();

  spark.sparkContext.setLogLevel("WARN")

  val kafka_servers = s"${args(0)}:9092" // 130.193.58.202
  val kafka_topic = "ecomon62"

  val df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_servers)
    .option("subscribe", kafka_topic)
    .option("failOnDataLoss", "false")
    .option("stopGracefullyOnShutdown", "true")
    .option("startingOffsets", "latest") // earliest
    .load()

  val schema = new StructType()
    .add("batch",
      ArrayType(
        new StructType().add("action", StringType)
          .add("number", StringType)
          .add("place",
            new StructType().add("shopNumber", StringType)
              .add("shiftNumber", StringType)
              .add("purchaseNumber", StringType)
              .add("cashNumber", StringType)
          )
          .add("clientInfo",
            new StructType().add("externalUid", StringType)
          )
          .add("usedDate", StringType )
          .add("issueDate", StringType )
          .add("categoryId", StringType )
      )
    )


  val se = df.select(
    //    col("key").cast("string"),
    //    col("partition").cast("string"),
    //    col("offset").cast("long"),
    col("timestamp").cast("timestamp"),
    col("value").cast("string")
  )

  import org.apache.spark.sql.streaming.Trigger

  val query = se.writeStream
    .format("console")
    .trigger(Trigger.ProcessingTime(3000L))
//    .option("path", "/home/otus_hw/spark-app/tmp/")
//    .option("checkpointLocation", "/home/otus_hw/spark-app/tmp/")
    .outputMode("append")
    .start()
  query.awaitTermination()


}
