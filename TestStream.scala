// Databricks notebook source
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._

val schema = new StructType().add("symbol", StringType).add("timestamp", TimestampType).add("bid_price", FloatType).add("trade_type", StringType).add("order_quantity", IntegerType)

//test

// COMMAND ----------

 val df = spark.readStream
    .format("kafka")
   .option("kafka.bootstrap.servers", "testkafka.westus.cloudapp.azure.com:9092")
   .option("subscribe", "stream")
   .option("startingOffsets", "latest")
   .load()

// COMMAND ----------

  val streamingDataFrame = df.selectExpr("cast (value as string) as json").select(from_json($"json", schema=schema).as("stockdata")).select("stockdata.*")

// COMMAND ----------

  val streamingCountsDF =
    streamingDataFrame
      .groupBy(window($"timestamp", "2 minutes"), $"symbol")
      .count()

// COMMAND ----------

streamingCountsDF.isStreaming

// COMMAND ----------

val query2 =
  streamingCountsDF
    .writeStream
    .format("memory")        // memory = store in-memory table (for testing only in Spark 2.0)
    .queryName("counts")     // counts = name of the in-memory table
    .outputMode("complete")  // complete = all the counts should be in the table
    .start()

// COMMAND ----------

  streamingDataFrame.writeStream
    .format("parquet")
    .option("path", "dbfs:/stockstream-output/")
    .option("checkpointLocation", "dbfs:/stockstream-checkpoint/")
    .start()

// COMMAND ----------

spark.sql("select * from counts").show()

// COMMAND ----------

sql("select count(*) from counts")

// COMMAND ----------

  streamingDataFrame.writeStream
    .format("parquet")
    .option("path", "dbfs:/stockstream-output/")
    .option("checkpointLocation", "dbfs:/stockstream-checkpoint/")
    .start()

// COMMAND ----------

// MAGIC %sql select * from counts

// COMMAND ----------

// MAGIC %fs ls dbfs:/stockstream-checkpoint/

// COMMAND ----------

// MAGIC %sh telnet testkafka.westus.cloudapp.azure.com 9092

// COMMAND ----------

// MAGIC %sh ping 104.40.85.204

// COMMAND ----------

