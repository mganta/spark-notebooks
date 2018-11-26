// Databricks notebook source
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types._

val schema = new StructType().add("symbol", StringType).add("timestamp", TimestampType).add("bid_price", FloatType).add("trade_type", StringType).add("order_quantity", IntegerType)

val eventhubParameters = Map[String, String] (
      "eventhubs.policyname" -> "RootManageSharedAccessKey",
      "eventhubs.policykey" -> "ljkjk",
      "eventhubs.namespace" -> "spearfishstream",
      "eventhubs.name" -> "stockstream",
      "eventhubs.partition.count" -> "2",
      "eventhubs.consumergroup" -> "$Default",
      "eventhubs.progressTrackingDir" -> "dbfs:/eventhub-checkpoint",
      "eventhubs.maxRate" -> s"20",
      "eventhubs.sql.containsProperties" -> "true",
      "eventhubs.sql.userDefinedKeys" -> "creationTime,randomUserProperty" 
    )

// COMMAND ----------

 val df = spark.readStream
    .format("eventhubs")
    .options(eventhubParameters)
   .load()



// COMMAND ----------

  val streamingDataFrame = df.selectExpr("cast (body as string) as json").select(from_json($"json", schema=schema).as("stockdata")).select("stockdata.*")

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

