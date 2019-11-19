// Databricks notebook source
import org.apache.spark.eventhubs._
    import com.microsoft.azure.eventhubs._

// COMMAND ----------

import org.apache.spark.eventhubs._
    import com.microsoft.azure.eventhubs._

    // Build connection string with the above information
    val namespaceName = ""
    val eventHubName = ""
    val sasKeyName = ""
    val sasKey = "="

    val connStr = new com.microsoft.azure.eventhubs.ConnectionStringBuilder()
                .setNamespaceName(namespaceName)
                .setEventHubName(eventHubName)
                .setSasKeyName(sasKeyName)
                .setSasKey(sasKey)

    val customEventhubParameters =
      EventHubsConf(connStr.toString())
      .setMaxEventsPerTrigger(5)

    val incomingStream = spark.readStream.format("eventhubs").options(customEventhubParameters.toMap).load()

    //incomingStream.printSchema

    // Sending the incoming stream into the console.
    // Data comes in batches!
    //incomingStream.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()

import org.apache.spark.sql.types._
    import org.apache.spark.sql.functions._

    // Event Hub message format is JSON and contains "body" field
    // Body is binary, so we cast it to string to see the actual content of the message
    val messages =
      incomingStream
      .withColumn("Offset", $"offset".cast(LongType))
      .withColumn("Time (readable)", $"enqueuedTime".cast(TimestampType))
      .withColumn("Timestamp", $"enqueuedTime".cast(LongType))
      .withColumn("body", $"body".cast(StringType))
      .select("Offset", "Time (readable)", "Timestamp", "body")
    
    messages.printSchema
    //messages.writeStream.outputMode("append").format("console").option("truncate", false).start().awaitTermination()
    //import org.apache.spark.sql.streaming.Trigger.ProcessingTime
//
    //val result =
    //  incomingStream
    //    .writeStream
    //    .format("parquet")
    //    .option("path", "/mnt/DatabricksSentimentPowerBI")
    //    .option("checkpointLocation", "/mnt/sample/check2")
    //    .start()
import org.apache.spark.eventhubs.{ ConnectionStringBuilder, EventHubsConf, EventPosition }
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

// The connection string for the Event Hub you will WRTIE to. 
val connString = "Endpoint="   
val eventHubsConfWrite = EventHubsConf(connString)



    val query = 
      messages
        .writeStream
        .format("eventhubs")
        .outputMode("update")
        .options(eventHubsConfWrite.toMap)
        .trigger(ProcessingTime("25 seconds"))
        .option("checkpointLocation", "/checkpoint/")
        .start()

// COMMAND ----------

