// Databricks notebook source
val fileLocation = "dbfs:/FileStore/tables/BANK_RISK22.csv"
val fileType = "csv"
val data = spark.read.format(fileType) 
  .option("header", "true") //一行目はヘッダ
  .option("inferSchema", "true") //スキーマは自動推測
  .load(fileLocation)
display(data)

// COMMAND ----------

data.write.mode(SaveMode.Overwrite).saveAsTable("userlist") 

// COMMAND ----------

var sdf = spark.sql("""
                     SELECT 
                     concat(
                     '{"Line_number":"', cast(Line_number AS STRING), '",' ,
                      '"customer_number":"',cast(customer_number AS STRING), '",' ,
                      '"loan_request_amount":"',cast(loan_request_amount AS STRING), '",' ,
                      '"unpaid_collateral_amount":"',cast(unpaid_collateral_amount AS STRING), '",' ,
                      '"current_asset_value":"',cast(current_asset_value AS STRING), '",' ,
                      '"reason_for_debt":"',cast(reason_for_debt AS STRING), '",' ,
                      '"debt_to_income_ratio":"',cast(debt_to_income_ratio AS STRING), '",' ,
                      '"number_of_credit_inquiries":"',cast(number_of_credit_inquiries AS STRING), '",' ,
                      '"number_of_credit_lines":"',cast(number_of_credit_lines AS STRING), '",' ,
                      '"years_of_service":"',cast(years_of_service AS STRING), '",' ,
                      '"default_flag":"',cast(default_flag AS STRING), '",' ,
                      '"number_of_delinquent_trade_lines":"',cast(number_of_delinquent_trade_lines AS STRING), '",' ,
                      '"gender":"',cast(gender AS STRING), '",' ,
                      '"district":"',cast(district AS STRING), '",' ,
                      '"Occupation":"',cast(Occupation AS STRING), '",' ,
                      '"age":"',cast(age AS STRING)
                      ,'"}' ) as body 
                     FROM userlist
""")
display(sdf)

// COMMAND ----------

val namespaceName = ""
val eventHubName = ""
val sasKeyName = ""
val sasKey = ""

// COMMAND ----------

import spark.implicits._
import org.apache.spark.sql._
case class clsuser(body:String)
import scala.collection.JavaConverters._
    import com.microsoft.azure.eventhubs._
    import java.util.concurrent._
    import scala.collection.immutable._
    import scala.concurrent.Future
    import scala.concurrent.ExecutionContext.Implicits.global

// COMMAND ----------

val connStr = new ConnectionStringBuilder()
          .setNamespaceName(namespaceName)
          .setEventHubName(eventHubName)
          .setSasKeyName(sasKeyName)
          .setSasKey(sasKey)


val pool = Executors.newScheduledThreadPool(1)
val eventHubClient = EventHubClient.createFromConnectionString(connStr.toString(), pool) //create->createFromConnectionString


def sleep(time: Long): Unit = Thread.sleep(time)
def sendEvent(body: String,delay : Long) = {
  sleep(delay)       
  val messageData = EventData.create(body.getBytes("UTF-8"))
  eventHubClient.get().send(messageData)
  //System.out.println("Sent event: " + body + "\n")
}
val df = sdf
df.as[clsuser].take(df.count.toInt).foreach(t => sendEvent(s"${t.body}",100))

// COMMAND ----------

