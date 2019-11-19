// Databricks notebook source
// MAGIC %md
// MAGIC #TwitterストリームデータをDatabricksでEvent Hubsに送信

// COMMAND ----------

// MAGIC %md
// MAGIC ![](https://docs.microsoft.com/ja-jp/azure/azure-databricks/media/databricks-stream-from-eventhubs/databricks-eventhubs-tutorial.png)
// MAGIC 
// MAGIC [参考リンク](https://docs.microsoft.com/ja-jp/azure/azure-databricks/databricks-stream-from-eventhubs)

// COMMAND ----------

// MAGIC %md
// MAGIC ##送信先のEvent Hub設定

// COMMAND ----------

val namespaceName = ""
val eventHubName = ""
val sasKeyName = ""
val sasKey = ""

// COMMAND ----------

// MAGIC %md
// MAGIC ##入力元のTwitter Application設定

// COMMAND ----------

val twitterConsumerKey = ""
val twitterConsumerSecret = ""
val twitterOauthAccessToken = ""
val twitterOauthTokenSecret = ""

// COMMAND ----------

import scala.collection.JavaConverters._
    import com.microsoft.azure.eventhubs._
    import java.util.concurrent._
    import scala.collection.immutable._
    import scala.concurrent.Future
    import scala.concurrent.ExecutionContext.Implicits.global

    val connStr = new ConnectionStringBuilder()
                .setNamespaceName(namespaceName)
                .setEventHubName(eventHubName)
                .setSasKeyName(sasKeyName)
                .setSasKey(sasKey)


    val pool = Executors.newScheduledThreadPool(1)
    val eventHubClient = EventHubClient.createFromConnectionString(connStr.toString(), pool) //create->createFromConnectionString

    def sleep(time: Long): Unit = Thread.sleep(time)

    def sendEvent(user : String , message: String,createatUTC: String,lang: String) = {
      //sleep(delay)
      val jsonmsg ="{\"user\":\""+ user + "\", \"message\":\""+ message + "\", \"createatUTC\":\""+ createatUTC + "\", \"lang\":\""+ lang + "\"}";
      val messageData = EventData.create(jsonmsg.getBytes("UTF-8"))
      eventHubClient.get().send(messageData)
      System.out.println(jsonmsg)   
    }

    // Add your own values to the list
    val testSource = List("Azure is the greatest!", "Azure isn't working :(", "Azure is okay.")

    // Specify 'test' if you prefer to not use Twitter API and loop through a list of values you define in `testSource`
    // Otherwise specify 'twitter'
    val dataSource = "twitter"

    if (dataSource == "twitter") {

      import twitter4j._
      import twitter4j.TwitterFactory
      import twitter4j.Twitter
      import twitter4j.conf.ConfigurationBuilder

      val cb = new ConfigurationBuilder()
        cb.setDebugEnabled(true)
        .setOAuthConsumerKey(twitterConsumerKey)
        .setOAuthConsumerSecret(twitterConsumerSecret)
        .setOAuthAccessToken(twitterOauthAccessToken)
        .setOAuthAccessTokenSecret(twitterOauthTokenSecret)

      val twitterFactory = new TwitterFactory(cb.build())
      val twitter = twitterFactory.getInstance()

      // Getting tweets with keyword "Azure" and sending them to the Event Hub in realtime!
      val query = new Query(" #Finance OR #Fintech OR #Money OR Bank OR 銀行 OR SBIホールディングス OR 地銀 OR 預かり資産 OR みずほ OR SMBC")
      query.setCount(100)
      //query.lang("ja")
      var finished = false
      
      import java.text.ParseException;
      import java.text.SimpleDateFormat;
      import java.util.Date;
      while (!finished) {
        val result = twitter.search(query)
        val statuses = result.getTweets()
        var lowestStatusId = Long.MaxValue
        var maxestStatusId = Long.MaxValue        
        for (status <- statuses.asScala) {
          if(!status.isRetweet()){
            
            var dater =status.getCreatedAt()
            var dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");         
            var strDate = dateFormat.format(dater);
            var user = status.getUser().getScreenName()
            if(status.getId() != maxestStatusId){
              sendEvent(user,status.getText(),strDate,status.getLang())
            }
          }
          //lowestStatusId = Math.min(status.getId(), lowestStatusId)
          maxestStatusId = Math.max(status.getId(), maxestStatusId)
        }
        sleep(30000)
//        query.setMaxId(lowestStatusId - 1)
        query.setSinceId(maxestStatusId + 1)
        System.out.println(maxestStatusId)
      }

    } else if (dataSource == "test") {
      // Loop through the list of test input data
      while (true) {

      }

    } else {
      System.out.println("Unsupported Data Source. Set 'dataSource' to \"twitter\" or \"test\"")
    }

    // Closing connection to the Event Hub
    eventHubClient.get().close()

// COMMAND ----------

