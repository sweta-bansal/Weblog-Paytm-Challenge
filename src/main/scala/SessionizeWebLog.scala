import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import org.apache.spark.sql.{Dataset, SparkSession}

object SessionizeWebLog {

  def main(args:Array[String]): Unit ={


    val resourcesPath = getClass.getResource("/2015_07_22_mktplace_shop_web_log_sample.log.gz").getPath
    val spark=SparkSession.builder().master("local").appName("WebLog").getOrCreate()
    import spark.implicits._
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)

    //reaidng input data as dataset. Define the schema SessionData as case class
    val inputDF=spark.read.option("delimiter"," ").csv(resourcesPath).toDF("timestamp","elb","clientIPandPort","backendIPandPort","request_processing_time","backend_processing_time","response_processing_time","elb_status_code","backend_status_code","received_bytes","sent_bytes","URL","browser","ssl_cipher","ssl_protocol")
    val inputDS: Dataset[SessionData] = inputDF.as[SessionData]

    inputDS.show()

    /*
    pre-processed the data to split information like client ip, port, url in strutured format which is easily querable.
    The data is then stored into a dataset ProcessedData. Defined schema for processsed data in case class.
    */
    val processedDS=inputDS.map(
                                  r=> { new ProcessedData(
                                                            r.timestamp.replace("T", " "),
                                                            r.clientIPandPort.split(":")(0),
                                                            r.clientIPandPort.split(":")(1),
                                                            r.URL.split(" ")(1),
                                                            r.browser

                                                          )
                                      }
                               )
    processedDS.show()

    //converted string field timestamp to unixtimestamps for performing date-time operations on it
    val timeProcessedDS=processedDS.withColumn("timestamp",unix_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss").cast(TimestampType).as("timestamp"))

    //using Window function to find the rolling data after partitioning and ordering the data
    import org.apache.spark.sql.expressions.Window

    val orderedLogsWithPrevNext = timeProcessedDS.
      withColumn("PrevTime",lag("timestamp",1).over(Window.partitionBy("clientIP").orderBy("timestamp"))).
      withColumn("NextTime" , lead("timestamp" , 1).over(Window.partitionBy("clientIP").orderBy("timestamp"))).
      orderBy("timestamp")



    /**
      * Adding InactiveSince and InactiveUntil columns
      * InactiveSince: Time (in secs) ever since the user has been inactive (-1 if this is the user's first activity in the log).
      * InactiveUntil: Time (in secs) the user will stay inactive until its next activity (-1 if this is the user's last activity in the log).
      * */

    val orderedLogsWithTimeGaps = orderedLogsWithPrevNext.select("timestamp" ,"clientIP", "PrevTime", "NextTime").
      withColumn("InactiveSince" , col("timestamp").cast("long") - col("PrevTime").cast("long")).
      withColumn("InactiveUntil" , col("NextTime").cast("long") - col("timestamp").cast("long") ).
      orderBy("timestamp").
      na.fill(-1,Seq("InactiveSince")).na.fill(-1,Seq("InactiveUntil"))


    /**
      *  Let's keep only the rows with InactiveSince of either -1 or  a value larger InactivityThreshold,
      *  and also the rows with InactiveUntil of either -1 or a value larger than InactivityThreshold.
      *  Why? Because these are the moments in which either a new session starts or a session ends (or maybe both, i.e. sessions containing
      *  only one activity in the log, with InactiveSince and InactiveUntil value of -1)!
      */
    val sessionFirstAndLastActivity = orderedLogsWithTimeGaps.
      filter(col("InactiveSince").equalTo(-1) || col("InactiveSince") > 900 ||
        col("InactiveUntil").equalTo(-1) || col("InactiveUntil") > 900)
      .orderBy(asc("timestamp"))


    /**
      *  So far, each row belongs to a timestamp in which a session starts, or a session ends. Let's combine these two types
      *  of rows to one, to create a complete sessions list. So, our new dataframe would have client IP, Session StartTime,
      *  Session LastActivity, and SessionLength. The LastActivity column shows the user's last activity session,
      *  meaning the session EndTime happens 15 mins after (InactivityThreshold).
      *  So: SessionLength = LastActivity - StartTime + InactivityThreshold
      */
    val allSessionsList = sessionFirstAndLastActivity.
      filter(col("InactiveSince")>900 || col("InactiveSince").equalTo(-1)).
      select( "clientIP" ,"timestamp" ).
      withColumn( "LastActivity" ,
        lead("timestamp" , 1).over(Window.partitionBy("clientIP").orderBy("timestamp"))).
      withColumn("ActivityLength" , col("LastActivity").cast("long") - col("timestamp").cast("long")).
      withColumn("SessionLength" , col("ActivityLength").
        cast("long") + 900).na.fill(900, Seq("SessionLength")).
      withColumnRenamed("timestamp","StartTime")

    /**  Giving a unique sessionID to each session **/
    val sessions = allSessionsList.orderBy("StartTime").
      withColumn("SessionID", monotonically_increasing_id()).withColumnRenamed("clientIP" , "client")

    sessions.show(20)

    sessions.persist()

    println(" ------------- PART 1 : SESSIONIZE WITH ASSUMED 15 MINUTE WINDOW -----------------")
    sessions.select("SessionID" , "client" , "StartTime" , "SessionLength").show(50,false)

    /**
      * Sessionizing the original web log => Assign sessionID to each row of the weblogs.
      * I did this by joining the sessions dataframe to the original weblogs data frame using
      * the right condition on timestamps.
      */
    val sessionizedWebLogs = timeProcessedDS.join(sessions , timeProcessedDS("clientIP") === sessions("client") &&
      (timeProcessedDS("timestamp") === sessions("StartTime") ||
        (timeProcessedDS("timestamp") > sessions("StartTime") &&
          timeProcessedDS("timestamp").cast("long") - sessions("StartTime").cast("long") <=
            sessions("ActivityLength").cast("long"))))



    println("Sessionized web log for sessionID=2")
    sessionizedWebLogs.filter(col("SessionID").equalTo(2)).
      select("SessionID", "timestamp" , "clientIP" ,"URL").orderBy("timestamp").
      show(false)


    println("------------- PART 2 : FIND AVERAGE SESSION DURATION -----------------")
    sessions.select(avg("SessionLength").alias("Avg_Session_Time")).show()

    /**
      * Unique url visit per session.
      * I'm not sure if I've got the question exactly right. My own understanding is
      * to return urls which are visited exactly once in a given session.
      * */
    println("------------- PART 3 : FIND UNIQUE URL HITS PER SESSION -------------")
    sessionizedWebLogs.groupBy("SessionID" , "URL").count().alias("count").filter(col("count").equalTo(1))
      .select("SessionID" , "URL").show(false)

    /*** Most engaged users ***/
    val mostEngagedUsers = sessions.groupBy("client").
      agg( sum("SessionLength").alias("Total_SessionLength_Secs") ,
        avg("SessionLength").alias("Average_SessionLength_Secs")).
      orderBy(desc("Total_SessionLength_Secs"))

    println("------------- PART 4 : FIND MOST ENGAGED USERS -------------")
    mostEngagedUsers.show(20, false)



  }



}
