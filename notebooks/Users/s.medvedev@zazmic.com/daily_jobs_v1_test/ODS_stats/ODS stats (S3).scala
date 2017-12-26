// Databricks notebook source
// MAGIC %run "/Users/s.medvedev@zazmic.com/daily_jobs_v1_test/Config (S3)"

// COMMAND ----------

// MAGIC %run "/Users/s.medvedev@zazmic.com/daily_jobs_v1_test/Utils"

// COMMAND ----------

import org.joda.time.DateTime
var currentTs = DateTime.now()
val defaultDateTo = new DateTime(currentTs.getYear,currentTs.getMonthOfYear,currentTs.getDayOfMonth,0,0)
val defaultDateFrom = defaultDateTo.minusDays(1)

dbutils.widgets.text("dateFrom", defaultDateFrom.getMillis.toString, "dateFrom")
dbutils.widgets.text("dateTo", defaultDateTo.getMillis.toString, "dateTo")
dbutils.widgets.text("s3Folder", "/mnt/events", "s3Folder")

//we either take it from the incoming parameter or use default ts range (yesterday)
val tsFrom = dbutils.widgets.get("dateFrom").toLong
val tsTo = dbutils.widgets.get("dateTo").toLong
val s3Folder = dbutils.widgets.get("s3Folder")

val saveMode = SaveMode.Append //SaveMode.Overwrite - we need this mode to create mysql table

val s3Path = getS3PathForDates(tsFrom, tsTo, 0)

// COMMAND ----------

import java.util.TimeZone
import org.joda.time.{DateTime, DateTimeZone}
import java.sql.Timestamp
import org.apache.spark.util.StatCounter

val conxDF = sqlContext.read.json(getS3PathsForType(s3Folder, s3Path, "conx"):_*)

val conxRDDRaw = conxDF
// .filter($"as" >= tsFrom && $"as" <= tsTo)
.select("ap", "de", "at", "dt")   

val conxRdd = conxRDDRaw.rdd 

val conxDailyByDeviceDF = conxRdd.map { row =>
  val ap = row.getString(0)
  val de = row.getString(1)
  val at = row.getString(2)
  var utcActionTimestamp = extractTime(row, 3)
  val timezoneOffset = 0 // all should be in UTC -5 //todo read tz from devapp row.getIntOption("tz").getOrElse(-5)
  //val timezone = TimeZone.getAvailableIDs(timezoneOffset * 60 * 60 * 1000).head
  var dayTimestamp = new DateTime(utcActionTimestamp) //.withZone(DateTimeZone.forID(timezone))
  dayTimestamp = new DateTime(dayTimestamp.getYear, dayTimestamp.getMonthOfYear, dayTimestamp.getDayOfMonth, 0, 0)

  val view = if (at.equals("v")) 1l else 0
  val share = if (at.equals("s")) 1l else 0
  //cut off bogus dates
  if (dayTimestamp.isAfter(DateTime.now())) {
    null
  } else {
    ((ap, de, new Timestamp(dayTimestamp.getMillis)), (view, share))
  }
}.filter(x=> x != null)
.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
.map { row =>
  (row._1._1, row._1._2, row._1._3, row._2._1.toLong, row._2._2.toLong)
}
.toDF("ap", "de", "ts", "views", "shares")


// here we calculate max/min/avg/mean for views/shares per app/date
val aggrViewsSharesPerAppDateDF = conxDailyByDeviceDF.rdd.map { row =>
  val ap = row.getString(0)
  val de = row.getString(1)
  val ts = row.getTimestamp(2)
  val views = row.getLong(3)
  val shares = row.getLong(4)
  
 ((ap, ts), (views, shares))
}.groupByKey
  .mapValues { value =>
    val viewsCounter = StatCounter(value.map(_._1.toDouble))
    val sharesCounter = StatCounter(value.map(_._2.toDouble))
    (viewsCounter, sharesCounter)
  }
  .map { row =>
    val averageViews = (row._2._1.sum / row._2._1.count).toLong
    val averageShares = (row._2._2.sum / row._2._2.count).toLong
    (row._1._1, row._1._2, 
      row._2._1.max.toLong, row._2._1.min.toLong, row._2._1.mean.toLong, averageViews.toLong, //views stats
      row._2._2.max.toLong, row._2._2.min.toLong, row._2._2.mean.toLong, averageShares.toLong //shares stats
    )
  }.toDF("ap", "ts",
    "views_max", "views_min", "views_mean", "views_avg",
    "shares_max", "shares_min", "shares_mean", "shares_avg"
  )
aggrViewsSharesPerAppDateDF.write.mode(SaveMode.Overwrite).saveAsTable("aggrViewsSharesPerAppDateDF")




// COMMAND ----------

val msgDF = sqlContext.read.json(getS3PathsForType(s3Folder, s3Path, "msg"):_*)

val msgRDDRaw = msgDF
// .filter($"dt" >= tsFrom && $"dt" <= tsTo)
.select("ap", "de", "mc", "dt")

val msgRdd = msgRDDRaw.rdd 

val msgDailyByDeviceDF = msgRdd.map { row =>
  val ap = row.getString(0)
  val de = row.getString(1)
  val mc = if (row.isNullAt(2)) 0 else row.get(2).toString.toInt
  var utcActionTimestamp = extractTime(row, 3)
  val timezoneOffset = 0 //-5 //todo read tz from devapp row.getIntOption("tz").getOrElse(-5)
  val timezone = TimeZone.getAvailableIDs(timezoneOffset * 60 * 60 * 1000).head
  var dayTimestamp = new DateTime(utcActionTimestamp).withZone(DateTimeZone.forID(timezone))
  dayTimestamp = new DateTime(dayTimestamp.getYear, dayTimestamp.getMonthOfYear, dayTimestamp.getDayOfMonth, 0, 0)

   //cut off bogus dates
  if (dayTimestamp.isAfter(DateTime.now())) {
    null
  } else {
    ((ap, de, new Timestamp(dayTimestamp.getMillis)), mc)
  }
}.filter(x=> x != null)
.reduceByKey((a, b) => a + b)
.map { row =>
  (row._1._1,row._1._2,row._1._3,row._2.toLong)
}
.toDF("ap", "de", "ts", "mc")

val msgDailyByDeviceWithViewShareDF = msgDailyByDeviceDF
  .join(conxDailyByDeviceDF,
    msgDailyByDeviceDF.col("ap") === conxDailyByDeviceDF.col("ap") &&
      msgDailyByDeviceDF.col("de") === conxDailyByDeviceDF.col("de") &&
      msgDailyByDeviceDF.col("ts") === conxDailyByDeviceDF.col("ts"),
    "outer"
  ).map{x=>
    val apMsg = extractString(x,0)
    val deMsg = extractString(x,1)
    val tsMsg = extractTimestamp(x,2)
    val mc = extractLong(x, 3)
    val apConx = extractString(x,4)
    val deConx = extractString(x,5)
    val tsConx = extractTimestamp(x,6)
    val views = extractLong(x, 7)
    val shares = extractLong(x, 8)
    if (apMsg == null) {
      (apConx,deConx,tsConx,mc,views,shares)
    } else {
      (apMsg,deMsg,tsMsg,mc,views,shares)
    }
  }.toDF("ap", "de", "ts", "mc", "views", "shares")

// msgDailyByDeviceWithViewShareDF.cache()
msgDailyByDeviceWithViewShareDF.write.mode(SaveMode.Overwrite).saveAsTable("msgDailyByDeviceWithViewShareDF")


// COMMAND ----------

val appxDF = sqlContext.read.json(getS3PathsForType(s3Folder, s3Path, "appx"):_*) 

// val addTimezone = 5 * 60 * 60 * 1000 + 1
val appxRDDRaw = appxDF
// .filter($"as" >= (tsFrom + addTimezone) && $"as" <= (tsTo + addTimezone))
.select("ap", "de", "ss", "at", "as")

// //todo we need to use dt here

val apppxRdd = appxRDDRaw.rdd 
val sessionsDailyByDeviceRDD = apppxRdd.map { row =>
  if (!row.isNullAt(0) && !row.isNullAt(1) && !row.isNullAt(2) && !row.isNullAt(3) && !row.isNullAt(4)) {
    val ap = row.getString(0)
    val de = row.getString(1)
    val ss = row.getString(2)
    val at = row.getString(3)
    var utcActionTimestamp = extractTime(row, 4)
    val timezoneOffset = 0 //-5 //todo read tz from devapp row.getIntOption("tz").getOrElse(-5)
    //val timezone = TimeZone.getAvailableIDs(timezoneOffset * 60 * 60 * 1000).head
    var dayTimestamp = new DateTime(utcActionTimestamp) //.withZone(DateTimeZone.forID(timezone))
    dayTimestamp = new DateTime(dayTimestamp.getYear, dayTimestamp.getMonthOfYear, dayTimestamp.getDayOfMonth, 0, 0)

    val range = if (at.equals("a")) {
      (utcActionTimestamp, -1l, -1l)
    } else {
      (-1l, utcActionTimestamp, -1l)
    }
    
    //cut off bogus dates
    if (dayTimestamp.isAfter(DateTime.now())) {
      null
    } else {
      ((ap, de, new Timestamp(dayTimestamp.getMillis), ss), range) //(ap,de,ts,ss),(tsFrom,tsTo,sessionLength)
    }  
  } else {
    null
  }
}.filter(x=> x != null && x._1 != null)
  .reduceByKey { (a: Tuple3[Long, Long, Long], b: Tuple3[Long, Long, Long]) =>
    if (a == null || b == null) {
      null
    } else if (a._1 > 0) {
      (a._1, b._2, b._2 - a._1)
    } else if (b._1 > 0) {
      (b._1, a._2, a._2 - b._1)
    } else null
  }
   .filter(_._2 != null)
  .map { row =>
    val session = if (row._2._3>0) {
      row._2._3
    } else {
      30000 //default session
    }
    (row._1._1, row._1._2, row._1._3, session.toLong) //(ap,de,ts),sessionLength
  }
  .toDF("ap", "de", "ts", "session")

sessionsDailyByDeviceRDD.write.mode(SaveMode.Overwrite).saveAsTable("sessionsDailyByDeviceRDD")

// COMMAND ----------

val sessionsDailyByDeviceRDDCached = sqlContext.sql("select * from sessionsDailyByDeviceRDD")
val aggrViewsSharesPerAppDateDFCached  = sqlContext.sql("select * from aggrViewsSharesPerAppDateDF")
val msgDailyByDeviceWithViewShareDFCached  = sqlContext.sql("select * from msgDailyByDeviceWithViewShareDF")

val summaryByDeviceDFRaw = sessionsDailyByDeviceRDDCached.join(
  msgDailyByDeviceWithViewShareDFCached,
  msgDailyByDeviceWithViewShareDFCached.col("ap") === sessionsDailyByDeviceRDDCached.col("ap") &&
    msgDailyByDeviceWithViewShareDFCached.col("de") === sessionsDailyByDeviceRDDCached.col("de") &&
    msgDailyByDeviceWithViewShareDFCached.col("ts") === sessionsDailyByDeviceRDDCached.col("ts"),
    "outer"
).map{x=>
    val apMsg = extractString(x,0)
    val deMsg = extractString(x,1)
    val tsMsg = extractTimestamp(x,2)
    val session = extractLong(x, 3)
    val apConx = extractString(x,4)
    val deConx = extractString(x,5)
    val tsConx = extractTimestamp(x,6)
    val mc = extractLong(x, 7)
    val views = extractLong(x, 8)
    val shares = extractLong(x, 9)
    if (apMsg == null) {
      (apConx,deConx,tsConx,session,mc,views,shares)
    } else {
      (apMsg,deMsg,tsMsg,session,mc,views,shares)
    }
  }
   .toDF("ap", "de", "ts", "session", "msg", "views", "shares")

val summaryByDeviceDF = summaryByDeviceDFRaw.join(
  aggrViewsSharesPerAppDateDFCached,
  summaryByDeviceDFRaw.col("ap") === aggrViewsSharesPerAppDateDFCached.col("ap") &&
  summaryByDeviceDFRaw.col("ts") === aggrViewsSharesPerAppDateDFCached.col("ts")
).drop(aggrViewsSharesPerAppDateDFCached.col("ap"))
.drop(aggrViewsSharesPerAppDateDFCached.col("ts"))

//summaryDFCached.persist(StorageLevel.MEMORY_AND_DISK)
summaryByDeviceDF.write.mode(SaveMode.Overwrite).saveAsTable("summaryByDeviceDF")



// COMMAND ----------

val summaryByDeviceDFCached = sqlContext.sql("select * from summaryByDeviceDF")

val summaryDF = summaryByDeviceDFCached.rdd.map { row =>
  val ap = row.getString(0)
  val ts = extractTime(row, 2)
  val session = row.getLong(3)
  val mc = row.getLong(4)
  val views = row.getLong(5)
  val shares = row.getLong(6)
  val views_mean = row.getLong(9)
  val shares_mean = row.getLong(13)
  val isViewed = views > 0
  val isShared = shares > 0
  val isHeavyViewUsage = views >= views_mean
  val isHeavyShareUsage = shares >= shares_mean
  //cut off bogus dates
  if (new DateTime(ts).isAfter(DateTime.now())) {
    null
  } else {
   ((ap, new Timestamp(ts), isViewed, isShared, isHeavyViewUsage, isHeavyShareUsage), (session, mc, views, shares))
  }
}.filter(x=> x != null)
  .groupByKey
  .mapValues { value =>
    val sessionCounter = StatCounter(value.map(_._1.toDouble))
    val msgCounter = StatCounter(value.map(_._2.toDouble))
    val viewsCounter = value.map(_._3).sum
    val sharesCounter = value.map(_._4).sum
    (sessionCounter, msgCounter, viewsCounter, sharesCounter, value.size)
  }
  .map { row =>
    val averageSessions = (row._2._1.sum / row._2._1.count).toLong
    val averageMsg = (row._2._2.sum / row._2._2.count).toLong
    (row._1._1, row._1._2,
      row._1._3,row._1._4, //is viewed/shared
      row._1._5,row._1._6, //is heavy viewed/shared
      (row._2._1.max/1000).toLong, (row._2._1.min/1000).toLong, (row._2._1.mean/1000).toLong, (averageSessions/1000).toLong, row._2._1.count, //sessions
      row._2._2.max, row._2._2.min, row._2._2.mean.toLong, averageMsg.toLong, row._2._2.count, //messages
      row._2._3, //views
      row._2._4, //shares
      row._2._5 // unique device count
    )
  }
  .toDF("ap", "ts",
    "is_viewed", "is_shared",
    "is_heavy_views", "is_heavy_shares",
    "ss_max", "ss_min", "ss_mean", "ss_avg", "ss_cnt",
    "msg_max", "msg_min", "msg_mean", "msg_avg", "msg_cnt",
    "views",
    "shares",
    "dev_cnt"
  )

summaryDF.write.mode(SaveMode.Overwrite).saveAsTable("dailystats")



// COMMAND ----------

val summaryDFCached = sqlContext.sql("select * from dailystats")

//list of days in the new stats report
val datesList = summaryDFCached.select($"ts").distinct().collect().toList

if (storeResultsToMySQL) {
  //delete existing rows
  import org.apache.spark.sql.execution.datasources.jdbc._

  val deleteQuery = "DELETE FROM ods_stats_daily WHERE ts=?;"
  val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1,"ods_stats_daily",Map()))()
  try {
    datesList.foreach{ ts =>
      val stmt = mysqlConnection.prepareStatement(deleteQuery)
      stmt.setTimestamp(1, ts.getTimestamp(0))
      stmt.execute()
    }
  }
  finally {
    if (null != mysqlConnection) {
      mysqlConnection.close()
    }
  }

  // write to MySQL
  summaryDFCached
  .write.mode(saveMode)
.jdbc(jdbcDipCts1, "ods_stats_daily", new java.util.Properties())
} else {
  display(summaryDFCached)
}

// COMMAND ----------

dbutils.notebook.exit("success")