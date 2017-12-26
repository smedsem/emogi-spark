// Databricks notebook source
// MAGIC %run "/Users/s.medvedev@zazmic.com/daily_jobs_v1_test/Config (S3)"

// COMMAND ----------

// MAGIC %run "/Users/s.medvedev@zazmic.com/daily_jobs_v1_test/Utils"

// COMMAND ----------

import org.joda.time.DateTime
import java.util.TimeZone
import org.joda.time.{DateTime, DateTimeZone}
import java.sql.Timestamp
import org.apache.spark.util.StatCounter
import org.apache.spark.sql.execution.datasources.jdbc._
import org.apache.spark.sql.types._

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

val saveMode = SaveMode.Append //SaveMode.Overwrite // // //  - we need this mode to create mysql table

val deleteBeforeSave = true


// COMMAND ----------

val appsDF = spark.read.jdbc(jdbcUrlPUB, "pub_platform", new Properties()).toDF().select("pub_platform_id","name").toDF("app_id","app_name")
val trigDF = spark.read.jdbc(jdbcUrlCMS, "trig", new Properties()).toDF().select("trig_id", "trig_type_id", "trig_key")
val xplaDF = spark.read.jdbc(jdbcUrlCMS, "xpla", new Properties()).toDF().select("xpla_id", "name").toDF("xpla_id", "xpla_name")
val campDF = spark.read.jdbc(jdbcUrlCMS, "camp", new Properties()).toDF().select("camp_id", "name").toDF("camp_id", "camp_name")
val advDF = spark.read.jdbc(jdbcUrlCMS, "adv", new Properties()).toDF().select("adv_id", "name").toDF("adv_id", "adv_name")
val contentDF = spark.read.jdbc(jdbcUrlCMS, "content", new Properties()).toDF().select("content_id", "name").toDF("content_id", "content_name")


// COMMAND ----------

val conxDF = sqlContext.read
  .schema(StructType(List(
        StructField("ap", StringType),
        StructField("at", StringType),
        StructField("av", StringType),
        StructField("xp", StringType),
        StructField("ma", StringType),
        StructField("dt", LongType),
        StructField("tr", StringType),
        StructField("ca", StringType),
        StructField("co", StringType),
        StructField("pl", StringType)
      )))
  .json(getS3PathsForType(s3Folder, getS3PathForDates(tsFrom, tsTo, 0), "conx"):_*)

val mxRawRdd = conxDF
  .select("ap", "at", "av", "xp", "ma", "dt", "ca", "pl", "tr")
  .map {row =>
    val ap = row.getString(0)
    val at = row.getString(1)
    val av = extractNullableString(row, 2) 
    val xp = row.getString(3)
    val ma = row.getString(4)
    var utcActionTimestamp = extractTime(row, 5)
    var dayTimestamp = new DateTime(utcActionTimestamp)
    dayTimestamp = new DateTime(dayTimestamp.getYear, dayTimestamp.getMonthOfYear, dayTimestamp.getDayOfMonth, 0, 0)
    val ca = extractNullableString(row, 6)
    val pl = extractNullableString(row, 7)
    val tr = extractNullableString(row, 8)
    (ap, at, av, xp, ma, new Timestamp(dayTimestamp.getMillis), ca, pl, tr)
  }
  .toDF("ap", "at", "av", "xp", "ma", "dt", "ca", "pl", "tr")

val mxViewsRdd = mxRawRdd
  .filter($"at" === "v")
  .distinct
  //.rdd 

val mxSharesRdd = mxRawRdd
  .filter($"at" === "s")
  //.rdd 

val mxDF = mxViewsRdd.union(mxSharesRdd)

val mxRdd = mxDF.rdd

val currentTs = new Timestamp(DateTime.now().getMillis)


// COMMAND ----------

// val conxDF = sqlContext.read.json(getS3PathsForType(s3Folder, getS3PathForDates(tsFrom, tsTo, 0), "conx"):_*)

val mxRddContent = conxDF
  .select("ap", "at", "av", "xp", "ma", "dt", "ca", "co")
  .filter($"co".isNotNull)
  .map {row =>
    val ap = row.getString(0)
    val at = row.getString(1)
    val av = extractNullableString(row, 2)
    val xp = row.getString(3)
    val ma = row.getString(4)
    var utcActionTimestamp = extractTime(row, 5)
    var dayTimestamp = new DateTime(utcActionTimestamp)
    dayTimestamp = new DateTime(dayTimestamp.getYear, dayTimestamp.getMonthOfYear, dayTimestamp.getDayOfMonth, 0, 0)
    val ca = extractNullableString(row, 6)
    val co = row.getString(7)
    (ap, at, av, xp, ma, new Timestamp(dayTimestamp.getMillis), ca, co)
  }
  .toDF("ap", "at", "av", "xp", "ma", "dt", "ca", "co")

val contentDeliveryDFRaw = mxRddContent
.rdd
.map { row =>
    val ap = row.getString(0)
    val at = row.getString(1)
    val av =  extractNullableString(row, 2)
    val xp = row.getString(3)
    val dt = row.getTimestamp(5)
    val ca = extractNullableString(row, 6)
    val co = row.getString(7)
    val viewBranded = if (av != null && at.equals("v")) 1l else 0
    val shareBranded = if (av != null && at.equals("s")) 1l else 0
    val viewOrganic = if (av == null && at.equals("v")) 1l else 0
    val shareOrganic = if (av == null && at.equals("s")) 1l else 0
    ((ap, dt, av, ca, xp, co), (viewBranded, shareBranded, viewOrganic, shareOrganic))
}.filter(x=> x != null)
.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4))
.map { row =>
  val viewBranded = row._2._1.toLong
  val shareBranded = row._2._2.toLong
  val viewOrganic = row._2._3.toLong
  val shareOrganic = row._2._4.toLong
  val appId = row._1._1
  val dt = new DateTime(row._1._2.getTime)
  
  val orgo_share_rt = if (viewOrganic == 0) 0.0 else roundToPrecision((shareOrganic * 100f) / viewOrganic, 2)
  val branded_share_rt = if (viewBranded == 0) 0.0 else roundToPrecision((shareBranded * 100f) / viewBranded, 2)
  
  val allShares = shareBranded + shareOrganic
  val allViews = viewBranded + viewOrganic
  val all_share_rt = if (allViews == 0) 0.0 else roundToPrecision((viewBranded * 100f) / allViews, 2)
  
  val plat_vers = 3
  
  val av = row._1._3
  val ca = row._1._4
  val xp = row._1._5
  val co = row._1._6
  val is_branded = av != null
  
  (row._1._2, appId, viewOrganic, shareOrganic, orgo_share_rt, viewBranded, shareBranded, branded_share_rt, allViews, allShares, all_share_rt, plat_vers, currentTs, xp, av, ca, co, is_branded)
}
.toDF("ymd","app_id","orgo_views","orgo_shares","orgo_share_rt","branded_views","branded_shares","branded_share_rt","all_views","all_shares","all_share_rt","plat_vers", "date_modified", "xp", "av","ca","co", "is_branded")


val contentDeliveryDF = contentDeliveryDFRaw
  .join(appsDF, appsDF.col("app_id") === contentDeliveryDFRaw.col("app_id"), "left_outer")
  .drop(appsDF.col("app_id")).drop(contentDeliveryDFRaw.col("app_id"))
  .join(xplaDF, xplaDF.col("xpla_id") === contentDeliveryDFRaw.col("xp"), "left_outer")
  .join(advDF, advDF.col("adv_id") === contentDeliveryDFRaw.col("av"), "left_outer")
  .join(campDF, campDF.col("camp_id") === contentDeliveryDFRaw.col("ca"), "left_outer")
  .join(contentDF, contentDF.col("content_id") === contentDeliveryDFRaw.col("co"), "left_outer")
  .select("ymd","app_id","app_name","xpla_id","xpla_name", "adv_id","adv_name", "camp_id","camp_name", "content_id","content_name", "orgo_views","orgo_shares","orgo_share_rt","branded_views","branded_shares", "branded_share_rt","all_views", "all_shares","all_share_rt", "plat_vers", "is_branded", "date_modified")
  .toDF("ymd","app_id","app_name","xpla_id","xpla_name","adv_id","adv_name", "camp_id","camp_name", "content_id","content_name", "orgo_views","orgo_shares","orgo_share_rt","branded_views","branded_shares", "branded_share_rt","all_views", "all_shares","all_share_rt", "plat_vers", "is_branded", "date_modified")


// delete existing rows
// list of days in the new stats report
val datesList = contentDeliveryDF.select($"ymd").distinct().collect()
val table = "content_delivery"

if (storeResultsToMySQL) { 
  if (deleteBeforeSave) {
    val deleteQuery = s"DELETE FROM $table WHERE ymd=?;"
    val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1ProductAnalytics,table,scala.Predef.Map()))()
    try {
      datesList.foreach { d =>
        val stmt = mysqlConnection.prepareStatement(deleteQuery)
        stmt.setTimestamp(1, d(0).asInstanceOf[Timestamp])
        stmt.execute()
        println(s"Deleted records from $table for day=$d")
      }
    }
    finally {
      if (null != mysqlConnection) {
        mysqlConnection.close()
      }
    }
  }
  //write to MySQL
  print(s"Starting writing results to $table...")
  val startTs = DateTime.now()

  contentDeliveryDF
    .write
    .mode(saveMode)
    .jdbc(jdbcDipCts1ProductAnalytics, table, new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")
} else {
  display(contentDeliveryDF)
}


// COMMAND ----------


val platCampDeliveryDFRaw = mxRdd.map { row =>
    val ap = row.getString(0)
    val at = row.getString(1)
    val av =  extractNullableString(row, 2)
    val xp = row.getString(3)
    val dt = row.getTimestamp(5)
    val ca = extractNullableString(row, 6)
    val pl = extractNullableString(row, 7)
    val viewBranded = if (av != null && at.equals("v")) 1l else 0
    val shareBranded = if (av != null && at.equals("s")) 1l else 0
    val viewOrganic = if (av == null && at.equals("v")) 1l else 0
    val shareOrganic = if (av == null && at.equals("s")) 1l else 0
    ((ap, dt, av, ca, xp, pl), (viewBranded, shareBranded, viewOrganic, shareOrganic))
}.filter(x=> x != null)
.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4))
.map { row =>
  val viewBranded = row._2._1.toLong
  val shareBranded = row._2._2.toLong
  val viewOrganic = row._2._3.toLong
  val shareOrganic = row._2._4.toLong
  val appId = row._1._1
  val dt = new DateTime(row._1._2.getTime)
  
  val orgo_share_rt = if (viewOrganic == 0) 0.0 else roundToPrecision((shareOrganic * 100f) / viewOrganic, 2)
  val branded_share_rt = if (viewBranded == 0) 0.0 else roundToPrecision((shareBranded * 100f) / viewBranded, 2)
  
  val allShares = shareBranded + shareOrganic
  val allViews = viewBranded + viewOrganic
  val all_share_rt = if (allViews == 0) 0.0 else roundToPrecision((viewBranded * 100f) / allViews, 2)
  
  val plat_vers = 3
  
  val av = row._1._3
  val ca = row._1._4
  val xp = row._1._5
  val pl = row._1._6
  val is_branded = av != null
  (row._1._2, appId, viewOrganic, shareOrganic, orgo_share_rt, viewBranded, shareBranded, branded_share_rt, allViews, allShares, all_share_rt, plat_vers, currentTs, xp, av, ca, pl, is_branded)
}
.toDF("ymd","app_id","orgo_views","orgo_shares","orgo_share_rt","branded_views","branded_shares","branded_share_rt","all_views","all_shares","all_share_rt","plat_vers", "date_modified", "xp", "av","ca", "platform", "is_branded")


val platCampDeliveryfDF = platCampDeliveryDFRaw
  .join(appsDF, appsDF.col("app_id") === platCampDeliveryDFRaw.col("app_id"), "left_outer")
  .drop(appsDF.col("app_id")).drop(platCampDeliveryDFRaw.col("app_id"))
  .join(xplaDF, xplaDF.col("xpla_id") === platCampDeliveryDFRaw.col("xp"), "left_outer")
  .join(advDF, advDF.col("adv_id") === platCampDeliveryDFRaw.col("av"), "left_outer")
  .join(campDF, campDF.col("camp_id") === platCampDeliveryDFRaw.col("ca"), "left_outer")
  .select("ymd","app_id","app_name","platform", "xpla_id","xpla_name", "adv_id","adv_name", "camp_id","camp_name", "orgo_views","orgo_shares","orgo_share_rt","branded_views","branded_shares", "branded_share_rt","all_views", "all_shares","all_share_rt", "plat_vers", "is_branded", "date_modified")
  .toDF("ymd","app_id","app_name","platform","xpla_id","xpla_name","adv_id","adv_name", "camp_id","camp_name", "orgo_views","orgo_shares","orgo_share_rt","branded_views","branded_shares", "branded_share_rt","all_views", "all_shares","all_share_rt", "plat_vers", "is_branded", "date_modified")


// delete existing rows
// list of days in the new stats report
val datesList = platCampDeliveryfDF.select($"ymd").distinct().collect()
val table = "platform_campaign_delivery"

if (storeResultsToMySQL) { 
  if (deleteBeforeSave) {
    val deleteQuery = s"DELETE FROM $table WHERE ymd=?;"
    val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1ProductAnalytics,table,scala.Predef.Map()))()
    try {
      datesList.foreach { d =>
        val stmt = mysqlConnection.prepareStatement(deleteQuery)
        stmt.setTimestamp(1, d(0).asInstanceOf[Timestamp])
        stmt.execute()
        println(s"Deleted records from $table for day=$d")
      }
    }
    finally {
      if (null != mysqlConnection) {
        mysqlConnection.close()
      }
    }
  }
  //write to MySQL
  print(s"Starting writing results to $table...")
  val startTs = DateTime.now()

  platCampDeliveryfDF
    .write
    .mode(saveMode)
    .jdbc(jdbcDipCts1ProductAnalytics, table, new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")
} else {
  display(platCampDeliveryfDF)
}


// COMMAND ----------

val mxRddAppDelivery = mxDF.select("ap", "at", "av", "xp", "ma", "dt", "pl").rdd

val appDeliveryDFRaw = mxRddAppDelivery.map { row =>
    val ap = row.getString(0)
    val at = row.getString(1)
    val av =  extractNullableString(row, 2)
    val dt = row.getTimestamp(5)
    val pl = extractNullableString(row, 6)
    val viewBranded = if (av != null && at.equals("v")) 1l else 0
    val shareBranded = if (av != null && at.equals("s")) 1l else 0
    val viewOrganic = if (av == null && at.equals("v")) 1l else 0
    val shareOrganic = if (av == null && at.equals("s")) 1l else 0
    ((ap, pl, dt), (viewBranded, shareBranded, viewOrganic, shareOrganic))
}.filter(x=> x != null)
.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4))
.map { row =>
  val viewBranded = row._2._1.toLong
  val shareBranded = row._2._2.toLong
  val viewOrganic = row._2._3.toLong
  val shareOrganic = row._2._4.toLong
  val appId = row._1._1
  val platform = row._1._2
  val dt = new DateTime(row._1._3.getTime)
  
  val orgo_share_rt = if (viewOrganic == 0) 0.0 else roundToPrecision((shareOrganic * 100f) / viewOrganic, 2)
  val branded_share_rt = if (viewBranded == 0) 0.0 else roundToPrecision((shareBranded * 100f) / viewBranded, 2)
  
  val allShares = shareBranded + shareOrganic
  val allViews = viewBranded + viewOrganic
  val all_share_rt = if (allViews == 0) 0.0 else roundToPrecision((viewBranded * 100f) / allViews, 2)
  
  val plat_vers = 3

  (row._1._3, appId, viewOrganic, shareOrganic, orgo_share_rt, viewBranded, shareBranded, branded_share_rt, allViews, allShares, all_share_rt, plat_vers, platform, currentTs)
}
.toDF("ymd","app_id","orgo_views","orgo_shares","orgo_share_rt","branded_views","branded_shares","branded_share_rt","all_views","all_shares","all_share_rt","plat_vers","platform", "date_modified")

val appDeliveryDF = appDeliveryDFRaw
  .join(appsDF, appsDF.col("app_id") === appDeliveryDFRaw.col("app_id"), "left_outer")
  .drop(appsDF.col("app_id")).drop(appDeliveryDFRaw.col("app_id"))


// delete existing rows
// list of days in the new stats report
val datesList = appDeliveryDF.select($"ymd").distinct().collect()

if (storeResultsToMySQL) { 
  if (deleteBeforeSave) {
    val deleteQuery = "DELETE FROM app_delivery WHERE ymd=?;"
    val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1ProductAnalytics,"app_delivery",scala.Predef.Map()))()
    try {
      datesList.foreach { d =>
        val stmt = mysqlConnection.prepareStatement(deleteQuery)
        stmt.setTimestamp(1, d(0).asInstanceOf[Timestamp])
        stmt.execute()
        println(s"Deleted records from app_delivery for day=$d")
      }
    }
    finally {
      if (null != mysqlConnection) {
        mysqlConnection.close()
      }
    }
  }
  //write to MySQL
  print("Starting writing results to app_delivery...")
  val startTs = DateTime.now()

  appDeliveryDF
    .write
    .mode(saveMode)
    .jdbc(jdbcDipCts1ProductAnalytics, "app_delivery", new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")
} else {
  display(appDeliveryDF)
}


// COMMAND ----------

val mxRddTrig = mxDF
  .select("ap", "at", "av", "xp", "ma", "dt", "tr")
  .filter($"tr".isNotNull)
  .rdd

val compTriggerPerfDFRaw = mxRddTrig.map { row =>
    val ap = row.getString(0)
    val at = row.getString(1)
    val av =  extractNullableString(row, 2)
    val dt = row.getTimestamp(5)
    val tr = row.getString(6)
    val viewBranded = if (av != null && at.equals("v")) 1l else 0
    val shareBranded = if (av != null && at.equals("s")) 1l else 0
    val viewOrganic = if (av == null && at.equals("v")) 1l else 0
    val shareOrganic = if (av == null && at.equals("s")) 1l else 0
    ((ap, dt, tr), (viewBranded, shareBranded, viewOrganic, shareOrganic))
}.filter(x=> x != null)
.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4))
.map { row =>
  val viewBranded = row._2._1.toLong
  val shareBranded = row._2._2.toLong
  val viewOrganic = row._2._3.toLong
  val shareOrganic = row._2._4.toLong
  val appId = row._1._1
  val dt = new DateTime(row._1._2.getTime)
  
  val orgo_share_rt = if (viewOrganic == 0) 0.0 else roundToPrecision((shareOrganic * 100f) / viewOrganic, 2)
  val branded_share_rt = if (viewBranded == 0) 0.0 else roundToPrecision((shareBranded * 100f) / viewBranded, 2)
  
  val allShares = shareBranded + shareOrganic
  val allViews = viewBranded + viewOrganic
  val all_share_rt = if (allViews == 0) 0.0 else roundToPrecision((viewBranded * 100f) / allViews, 2)
  
  val plat_vers = 3
  
  val tr = row._1._3
  
  (row._1._2, appId, viewOrganic, shareOrganic, orgo_share_rt, viewBranded, shareBranded, branded_share_rt, allViews, allShares, all_share_rt, plat_vers, currentTs, tr)
}
.toDF("ymd","app_id","orgo_views","orgo_shares","orgo_share_rt","branded_views","branded_shares","branded_share_rt","all_views","all_shares","all_share_rt","plat_vers", "date_modified", "tr")

val compTriggerPerfDF = compTriggerPerfDFRaw
  .join(appsDF, appsDF.col("app_id") === compTriggerPerfDFRaw.col("app_id"), "left_outer")
  .drop(appsDF.col("app_id")).drop(compTriggerPerfDFRaw.col("app_id"))
  .join(trigDF, trigDF.col("trig_id") === compTriggerPerfDFRaw.col("tr"), "left_outer")
  .select("ymd","app_id","app_name","trig_key","trig_type_id", "orgo_views","orgo_shares","orgo_share_rt","branded_views","branded_shares", "branded_share_rt","all_views", "all_shares","all_share_rt", "plat_vers", "date_modified")
.toDF("ymd","app_id","app_name","trig","trig_type", "orgo_views","orgo_shares","orgo_share_rt","branded_views","branded_shares", "branded_share_rt","all_views", "all_shares","all_share_rt", "plat_vers", "date_modified")


// delete existing rows
// list of days in the new stats report
val datesList = compTriggerPerfDF.select($"ymd").distinct().collect()
val table = "comparative_trigger_perf"

if (storeResultsToMySQL) { 
  if (deleteBeforeSave) {
    val deleteQuery = s"DELETE FROM $table WHERE ymd=?;"
    val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1ProductAnalytics,table,scala.Predef.Map()))()
    try {
      datesList.foreach { d =>
        val stmt = mysqlConnection.prepareStatement(deleteQuery)
        stmt.setTimestamp(1, d(0).asInstanceOf[Timestamp])
        stmt.execute()
        println(s"Deleted records from $table for day=$d")
      }
    }
    finally {
      if (null != mysqlConnection) {
        mysqlConnection.close()
      }
    }
  }
  //write to MySQL
  print(s"Starting writing results to $table...")
  val startTs = DateTime.now()

  compTriggerPerfDF
    .write
    .mode(saveMode)
    .jdbc(jdbcDipCts1ProductAnalytics, table, new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")
} else {
  display(compTriggerPerfDF)
}


// COMMAND ----------


val triggerPerfDFRaw = mxRddTrig.map { row =>
    val ap = row.getString(0)
    val at = row.getString(1)
    val av =  extractNullableString(row, 2)
    val xp = row.getString(3)
    val dt = row.getTimestamp(5)
    val tr = row.getString(6)
    val viewBranded = if (av != null && at.equals("v")) 1l else 0
    val shareBranded = if (av != null && at.equals("s")) 1l else 0
    val viewOrganic = if (av == null && at.equals("v")) 1l else 0
    val shareOrganic = if (av == null && at.equals("s")) 1l else 0
    ((ap, dt, tr, xp), (viewBranded, shareBranded, viewOrganic, shareOrganic))
}.filter(x=> x != null)
.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4))
.map { row =>
  val viewBranded = row._2._1.toLong
  val shareBranded = row._2._2.toLong
  val viewOrganic = row._2._3.toLong
  val shareOrganic = row._2._4.toLong
  val appId = row._1._1
  val dt = new DateTime(row._1._2.getTime)
  
  val orgo_share_rt = if (viewOrganic == 0) 0.0 else roundToPrecision((shareOrganic * 100f) / viewOrganic, 2)
  val branded_share_rt = if (viewBranded == 0) 0.0 else roundToPrecision((shareBranded * 100f) / viewBranded, 2)
  
  val allShares = shareBranded + shareOrganic
  val allViews = viewBranded + viewOrganic
  val all_share_rt = if (allViews == 0) 0.0 else roundToPrecision((viewBranded * 100f) / allViews, 2)
  
  val plat_vers = 3
  
  val tr = row._1._3
  val xp = row._1._4
  
  (row._1._2, appId, viewOrganic, shareOrganic, orgo_share_rt, viewBranded, shareBranded, branded_share_rt, allViews, allShares, all_share_rt, plat_vers, currentTs, tr, xp)
}
.toDF("ymd","app_id","orgo_views","orgo_shares","orgo_share_rt","branded_views","branded_shares","branded_share_rt","all_views","all_shares","all_share_rt","plat_vers", "date_modified", "tr", "xp")

val triggerPerfDF = triggerPerfDFRaw
  .join(appsDF, appsDF.col("app_id") === triggerPerfDFRaw.col("app_id"), "left_outer")
  .drop(appsDF.col("app_id")).drop(triggerPerfDFRaw.col("app_id"))
  .join(trigDF, trigDF.col("trig_id") === triggerPerfDFRaw.col("tr"), "left_outer")
  .join(xplaDF, xplaDF.col("xpla_id") === triggerPerfDFRaw.col("xp"), "left_outer")
  .select("ymd","app_id","app_name","xpla_id","xpla_name", "trig_key","trig_type_id", "orgo_views","orgo_shares","orgo_share_rt","branded_views","branded_shares", "branded_share_rt","all_views", "all_shares","all_share_rt", "plat_vers",  "date_modified")
  .toDF("ymd","app_id","app_name","xpla_id","xpla_name","trig","trig_type", "orgo_views","orgo_shares","orgo_share_rt","branded_views","branded_shares", "branded_share_rt","all_views", "all_shares","all_share_rt", "plat_vers", "date_modified")


// delete existing rows
// list of days in the new stats report
val datesList = triggerPerfDF.select($"ymd").distinct().collect()
val table = "trigger_perf"

if (storeResultsToMySQL) { 
  if (deleteBeforeSave) {
    val deleteQuery = s"DELETE FROM $table WHERE ymd=?;"
    val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1ProductAnalytics,table,scala.Predef.Map()))()
    try {
      datesList.foreach { d =>
        val stmt = mysqlConnection.prepareStatement(deleteQuery)
        stmt.setTimestamp(1, d(0).asInstanceOf[Timestamp])
        stmt.execute()
        println(s"Deleted records from $table for day=$d")
      }
    }
    finally {
      if (null != mysqlConnection) {
        mysqlConnection.close()
      }
    }
  }
  //write to MySQL
  print(s"Starting writing results to $table...")
  val startTs = DateTime.now()

  triggerPerfDF
    .write
    .mode(saveMode)
    .jdbc(jdbcDipCts1ProductAnalytics, table, new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")
} else {
  display(triggerPerfDF)
}


// COMMAND ----------

val mxRddCamp = mxDF.select("ap", "at", "av", "xp", "ma", "dt", "ca", "pl").rdd

val campDeliveryDFRaw = mxRddCamp.map { row =>
    val ap = row.getString(0)
    val at = row.getString(1)
    val av =  extractNullableString(row, 2)
    val xp = row.getString(3)
    val dt = row.getTimestamp(5)
    val ca = extractNullableString(row, 6)
    val pl = extractNullableString(row, 7)
    val viewBranded = if (av != null && at.equals("v")) 1l else 0
    val shareBranded = if (av != null && at.equals("s")) 1l else 0
    val viewOrganic = if (av == null && at.equals("v")) 1l else 0
    val shareOrganic = if (av == null && at.equals("s")) 1l else 0
    ((ap, dt, av, ca, xp, pl), (viewBranded, shareBranded, viewOrganic, shareOrganic))
}.filter(x=> x != null)
.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4))
.map { row =>
  val viewBranded = row._2._1.toLong
  val shareBranded = row._2._2.toLong
  val viewOrganic = row._2._3.toLong
  val shareOrganic = row._2._4.toLong
  val appId = row._1._1
  val platform = row._1._6
  val dt = new DateTime(row._1._2.getTime)
  
  val orgo_share_rt = if (viewOrganic == 0) 0.0 else roundToPrecision((shareOrganic * 100f) / viewOrganic, 2)
  val branded_share_rt = if (viewBranded == 0) 0.0 else roundToPrecision((shareBranded * 100f) / viewBranded, 2)
  
  val allShares = shareBranded + shareOrganic
  val allViews = viewBranded + viewOrganic
  val all_share_rt = if (allViews == 0) 0.0 else roundToPrecision((viewBranded * 100f) / allViews, 2)
  
  val plat_vers = 3
  
  val av = row._1._3
  val ca = row._1._4
  val xp = row._1._5
  val is_branded = av != null
  
  (row._1._2, appId, viewOrganic, shareOrganic, orgo_share_rt, viewBranded, shareBranded, branded_share_rt, allViews, allShares, all_share_rt, plat_vers, currentTs, xp, av, ca, is_branded, platform)
}
.toDF("ymd","app_id","orgo_views","orgo_shares","orgo_share_rt","branded_views","branded_shares","branded_share_rt","all_views","all_shares","all_share_rt","plat_vers", "date_modified", "xp", "av","ca", "is_branded", "platform")


val campDeliveryfDF = campDeliveryDFRaw
  .join(appsDF, appsDF.col("app_id") === campDeliveryDFRaw.col("app_id"), "left_outer")
  .drop(appsDF.col("app_id")).drop(campDeliveryDFRaw.col("app_id"))
  .join(xplaDF, xplaDF.col("xpla_id") === campDeliveryDFRaw.col("xp"), "left_outer")
  .join(advDF, advDF.col("adv_id") === campDeliveryDFRaw.col("av"), "left_outer")
  .join(campDF, campDF.col("camp_id") === campDeliveryDFRaw.col("ca"), "left_outer")
  .select("ymd","app_id","app_name","platform","xpla_id","xpla_name", "adv_id","adv_name", "camp_id","camp_name", "orgo_views","orgo_shares","orgo_share_rt","branded_views","branded_shares", "branded_share_rt","all_views", "all_shares","all_share_rt", "plat_vers", "is_branded", "date_modified")
  .toDF("ymd","app_id","app_name","platform","xpla_id","xpla_name","adv_id","adv_name", "camp_id","camp_name", "orgo_views","orgo_shares","orgo_share_rt","branded_views","branded_shares", "branded_share_rt","all_views", "all_shares","all_share_rt", "plat_vers", "is_branded", "date_modified")


// delete existing rows
// list of days in the new stats report
val datesList = campDeliveryfDF.select($"ymd").distinct().collect()
val table = "campaign_delivery"

if (storeResultsToMySQL) { 
  if (deleteBeforeSave) {
    val deleteQuery = s"DELETE FROM $table WHERE ymd=?;"
    val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1ProductAnalytics,table,scala.Predef.Map()))()
    try {
      datesList.foreach { d =>
        val stmt = mysqlConnection.prepareStatement(deleteQuery)
        stmt.setTimestamp(1, d(0).asInstanceOf[Timestamp])
        stmt.execute()
        println(s"Deleted records from $table for day=$d")
      }
    }
    finally {
      if (null != mysqlConnection) {
        mysqlConnection.close()
      }
    }
  }
  //write to MySQL
  print(s"Starting writing results to $table...")
  val startTs = DateTime.now()

  campDeliveryfDF
    .write
    .mode(saveMode)
    .jdbc(jdbcDipCts1ProductAnalytics, table, new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")
} else {
  display(campDeliveryfDF)
}


// COMMAND ----------

dbutils.notebook.exit("success")