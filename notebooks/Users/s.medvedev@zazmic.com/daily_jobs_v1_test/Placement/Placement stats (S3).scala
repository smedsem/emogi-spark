// Databricks notebook source
// MAGIC %run "/Users/s.medvedev@zazmic.com/daily_jobs_v1_test/Config (S3)"

// COMMAND ----------

// MAGIC %run "/Users/s.medvedev@zazmic.com/daily_jobs_v1_test/Utils"

// COMMAND ----------

import org.joda.time.DateTime
import org.apache.spark.sql.execution.datasources.jdbc._
import java.util.TimeZone
import org.joda.time.{DateTime, DateTimeZone}
import java.sql.Timestamp
import org.apache.spark.util.StatCounter

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

val saveMode = SaveMode.Append //SaveMode.Overwrite // // - we need this mode to create mysql table

// COMMAND ----------

val conxDF = sqlContext.read.json(getS3PathsForType(s3Folder, getS3PathForDates(tsFrom, tsTo, 0), "conx"):_*)

val mxRawRdd = conxDF
  .select("ap", "at", "ca", "av", "cp", "xp", "ma", "dt")
  .map {row =>
    val ap = row.getString(0)
    val at = row.getString(1)
    val ca = extractNullableString(row, 2) 
    val av = extractNullableString(row, 3) 
    val receivers = if (row.isNullAt(4)) {
      0
    } else {
      val num = row.get(4).toString.toInt
      if (num > 0 && at.equals("s")) num - 1 else 0
    }  
    val xp = row.getString(5)
    val ma = row.getString(6)
    var utcActionTimestamp = extractTime(row, 7)
    var hourTimestamp = new DateTime(utcActionTimestamp)
    hourTimestamp = new DateTime(hourTimestamp.getYear, hourTimestamp.getMonthOfYear, hourTimestamp.getDayOfMonth, hourTimestamp.getHourOfDay, 0)
    (ap, at, ca, av, receivers, xp, ma, new Timestamp(hourTimestamp.getMillis))
  }
  .toDF("ap", "at", "ca", "av", "cp", "xp", "ma", "dt")

val mxViewsRdd = mxRawRdd
  .filter($"at" === "v")
  .distinct
  .rdd 

val mxSharesRdd = mxRawRdd
  .filter($"at" === "s")
  .rdd 

val mxRdd = mxViewsRdd.union(mxSharesRdd)

// val mxRdd = conxDF
//   .select("ap", "at", "ca", "av", "cp", "xp", "ma", "dt")
//   .map {row =>
//     val ap = row.getString(0)
//     val at = row.getString(1)
//     val ca = if (!row.isNullAt(2)) row.getString(2) else null
//     val av = if (!row.isNullAt(3)) row.getString(3) else null
//     val receivers = if (row.isNullAt(4)) {
//       0
//     } else {
//       val num = row.get(4).toString.toInt
//       if (num > 0 && at.equals("s")) num - 1 else 0
//     }  
//     val xp = row.getString(5)
//     val ma = row.getString(6)
//     var utcActionTimestamp = extractTime(row, 7)
//     var hourTimestamp = new DateTime(utcActionTimestamp)
//     hourTimestamp = new DateTime(hourTimestamp.getYear, hourTimestamp.getMonthOfYear, hourTimestamp.getDayOfMonth, hourTimestamp.getHourOfDay, 0)
//     (ap, at, ca, av, receivers, xp, ma, new Timestamp(hourTimestamp.getMillis))
//   }
//   .toDF("ap", "at", "ca", "av", "receivers", "xp", "ma", "dt")
//   .distinct
//   .rdd 


val campaignPerformanceWithNamesDFResult = mxRdd.map { row =>
    val ap = row.getString(0)
    val at = row.getString(1)
    val ca = extractNullableString(row, 2) 
    val av =  extractNullableString(row, 3) 
    val receivers = row.getInt(4)
    val xp = row.getString(5)
    val dt = row.getTimestamp(7)

    val view = if (at.equals("v")) 1l else 0
    val share = if (at.equals("s")) 1l else 0

    ((ap, xp, ca, av, dt), (view, share, receivers))
}.filter(x=> x != null)
.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
.map { row =>
  val views = row._2._1.toLong
  val shares = row._2._2.toLong
  val receivers = row._2._3.toLong
  val appId = row._1._1
  val xplaId = row._1._2
  val campId = row._1._3
  val advId = row._1._4
  val isBranded = advId != null
  val dt = new DateTime(row._1._5.getTime)
  (row._1._5, 
   dt.getYear, dt.getMonthOfYear, dt.getDayOfMonth, dt.getHourOfDay, dt.dayOfWeek().get, 
   views, shares, receivers,
   xplaId, campId, advId, appId, isBranded)
}
.toDF("date","year","month","day","hour","dow","views","shares","receiver_views","xpla_id","camp_id","adv_id","app_id", "is_branded")

// delete existing rows
// list of days in the new stats report
val datesList = campaignPerformanceWithNamesDFResult.select($"year",$"month",$"day").distinct().collect()


if (storeResultsToMySQL) { 

  val deleteQuery = "DELETE FROM campaign_performance WHERE year=? and month=? and day=?;"
  
  val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1,"campaign_performance",scala.Predef.Map()))()
  try {
    datesList.foreach { d =>
      val stmt = mysqlConnection.prepareStatement(deleteQuery)
      stmt.setInt(1, d(0).asInstanceOf[Int])
      stmt.setInt(2, d(1).asInstanceOf[Int])
      stmt.setInt(3, d(2).asInstanceOf[Int])
      stmt.execute()
      println(s"Deleted records from campaign_performance for day=$d")
    }
  }
  finally {
    if (null != mysqlConnection) {
      mysqlConnection.close()
    }
  }

  //write to MySQL
  print("Starting writing results to campaign_performance...")

  val startTs = DateTime.now()
  
  campaignPerformanceWithNamesDFResult
  .write.mode(saveMode)
  .jdbc(jdbcDipCts1, "campaign_performance", new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")
} else {
  display(campaignPerformanceWithNamesDFResult)
}


// COMMAND ----------

// import java.util.TimeZone
// import org.joda.time.{DateTime, DateTimeZone}
// import java.sql.Timestamp
// import org.apache.spark.util.StatCounter

// val conxDF = sqlContext.read.json(getS3PathsForType(s3Folder, getS3PathForDates(tsFrom, tsTo, 0), "conx"):_*)

// val conxRDDRaw = conxDF
// //.filter($"dt" >= tsFrom && $"dt" <= tsTo)
// .select("ap", "at", "xp", "ca", "av", "cp", "dt")
// .distinct

// val conxRdd = conxRDDRaw.rdd 

// val campaignPerformanceWithNamesDFResult = conxRdd.map { row =>
//     val ap = row.getString(0)
//     val at = row.getString(1)
//     val xp = row.getString(2)
//     val ca = if (!row.isNullAt(3)) row.getString(3) else null
//     val av =  if (!row.isNullAt(4)) row.getString(4) else null
//     val receivers = if (row.isNullAt(5)) {
//       0
//     } else {
//       val num = row.get(5).toString.toInt
//       if (num > 0 && at.equals("s")) num - 1 else 0
//     }  

//     var utcActionTimestamp = extractTime(row, 6)
//     var hourTimestamp = new DateTime(utcActionTimestamp)
//     hourTimestamp = new DateTime(hourTimestamp.getYear, hourTimestamp.getMonthOfYear, hourTimestamp.getDayOfMonth, hourTimestamp.getHourOfDay, 0)

//     val view = if (at.equals("v")) 1l else 0
//     val share = if (at.equals("s")) 1l else 0
//     //cut off bogus dates
//     if (hourTimestamp.isAfter(DateTime.now())) {
//       null
//     } else {
//       ((new Timestamp(hourTimestamp.getMillis), ap, xp, ca, av), (view, share, receivers))
//     }
// }.filter(x=> x != null)
// .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
// .map { row =>
//   val date = new DateTime(row._1._1.getTime)
//   val views = row._2._1.toLong
//   val shares = row._2._2.toLong
//   val receivers = row._2._3.toLong
//   val appId = row._1._2
//   val xplaId = row._1._3
//   val campId = row._1._4
//   val advId = row._1._5
//   val isBranded = row._1._5 != null
//   (row._1._1, date.getYear, date.getMonthOfYear, date.getDayOfMonth, date.getHourOfDay, date.dayOfWeek().get, 
//    views, shares, receivers,
//    xplaId, campId, advId, appId, isBranded)
// }
// .toDF("date","year","month","day","hour","dow","views","shares","receiver_views","xpla_id","camp_id","adv_id","app_id", "is_branded")

// // delete existing rows
// // list of days in the new stats report
// val datesList = campaignPerformanceWithNamesDFResult.select($"year",$"month",$"day").distinct().collect()

// import org.apache.spark.sql.execution.datasources.jdbc._

// if (storeResultsToMySQL) { 

//   val deleteQuery = "DELETE FROM campaign_performance WHERE year=? and month=? and day=?;"
  
//   val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1,"campaign_performance",scala.Predef.Map()))()
//   try {
//     datesList.foreach { d =>
//       val stmt = mysqlConnection.prepareStatement(deleteQuery)
//       stmt.setInt(1, d(0).asInstanceOf[Int])
//       stmt.setInt(2, d(1).asInstanceOf[Int])
//       stmt.setInt(3, d(2).asInstanceOf[Int])
//       stmt.execute()
//       println(s"Deleted records from campaign_performance for day=$d")
//     }
//   }
//   finally {
//     if (null != mysqlConnection) {
//       mysqlConnection.close()
//     }
//   }

//   //write to MySQL
//   print("Starting writing results to campaign_performance...")

//   val startTs = DateTime.now()
  
//   campaignPerformanceWithNamesDFResult
//   .write.mode(saveMode)
//   .jdbc(jdbcDipCts1, "campaign_performance", new java.util.Properties())

//   val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
//   println(s" - DONE, it took $durationMinutes minutes")
// } else {
//   display(campaignPerformanceWithNamesDFResult)
// }


// COMMAND ----------

dbutils.notebook.exit("success")

// COMMAND ----------


// val from = 1504569600000l
// val to = 1504742399999l
// val s3Folder = "/mnt/events"

// val paths = getS3PathsForType(s3Folder, getS3PathForDates(from, to, 0), "conx")
// val conxDF = sqlContext.read.json(paths:_*)

// val conxRDDRaw = conxDF
// //.filter($"dt" >= tsFrom && $"dt" <= tsTo)
// .select("ap", "at", "ct", "xp", "ca", "av", "cp", "dt")
// .distinct

// val conxRdd = conxRDDRaw.rdd 

// val campaignPerformanceWithNamesDFResult = conxRdd.map { row =>
//     val ap = row.getString(0)
//     val at = row.getString(1)
//     val ct = row.getString(2)
//     val xp = row.getString(3)
//     val ca = if (!row.isNullAt(4)) row.getString(4) else null
//     val av =  if (!row.isNullAt(5)) row.getString(5) else null
//     val receivers = if (row.isNullAt(6)) {
//       0
//     } else {
//       val num = row.get(6).toString.toInt
//       if (num > 0 && at.equals("s")) num - 1 else 0
//     }  

//     var utcActionTimestamp = extractTime(row, 7)
//     var hourTimestamp = new DateTime(utcActionTimestamp)
//     hourTimestamp = new DateTime(hourTimestamp.getYear, hourTimestamp.getMonthOfYear, hourTimestamp.getDayOfMonth, hourTimestamp.getHourOfDay, 0)

//     val view = if (at.equals("v")) 1l else 0
//     val share = if (at.equals("s")) 1l else 0
//     //cut off bogus dates
//     if (hourTimestamp.isAfter(DateTime.now())) {
//       null
//     } else {
//       ((new Timestamp(hourTimestamp.getMillis), ap, ct, xp, ca, av), (view, share, receivers))
//     }
// }.filter(x=> x != null)
// .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
// .map { row =>
//   val date = new DateTime(row._1._1.getTime)
//   val views = row._2._1.toLong
//   val shares = row._2._2.toLong
//   val receivers = row._2._3.toLong
//   val appId = row._1._2
//   val contentType = row._1._3
//   val xplaId = row._1._4
//   val campId = row._1._5
//   val advId = row._1._6
//   val isBranded = row._1._6 != null
//   (row._1._1, date.getYear, date.getMonthOfYear, date.getDayOfMonth, date.getHourOfDay, date.dayOfWeek().get, 
//    views, shares, receivers,
//    contentType, xplaId, campId, advId, appId, isBranded)
// }
// .toDF("date","year","month","day","hour","dow","views","shares","receiver_views","content_type","xpla_id","camp_id","adv_id","app_id", "is_branded")

// display(campaignPerformanceWithNamesDFResult)

// COMMAND ----------

// val df = campaignPerformanceWithNamesDFResult.filter($"camp_id" === "zyv7hd")
// val df1 = df.select("views").map(_.getLong(0)).collect.sum
// // display(df1)