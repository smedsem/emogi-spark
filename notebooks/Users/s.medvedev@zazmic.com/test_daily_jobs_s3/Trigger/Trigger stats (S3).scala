// Databricks notebook source
// MAGIC %run "/Users/alex@emogi.com/daily_jobs_s3/Config (S3)"

// COMMAND ----------

// MAGIC %run "/Users/alex@emogi.com/daily_jobs_s3/Utils"

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

val saveMode = SaveMode.Append //SaveMode.Overwrite // //SaveMode.Overwrite // // - we need this mode to create mysql table

// COMMAND ----------


val conxDF = sqlContext.read.json(getS3PathsForType(s3Folder, getS3PathForDates(tsFrom, tsTo, 0), "conx"):_*)

val mxRawRdd = conxDF
  .select("ap", "at", "tr", "xp", "ma", "dt")
  .map {row =>
    val ap = row.getString(0)
    val at = row.getString(1)
    val tr = row.getString(2)
    val xp = row.getString(3)
    val ma = row.getString(4)
    var utcActionTimestamp = extractTime(row, 5)
    var hourTimestamp = new DateTime(utcActionTimestamp)
    hourTimestamp = new DateTime(hourTimestamp.getYear, hourTimestamp.getMonthOfYear, hourTimestamp.getDayOfMonth, hourTimestamp.getHourOfDay, 0)
    (ap, at, tr, xp, ma, new Timestamp(hourTimestamp.getMillis))
  }
  .toDF("ap", "at", "tr", "xp", "ma", "dt")

val mxViewsRdd = mxRawRdd
  .filter($"at" === "v")
  .distinct
  .rdd 

val mxSharesRdd = mxRawRdd
  .filter($"at" === "s")
  .rdd 

val mxRdd = mxViewsRdd.union(mxSharesRdd)

// val mxRdd = conxDF
//   .select("ap", "at", "tr", "xp", "ma", "dt")
//   .map {row =>
//     val ap = row.getString(0)
//     val at = row.getString(1)
//     val tr = row.getString(2)
//     val xp = row.getString(3)
//     val ma = row.getString(4)
//     var utcActionTimestamp = extractTime(row, 5)
//     var hourTimestamp = new DateTime(utcActionTimestamp)
//     hourTimestamp = new DateTime(hourTimestamp.getYear, hourTimestamp.getMonthOfYear, hourTimestamp.getDayOfMonth, hourTimestamp.getHourOfDay, 0)
//     (ap, at, tr, xp, ma, new Timestamp(hourTimestamp.getMillis))
//   }
//   .toDF("ap", "at", "tr", "xp", "ma", "dt")
//   .distinct
//   .rdd 

val triggerPerformanceWithNamesDFResult = mxRdd.map { row =>
    val ap = row.getString(0)
    val at = row.getString(1)
    val tr = row.getString(2)
    val xp = row.getString(3)
    
    val dt = row.getTimestamp(5)
  
    val view = if (at.equals("v")) 1l else 0
    val share = if (at.equals("s")) 1l else 0
    ((ap, xp, tr, dt), (view, share))
}.filter(x=> x != null)
.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
.map { row =>
  val views = row._2._1.toLong
  val shares = row._2._2.toLong
  val appId = row._1._1
  val xplaId = row._1._2
  val trigId = row._1._3
  val dt = new DateTime(row._1._4.getTime)

  (row._1._4, 
   dt.getYear, dt.getMonthOfYear, dt.getDayOfMonth, dt.getHourOfDay, dt.dayOfWeek().get, 
   views, shares, 
   xplaId, trigId, appId)
}
.toDF("date","year","month","day","hour","dow","views","shares","xpla_id","trig_id","app_id")

// delete existing rows
// list of days in the new stats report
val datesList = triggerPerformanceWithNamesDFResult.select($"year",$"month",$"day").distinct().collect()

import org.apache.spark.sql.execution.datasources.jdbc._

if (storeResultsToMySQL) { 

  val deleteQuery = "DELETE FROM trigger_performance WHERE year=? and month=? and day=?;"
  
  val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1,"trigger_performance",scala.Predef.Map()))()
  try {
    datesList.foreach { d =>
      val stmt = mysqlConnection.prepareStatement(deleteQuery)
      stmt.setInt(1, d(0).asInstanceOf[Int])
      stmt.setInt(2, d(1).asInstanceOf[Int])
      stmt.setInt(3, d(2).asInstanceOf[Int])
      stmt.execute()
      println(s"Deleted records from trigger_performance for day=$d")
    }
  }
  finally {
    if (null != mysqlConnection) {
      mysqlConnection.close()
    }
  }

  //write to MySQL
  print("Starting writing results to trigger_performance...")

  val startTs = DateTime.now()
  
  triggerPerformanceWithNamesDFResult
  .write.mode(saveMode)
  .jdbc(jdbcDipCts1, "trigger_performance", new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")
} else {
  display(triggerPerformanceWithNamesDFResult)
}


// COMMAND ----------

// import java.util.TimeZone
// import org.joda.time.{DateTime, DateTimeZone}
// import java.sql.Timestamp
// import org.apache.spark.util.StatCounter

// val conxDF = sqlContext.read.json(getS3PathsForType(s3Folder, getS3PathForDates(tsFrom, tsTo, 0), "conx"):_*)

// val conxRDDRaw = conxDF
// //.filter($"dt" >= tsFrom && $"dt" <= tsTo)
// .select("ap", "at", "xp", "tr", "dt")
// .distinct

// val conxRdd = conxRDDRaw.rdd 

// val triggerPerformanceWithNamesDFResult = conxRdd.map { row =>
//     val ap = row.getString(0)
//     val at = row.getString(1)
//     val xp = row.getString(2)
//     val tr = row.getString(3)
//     var utcActionTimestamp = extractTime(row, 4)
//     var hourTimestamp = new DateTime(utcActionTimestamp)
//     hourTimestamp = new DateTime(hourTimestamp.getYear, hourTimestamp.getMonthOfYear, hourTimestamp.getDayOfMonth, hourTimestamp.getHourOfDay, 0)

//     val view = if (at.equals("v")) 1l else 0
//     val share = if (at.equals("s")) 1l else 0
//     //cut off bogus dates
//     if (hourTimestamp.isAfter(DateTime.now())) {
//       null
//     } else {
//       ((new Timestamp(hourTimestamp.getMillis), ap, xp, tr), (view, share))
//     }
// }.filter(x=> x != null)
// .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
// .map { row =>
//   val date = new DateTime(row._1._1.getTime)
//   val views = row._2._1.toLong
//   val shares = row._2._2.toLong
//   val appId = row._1._2
//   val xplaId = row._1._3
//   val trigId = row._1._4
//   (row._1._1, date.getYear, date.getMonthOfYear, date.getDayOfMonth, date.getHourOfDay, date.dayOfWeek().get, 
//    views, shares, 
//    xplaId, trigId, appId)
// }
// .toDF("date","year","month","day","hour","dow","views","shares","xpla_id","trig_id","app_id")

// // delete existing rows
// // list of days in the new stats report
// val datesList = triggerPerformanceWithNamesDFResult.select($"year",$"month",$"day").distinct().collect()

// import org.apache.spark.sql.execution.datasources.jdbc._

// if (storeResultsToMySQL) { 

//   val deleteQuery = "DELETE FROM trigger_performance WHERE year=? and month=? and day=?;"
  
//   val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1,"trigger_performance",scala.Predef.Map()))()
//   try {
//     datesList.foreach { d =>
//       val stmt = mysqlConnection.prepareStatement(deleteQuery)
//       stmt.setInt(1, d(0).asInstanceOf[Int])
//       stmt.setInt(2, d(1).asInstanceOf[Int])
//       stmt.setInt(3, d(2).asInstanceOf[Int])
//       stmt.execute()
//       println(s"Deleted records from trigger_performance for day=$d")
//     }
//   }
//   finally {
//     if (null != mysqlConnection) {
//       mysqlConnection.close()
//     }
//   }

//   //write to MySQL
//   print("Starting writing results to trigger_performance...")

//   val startTs = DateTime.now()
  
//   triggerPerformanceWithNamesDFResult
//   .write.mode(saveMode)
//   .jdbc(jdbcDipCts1, "trigger_performance", new java.util.Properties())

//   val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
//   println(s" - DONE, it took $durationMinutes minutes")
// } else {
//   display(triggerPerformanceWithNamesDFResult)
// }


// COMMAND ----------

dbutils.notebook.exit("success")