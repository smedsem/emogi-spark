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

val saveMode = SaveMode.Append //SaveMode.Overwrite  // //// //SaveMode.Overwrite // // - we need this mode to create mysql table

// COMMAND ----------

import org.apache.spark.sql.types._
val conxSchema = StructType(List(
        StructField("ap", StringType),
        StructField("at", StringType),
        StructField("tr", StringType),
        StructField("tk", StringType),
        StructField("xp", StringType),
        StructField("ma", StringType),
        StructField("dt", LongType),
        StructField("v", LongType)
      ))

val conxDF = sqlContext.read.schema(conxSchema).json(getS3PathsForType(s3Folder, getS3PathForDates(tsFrom, tsTo, 0), "conx"):_*)

val mxRawRdd = conxDF
  .filter($"v" > 2 && $"tk".isNotNull)
  .select("ap", "at", "tr", "tk", "xp", "ma", "dt")
  .map {row =>
    val ap = row.getString(0)
    val at = row.getString(1)
    val tr = row.getString(2)
    val tk = row.getString(3)
    val xp = row.getString(4)
    val ma = row.getString(5)
    var utcActionTimestamp = extractTime(row, 6)
    var hourTimestamp = new DateTime(utcActionTimestamp)
    hourTimestamp = new DateTime(hourTimestamp.getYear, hourTimestamp.getMonthOfYear, hourTimestamp.getDayOfMonth, hourTimestamp.getHourOfDay, 0)
    (ap, at, tr, tk, xp, ma, new Timestamp(hourTimestamp.getMillis))
  }
  .toDF("ap", "at", "tr", "tk", "xp", "ma", "dt")

val mxViewsRdd = mxRawRdd
  .filter($"at" === "v")
  .distinct
  .rdd 

val mxSharesRdd = mxRawRdd
  .filter($"at" === "s")
  .rdd 

val mxRdd = mxViewsRdd.union(mxSharesRdd)

val triggerRulePerformanceWithNamesDFResult = mxRdd.map { row =>
    val ap = row.getString(0)
    val at = row.getString(1)
    val tr = row.getString(2)
    val tk = row.getString(3)
    val xp = row.getString(4)
    
    val dt = row.getTimestamp(6)
  
    val view = if (at.equals("v")) 1l else 0
    val share = if (at.equals("s")) 1l else 0
    ((ap, xp, tr, tk, dt), (view, share))
}.filter(x=> x != null)
.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
.map { row =>
  val views = row._2._1.toLong
  val shares = row._2._2.toLong
  val appId = row._1._1
  val xplaId = row._1._2
  val trigId = row._1._3
  val tokenId = row._1._4
  val dt = new DateTime(row._1._5.getTime)

  (row._1._5, 
   dt.getYear, dt.getMonthOfYear, dt.getDayOfMonth, dt.getHourOfDay, dt.dayOfWeek().get, 
   views, shares, 
   xplaId, trigId, tokenId, appId)
}
.toDF("date","year","month","day","hour","dow","views","shares","xpla_id","trig_id","token","app_id")

// delete existing rows
// list of days in the new stats report
val datesList = triggerRulePerformanceWithNamesDFResult.select($"year",$"month",$"day").distinct().collect()

import org.apache.spark.sql.execution.datasources.jdbc._

if (storeResultsToMySQL) { 

  val table = "trigger_rule_performance"
  val deleteQuery = s"DELETE FROM $table WHERE year=? and month=? and day=?;"
  
  val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1,table,scala.Predef.Map()))()
  try {
    datesList.foreach { d =>
      val stmt = mysqlConnection.prepareStatement(deleteQuery)
      stmt.setInt(1, d(0).asInstanceOf[Int])
      stmt.setInt(2, d(1).asInstanceOf[Int])
      stmt.setInt(3, d(2).asInstanceOf[Int])
      stmt.execute()
      println(s"Deleted records from $table for day=$d")
    }
  }
  finally {
    if (null != mysqlConnection) {
      mysqlConnection.close()
    }
  }

  //write to MySQL
  print(s"Starting writing results to $table...")

  val startTs = DateTime.now()
  
  triggerRulePerformanceWithNamesDFResult
  .write.mode(saveMode)
  .jdbc(jdbcDipCts1, table, new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")
} else {
  display(triggerRulePerformanceWithNamesDFResult)
}


// COMMAND ----------

dbutils.notebook.exit("success")