// Databricks notebook source
// MAGIC %run "/Users/alex@emogi.com/daily_jobs_s3/Config (S3)"

// COMMAND ----------

// MAGIC %run "/Users/alex@emogi.com/daily_jobs_s3/Utils"

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

val saveMode = SaveMode.Append //SaveMode.Overwrite // // - we need this mode to create mysql table

val s3Path = getS3PathForDates(tsFrom, tsTo, 0)

// COMMAND ----------


import com.datastax.spark.connector._
import java.util.TimeZone
import java.util.Date
import org.joda.time.{DateTime, DateTimeZone}
import java.sql.Timestamp
import java.text.SimpleDateFormat
import org.apache.spark.sql.functions._

val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")
val timeRangeSeconds = 300

val cassandraRdd = sqlContext
    .read
    .json(getS3PathsForType(s3Folder, getS3PathForDates(tsFrom, tsTo, 0), "dst"):_*) 
    .select("ap", "el", "ch", "de", "dt")
    .filter($"el".isNotNull && $"ch".isNotNull)

val cassandraDF = cassandraRdd.map { row =>
    val ap = row.getString(0)
    val el = row.getString(1)
    val ch = row.getString(2)
    val de = row.getString(3)
    val dt = row.getLong(4)
    var dayTs = new DateTime(dt)  //.withZone(DateTimeZone.forID(timezone))
    dayTs = new DateTime(dayTs.getYear, dayTs.getMonthOfYear, dayTs.getDayOfMonth, dayTs.getHourOfDay, 0)
    (ap, el, ch, de, new Timestamp(dayTs.getMillis), dt)
}
 .toDF("dst_ap", "dst_el", "dst_ch", "dst_de", "dst_ts", "dst_dt")

val conxDF = sqlContext.read
  .json(getS3PathsForType(s3Folder, s3Path, "conx"):_*)
  .select("ap", "de", "co", "at", "ch", "dt", "xp")   
  .filter($"co".isNotNull && $"ch".isNotNull)

val elementsContentDF = cassandraDF.join(conxDF,
  cassandraDF.col("dst_ap") === conxDF.col("ap") && 
  cassandraDF.col("dst_ch") === conxDF.col("ch") && 
  cassandraDF.col("dst_dt") <= conxDF.col("dt") && conxDF.col("dt") <= cassandraDF.col("dst_dt") + timeRangeSeconds * 1000,
  "right_outer"
)
  .filter($"dst_ap".isNotNull)
  .select("dst_ap", "dst_el", "dst_de", "dst_ts", "de", "co", "at", "xp")
  .rdd
  .map{row =>
    val dst_ap = row.getString(0)
    val dst_el = row.getString(1)
    val dst_de = row.getString(2)
    val dst_ts = row.getTimestamp(3)
    val de = row.getString(4)
    val co = row.getString(5)
    val at = row.getString(6)
    val xp = row.getString(7)
    val context_type = if (dst_de.equals(de)) 1 else 0
    val view = if (at.equals("v")) 1l else 0
    val share = if (at.equals("s")) 1l else 0
    ((dst_ts, dst_ap, dst_el, xp, co, context_type),(view, share))
  }
  .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
  .map { row =>
    var dayTs = new DateTime(row._1._1.getTime)
    dayTs = new DateTime(dayTs.getYear, dayTs.getMonthOfYear, dayTs.getDayOfMonth, dayTs.getHourOfDay, 0)
    (row._1._1, dayTs.getMonthOfYear, dayTs.getWeekOfWeekyear, dayTs.getDayOfWeek, dayTs.getHourOfDay, row._1._2, row._1._3, row._1._4, row._1._5, row._1._6, row._2._1.toLong, row._2._2.toLong, 300, "sec")
  }.toDF("date", "month", "week", "dow", "hour", "app_id", "element_id", "xpla_id", "content_id", "context_type", "views", "shares", "window_length", "window_type")


val startTs = DateTime.now()

// delete existing rows
// list of days in the new stats report
val datesList = elementsContentDF.select($"date").distinct().collect()

if (storeResultsToMySQL) {

  val table = "dip_element_content_counts"
  print(s"Starting writing results to $table...")

  import org.apache.spark.sql.execution.datasources.jdbc._

  val deleteQuery = s"DELETE FROM $table WHERE date=?;"
  val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1,table,Map()))()
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

  //write to MySQL

  elementsContentDF
  .write.mode(saveMode)
  .jdbc(jdbcDipCts1, table, new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")
} else {
  display(elementsContentDF)
}


// COMMAND ----------

dbutils.notebook.exit("success")
