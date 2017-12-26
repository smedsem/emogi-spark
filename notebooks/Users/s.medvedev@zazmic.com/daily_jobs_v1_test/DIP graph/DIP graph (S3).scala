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

// clearCache()

val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")

val cassandraRdd = sqlContext
    .read
    .json(getS3PathsForType(s3Folder, getS3PathForDates(tsFrom, tsTo, 0), "dst"):_*) 
    .select("ap", "el", "me", "dt")
    .filter($"el".isNotNull && $"me".isNotNull)

val cassandraDF = cassandraRdd.map { row =>
    val ap = row.getString(0)
    val el = row.getString(1)
    val me = row.getString(2)
    val dt = row.getLong(3)
    var dayTs = new DateTime(dt)  //.withZone(DateTimeZone.forID(timezone))
    dayTs = new DateTime(dayTs.getYear, dayTs.getMonthOfYear, dayTs.getDayOfMonth, dayTs.getHourOfDay, 0)
    (ap, el, me, new Timestamp(dayTs.getMillis))
}
 .toDF("ap", "el", "me", "ts")
 .sort($"el".asc)

val cassandraDF2 = cassandraDF

val elementsGraphDF = cassandraDF.join(cassandraDF2,
  cassandraDF.col("ap") === cassandraDF2.col("ap") && cassandraDF.col("me") === cassandraDF2.col("me") && cassandraDF.col("ts") === cassandraDF2.col("ts"),
  "right_outer"
).drop(cassandraDF.col("ap")).drop(cassandraDF2.col("me")).drop(cassandraDF.col("ts"))
  .toDF("parent_el", "ap", "el",  "me", "ts")
  .filter($"el" =!= $"parent_el")
  .groupBy($"ap",$"ts", $"parent_el",$"el")
  .agg(countDistinct($"el") as "weight")
  .select("ap", "parent_el", "el", "ts", "weight")
  .toDF("ap", "element_1_id", "element_2_id", "date", "weight")
  .map { row =>
    val ap = row.getString(0)
    val element_1_id = row.getString(1)
    val element_2_id = row.getString(2)
    val ts = row.getTimestamp(3)
    val weight = row.getLong(4).toDouble
    var dayTs = new DateTime(ts.getTime) 
    (ts, ap, element_1_id, element_2_id, dayTs.getMonthOfYear, dayTs.getWeekOfWeekyear, dayTs.getDayOfWeek, dayTs.getHourOfDay, weight, 1)
}.toDF("date", "ap", "element_1_id", "element_2_id", "month", "week", "dow", "hour", "weight", "edge_type")

val startTs = DateTime.now()

// delete existing rows
// list of days in the new stats report
val datesList = elementsGraphDF.select($"date").distinct().collect()

if (storeResultsToMySQL) {

  val table = "dip_element_graph_edges"
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

  elementsGraphDF
  .write.mode(saveMode)
  .jdbc(jdbcDipCts1, table, new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")
} else {
  display(elementsGraphDF)
}


// COMMAND ----------

dbutils.notebook.exit("success")