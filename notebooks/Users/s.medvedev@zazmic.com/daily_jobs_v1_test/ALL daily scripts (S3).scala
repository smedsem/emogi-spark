// Databricks notebook source
import org.joda.time._

var currentTs = DateTime.now()
val defaultDateTo = new DateTime(currentTs.getYear,currentTs.getMonthOfYear,currentTs.getDayOfMonth,0,0)//.minusDays(1)
val defaultDateFrom = defaultDateTo.minusDays(1)

// dbutils.widgets.text("dateFrom", defaultDateFrom.getMillis.toString, "dateFrom")
// dbutils.widgets.text("dateTo", defaultDateTo.getMillis.toString, "dateTo")
dbutils.widgets.text("s3Folder", "/mnt/events", "s3Folder")

//we either take it from the incoming parameter or use default ts range (yesterday)
val tsFrom = defaultDateFrom.getMillis //dbutils.widgets.get("dateFrom").toLong // 1512777600000l //
val tsTo = defaultDateTo.getMillis //dbutils.widgets.get("dateTo").toLong // 1513036800000l //
val s3Folder = dbutils.widgets.get("s3Folder")

val timeoutSec = 24*60*60 //1 day


// COMMAND ----------

println(s"Starting ALL daily scripts for the time range from ${new DateTime(tsFrom)} to ${new DateTime(tsTo)}...")
val dtAllStart = DateTime.now()

// COMMAND ----------

val dt4Start = DateTime.now()
println("Starting DIP Circle (S3) data daily script...")

val result = dbutils.notebook.run("/Users/alex@emogi.com/daily_jobs_s3/DIP Circles/DIP Circle runner (S3)", timeoutSec, Map("dateFrom" -> tsFrom.toString, "dateTo" -> tsTo.toString, "s3Folder" -> s3Folder))

println(s"DIP Circle data (S3) daily script...result = $result! It took ${Minutes.minutesBetween(DateTime.now(), dt4Start)} minutes")

// COMMAND ----------

val dt7Start = DateTime.now()
println("Starting Daily report daily script...")

val result = dbutils.notebook.run("/Users/alex@emogi.com/daily_jobs_s3/Daily report/Daily report runner (S3)", timeoutSec, Map("dateFrom" -> tsFrom.toString, "dateTo" -> tsTo.toString, "s3Folder" -> s3Folder))

println(s"Daily report (S3)  script...result = $result! It took ${Minutes.minutesBetween(DateTime.now(), dt7Start)} minutes")

// COMMAND ----------

val dt7Start = DateTime.now()
println("Starting Trigger stats data daily script...")

val result = dbutils.notebook.run("/Users/alex@emogi.com/daily_jobs_s3/Trigger/Trigger stats runner (S3)", timeoutSec, Map("dateFrom" -> tsFrom.toString, "dateTo" -> tsTo.toString, "s3Folder" -> s3Folder))

println(s"Trigger stats (S3) daily script...result = $result! It took ${Minutes.minutesBetween(DateTime.now(), dt7Start)} minutes")

// COMMAND ----------

val dt9Start = DateTime.now()
println("Starting DIP content daily script...")

val result = dbutils.notebook.run("/Users/alex@emogi.com/daily_jobs_s3/DIP content/DIP content runner (S3)", timeoutSec, Map("dateFrom" -> tsFrom.toString, "dateTo" -> tsTo.toString, "s3Folder" -> s3Folder))

println(s"DIP content (S3) script...result = $result! It took ${Minutes.minutesBetween(DateTime.now(), dt9Start)} minutes")

// COMMAND ----------

val dt1Start = DateTime.now()
println("Starting ODS stats (S3) daily script...")

val result = dbutils.notebook.run("/Users/alex@emogi.com/daily_jobs_s3/ODS_stats/ODS stats runner (S3)", timeoutSec, Map("dateFrom" -> tsFrom.toString, "dateTo" -> tsTo.toString, "s3Folder" -> s3Folder))

println(s"ODS stats (S3) daily script...result = $result! It took ${Minutes.minutesBetween(DateTime.now(), dt1Start)} minutes")

// COMMAND ----------

val dt2Start = DateTime.now()
println("Starting ODS ETL (S3) daily script...")

val result = dbutils.notebook.run("/Users/alex@emogi.com/daily_jobs_s3/ODS_ETL/ODS ETL runner (S3)", timeoutSec, Map("dateFrom" -> tsFrom.toString, "dateTo" -> tsTo.toString, "s3Folder" -> s3Folder))

println(s"ODS ETL (S3) daily script...result = $result! It took ${Minutes.minutesBetween(DateTime.now(), dt2Start)} minutes")

// COMMAND ----------

val dt3Start = DateTime.now()
println("Starting DIP stats (S3) daily script...")

val result = dbutils.notebook.run("/Users/alex@emogi.com/daily_jobs_s3/DIP stats/DIP ETL runner (S3)", timeoutSec, Map("dateFrom" -> tsFrom.toString, "dateTo" -> tsTo.toString, "s3Folder" -> s3Folder))

println(s"DIP stats (S3) daily script...result = $result! It took ${Minutes.minutesBetween(DateTime.now(), dt3Start)} minutes")

// COMMAND ----------

val dt5Start = DateTime.now()
println("Starting Placement stats data daily script...")

val result = dbutils.notebook.run("/Users/alex@emogi.com/daily_jobs_s3/Placement/Placement stats runner (S3)", timeoutSec, Map("dateFrom" -> tsFrom.toString, "dateTo" -> tsTo.toString, "s3Folder" -> s3Folder))

println(s"Placement stats (S3) daily script...result = $result! It took ${Minutes.minutesBetween(DateTime.now(), dt5Start)} minutes")

// COMMAND ----------

val dt8Start = DateTime.now()
println("Starting DIP graph daily script...")

val result = dbutils.notebook.run("/Users/alex@emogi.com/daily_jobs_s3/DIP graph/DIP Graph runner (S3)", timeoutSec, Map("dateFrom" -> tsFrom.toString, "dateTo" -> tsTo.toString, "s3Folder" -> s3Folder))

println(s"DIP graph (S3)  script...result = $result! It took ${Minutes.minutesBetween(DateTime.now(), dt8Start)} minutes")

// COMMAND ----------

val dt10Start = DateTime.now()
println("Starting Search stats data daily script...")

val result = dbutils.notebook.run("/Users/alex@emogi.com/daily_jobs_s3/Search/Search runner", timeoutSec, Map("dateFrom" -> tsFrom.toString, "dateTo" -> tsTo.toString, "s3Folder" -> s3Folder))

println(s"Search stats (S3) daily script...result = $result! It took ${Minutes.minutesBetween(DateTime.now(), dt10Start)} minutes")

// COMMAND ----------

val dt6Start = DateTime.now()
println("Starting Reach stats data daily script...")

val result = dbutils.notebook.run("/Users/alex@emogi.com/daily_jobs_s3/Reach/Reach stats runner (S3)", timeoutSec, Map("dateFrom" -> tsFrom.toString, "dateTo" -> tsTo.toString, "s3Folder" -> s3Folder))

println(s"Reach stats (S3) daily script...result = $result! It took ${Minutes.minutesBetween(DateTime.now(), dt6Start)} minutes")

// COMMAND ----------

println(s"DONE ALL daily scripts for the time range from ${new DateTime(tsFrom)} to ${new DateTime(tsTo)}.. it took ${Minutes.minutesBetween(DateTime.now(), dtAllStart)} minutes")