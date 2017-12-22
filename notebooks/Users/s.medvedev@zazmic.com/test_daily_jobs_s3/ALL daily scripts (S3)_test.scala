// Databricks notebook source
import org.joda.time._

var currentTs = DateTime.now()
val defaultDateTo = new DateTime(2017,12,10,0,0)//.minusDays(1)
val defaultDateFrom = defaultDateTo.minusDays(1)

// dbutils.widgets.text("dateFrom", defaultDateFrom.getMillis.toString, "dateFrom")
// dbutils.widgets.text("dateTo", defaultDateTo.getMillis.toString, "dateTo")
dbutils.widgets.text("s3Folder", "/mnt/non_prod_events/dev/raw_logs", "s3Folder")

//we either take it from the incoming parameter or use default ts range (yesterday)
val tsFrom = defaultDateFrom.getMillis // 1512777600000l //
val tsTo = defaultDateTo.getMillis //dbutils.widgets.get("dateTo").toLong // 1513036800000l //
val s3Folder = dbutils.widgets.get("s3Folder")

val timeoutSec = 24*60*60 //1 day

val scriptsPath ="/Users/s.medvedev@emogi.com/daily_jobs_s3"


// COMMAND ----------

println(s"Starting ALL daily scripts for the time range from ${new DateTime(tsFrom)} to ${new DateTime(tsTo)}...")
val dtAllStart = DateTime.now()

// COMMAND ----------

val dt4Start = DateTime.now()
println("Starting DIP Circle (S3) data daily script...")

val result = dbutils.notebook.run(s"$scriptsPath/DIP Circles/DIP Circle runner (S3)", timeoutSec, Map("dateFrom" -> tsFrom.toString, "dateTo" -> tsTo.toString, "s3Folder" -> s3Folder))

println(s"DIP Circle data (S3) daily script...result = $result! It took ${Minutes.minutesBetween(DateTime.now(), dt4Start)} minutes")

// COMMAND ----------

println(s"DONE ALL daily scripts for the time range from ${new DateTime(tsFrom)} to ${new DateTime(tsTo)}.. it took ${Minutes.minutesBetween(DateTime.now(), dtAllStart)} minutes")