// Databricks notebook source
import org.joda.time.{DateTime, Days}

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



// COMMAND ----------

import org.joda.time.{DateTime, Days}

val dateFrom = tsFrom.toString
val dateTo = tsTo.toString
val timeoutSec = 24*60*60 //1 day

val from = new DateTime(tsFrom)
val to = new DateTime(tsTo)

val daysCount = Days.daysBetween(from, to).getDays //+ 1

val dayRanges = for (day <- 0 until daysCount) yield {
  val tsFrom = if (day == 0) {
    from
  } else {
    val tsFromDay = from.plusDays(day)
    new DateTime(tsFromDay.getYear, tsFromDay.getMonthOfYear, tsFromDay.getDayOfMonth, 0, 0)
  }
  val tsTo = if (day == daysCount - 1) {
    to
  } else {
    new DateTime(tsFrom.getYear, tsFrom.getMonthOfYear, tsFrom.getDayOfMonth, 23, 59, 59)
  }
  (tsFrom.getMillis, tsTo.getMillis)
}

val globalStartTs = DateTime.now()

dayRanges.foreach { day =>
  val startTs = DateTime.now()
  print(s"Started collecting Reach S3 stats for the time range from ${new DateTime(day._1)} to ${new DateTime(day._2)}..")
  val result = dbutils.notebook.run("/Users/s.medvedev@zazmic.com/daily_jobs_v1_test/Reach/Reach stats (S3)", timeoutSec, Map("dateFrom" -> day._1.toString, "dateTo" -> day._2.toString, "s3Folder" -> s3Folder))
  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - processed day, result=$result, it took $durationMinutes minutes")
}

val durationGlobalMinutes = (((DateTime.now().getMillis - globalStartTs.getMillis) / 1000 )/ 60).toInt
println(s"Finished processing Reach S3 stats from=$from to=$to, it took $durationGlobalMinutes minutes")

// COMMAND ----------

dbutils.notebook.exit("success")