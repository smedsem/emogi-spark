// Databricks notebook source
// MAGIC %run "/Users/alex@emogi.com/daily_jobs_s3/Utils"

// COMMAND ----------

import org.joda.time._
import java.util.Properties

var currentTs = DateTime.now()
val defaultDateTo = new DateTime(currentTs.getYear,currentTs.getMonthOfYear,currentTs.getDayOfMonth,0,0)//.minusDays(1)
val defaultDateFrom = defaultDateTo.minusDays(1)

dbutils.widgets.text("dateFrom", defaultDateFrom.getMillis.toString, "dateFrom")
dbutils.widgets.text("dateTo", defaultDateTo.getMillis.toString, "dateTo")
dbutils.widgets.text("s3Folder", "/mnt/non_prod_events/qa/raw_logs", "s3Folder")

//we either take it from the incoming parameter or use default ts range (yesterday)
//val tsFrom = defaultDateFrom.getMillis //dbutils.widgets.get("dateFrom").toLong
//val tsTo = defaultDateTo.getMillis //dbutils.widgets.get("dateTo").toLong
//val s3Folder = dbutils.widgets.get("s3Folder")

val tsFrom = dbutils.widgets.get("dateFrom").toLong
val tsTo = dbutils.widgets.get("dateTo").toLong
val s3Folder = dbutils.widgets.get("s3Folder")

val s3Path = getS3PathForDates(tsFrom, tsTo, 0)

val timeoutSec = 24*60*60 //1 day


//DEV SETTINGS
 val jdbcDipCts1 = s"jdbc:mysql://172.18.240.158:3306/stats_dev?user=db_cluster&password=8FymtV6FZEzzgbE"
//val jdbcDipCts1ProductAnalytics = s"jdbc:mysql://172.18.240.158:3306/product_analytics?user=db_cluster&password=8FymtV6FZEzzgbE"
// val storeResultsToMySQL = false


// Creating dropdowns with filters
val df = spark.read.jdbc(jdbcDipCts1, "content_reach", new Properties())
display(df)
//val appsDF = spark.read.jdbc(jdbcUrlPUB, "pub_platform", new Properties()).toDF().drop("pub_id").toDF("app_id","app_name")

//val categoriesSeq = Seq("ALL") ++ categoriesDF.map(_.getString(1)).collect().toSeq
//dbutils.widgets.dropdown("category", categoriesSeq.head, categoriesSeq, "Category")
//val appsSeq = Seq("ALL") ++ appsDF.map(_.getString(1)).collect().toSeq
//dbutils.widgets.dropdown("app", appsSeq.head, appsSeq, "Apps")



//val appsDF = spark.read.jdbc(jdbcUrlPUB, "pub_platform", new //Properties()).toDF().select("pub_platform_id","name").toDF("app_id","app_name")

//appsDF.display





// COMMAND ----------

import java.util.TimeZone
import org.joda.time.{DateTime, DateTimeZone}
import java.sql.Timestamp
import org.apache.spark.util.StatCounter

println(getS3PathsForType(s3Folder, s3Path, "conx").head)
val conxDF = sqlContext.read.json(getS3PathsForType(s3Folder, s3Path, "conx"):_*).select("ac","dax")
conxDF.show
//val conxDF = sqlContext.read.json("/dev/raw_logs/conx_s3_v2/dt=2017-12-10/*")

//display(conxDF)

// COMMAND ----------

println(getS3PathsForType(s3Folder, s3Path, "conx").head)
