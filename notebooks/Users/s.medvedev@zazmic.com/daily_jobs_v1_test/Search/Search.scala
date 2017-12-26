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

val saveMode = SaveMode.Append //SaveMode.Overwrite // // // // // // - we need this mode to create mysql table
val deleteBeforeSave = true

// COMMAND ----------

val conxDF = sqlContext.read
  .schema(StructType(List(
        StructField("ap", StringType),
        StructField("at", StringType),
        StructField("ma", StringType),
        StructField("dt", LongType),
        StructField("cs", StringType),
        StructField("topic", StringType),
        StructField("co", StringType)
      )))
  .json(getS3PathsForType(s3Folder, getS3PathForDates(tsFrom, tsTo, 0), "conx"):_*) //TODO/remove true on PROD!
  .filter($"cs".isNotNull && $"topic" === "api-event")


val searchxDF = sqlContext.read
  .schema(StructType(List(
        StructField("ap", StringType),
        StructField("cs", StringType),
        StructField("ag", StringType),
        StructField("stxt", StringType),
        StructField("dt", LongType)
      )))
  .json(getS3PathsForType(s3Folder, getS3PathForDates(tsFrom, tsTo, 0), "searchx"):_*) //TODO/remove true on PROD!


val searchxRawDF = searchxDF
  .select("cs", "stxt", "dt", "ap") //, "de", "ag",  "la"
  .filter($"stxt".isNotNull)
  .map {row =>
    val searchId = row.getString(0)
    val searchText = row.getString(1)
    var utcActionTimestamp = extractTime(row, 2)
    val ap = row.getString(3)
    var dayTimestamp = new DateTime(utcActionTimestamp)
    dayTimestamp = new DateTime(dayTimestamp.getYear, dayTimestamp.getMonthOfYear, dayTimestamp.getDayOfMonth, 0, 0)
    (new Timestamp(dayTimestamp.getMillis), searchId, searchText, ap)
  }
  .toDF("ymd", "searchId", "term", "ap")


//calculating the count
val searchCountDF = searchxRawDF
.select("ymd", "term", "ap")
.rdd
.map{ row =>
    val ymd = row.getTimestamp(0)
    val searchText = row.getString(1)
    val ap = row.getString(2)
    ((ymd, searchText, ap), 1l)
}
.reduceByKey((a, b) => a + b)
.map { row =>
  val ymd = row._1._1
  val searchText = row._1._2
  val appId = row._1._3
  val number = row._2
  (ymd, appId, searchText,  number)
}
.toDF("ymd","ap","term","count")

// no result
val searchxNoResultDf = searchxRawDF.join(conxDF,
  searchxRawDF.col("searchId") === conxDF.col("cs") && searchxRawDF.col("ap") === conxDF.col("ap"),
  "left_outer"
).filter($"dt".isNull)
.drop(conxDF.col("ap"))
.select("ymd", "term", "ap")
// .distinct() //if we take distinct - then we calculate no result for every unique term
.rdd
.map{ row =>
    val ymd = row.getTimestamp(0)
    val searchText = row.getString(1)
    val ap = row.getString(2)
    ((ymd, searchText, ap), 1l)
}
.reduceByKey((a, b) => a + b)
.map { row =>
  val ymd = row._1._1
  val searchText = row._1._2
  val appId = row._1._3
  val number = row._2
  (ymd, appId, searchText,  number)
}
.toDF("ymd","ap","term","no_results")


// views/shares

val searchxConxDf = searchxRawDF.join(conxDF,
  searchxRawDF.col("searchId") === conxDF.col("cs") && searchxRawDF.col("ap") === conxDF.col("ap"),
  "right_outer"
).filter($"term".isNotNull)
.drop(conxDF.col("ap"))
.select("ymd", "term", "ap", "at", "co")

val searchViewSharesDF = searchxConxDf
.rdd
.map{ row =>
    val ymd = row.getTimestamp(0)
    val searchText = row.getString(1)
    val ap = row.getString(2)
    val at = row.getString(3)

    val view = if (at.equals("v")) 1l else 0
    val share = if (at.equals("s")) 1l else 0

    ((ymd, searchText, ap), (view, share))
}
.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
.map { row =>
  val view = row._2._1.toLong
  val share = row._2._2.toLong
  val searchText = row._1._2
  val appId = row._1._3
  (row._1._1, appId, searchText,  view, share)
}
.toDF("ymd","ap","term","views","shares")

//content
val searchxUniqueContentDf = searchxConxDf
  .select("ymd", "term", "ap", "co")
  .distinct()

val searchContentsDF = searchxUniqueContentDf
.rdd
.map{ row =>
    val ymd = row.getTimestamp(0)
    val searchText = row.getString(1)
    val ap = row.getString(2)
    ((ymd, searchText, ap), 1l)
}
.reduceByKey((a, b) => a + b)
.map { row =>
  val ymd = row._1._1
  val searchText = row._1._2
  val appId = row._1._3
  val contentNumber = row._2
  (ymd, appId, searchText, contentNumber)
}
.toDF("ymd","ap","term","contents")


val searchxUniqueDF = searchxRawDF
.select("ymd", "term", "ap")
.distinct()

val resultsDF = searchxUniqueDF
  .join(searchCountDF, Seq("ymd","ap", "term"),"left_outer")
  .join(searchxNoResultDf, Seq("ymd","ap", "term"),"left_outer")
  .join(searchViewSharesDF, Seq("ymd","ap", "term"),"left_outer")
  .join(searchContentsDF, Seq("ymd","ap", "term"),"left_outer")
  .select("ymd", "term", "ap", "count", "no_results", "views", "shares", "contents")
  .rdd
  .map{ row =>
    val ymd = row.getTimestamp(0)
    val ap = row.getString(1)
    val searchText = row.getString(2)
    val count = if (row.isNullAt(3)) 0l else row.getLong(3)
    val no_results = if (row.isNullAt(4)) 0l else row.getLong(4)
    val views = if (row.isNullAt(5)) 0l else row.getLong(5)
    val shares = if (row.isNullAt(6)) 0l else row.getLong(6)
    val contents = if (row.isNullAt(7)) 0l else row.getLong(7)
    (ymd,ap,searchText,count,no_results,views,shares,contents)
  }.toDF("ymd", "term", "ap", "count", "no_results", "views", "shares", "contents")



// delete existing rows
// list of days in the new stats report
val datesList = resultsDF.select($"ymd").distinct().collect()
val table = "search_performance"

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

  resultsDF
    .write
    .mode(saveMode)
    .jdbc(jdbcDipCts1ProductAnalytics, table, new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")
} else {
  display(resultsDF)
}



// COMMAND ----------

val conxSharesDF = conxDF
  .filter($"at" === "s") 

val searchxRawDF = searchxDF
  .select("cs", "stxt", "dt", "ap") //, "de", "ag",  "la"
  .filter($"stxt".isNotNull)
  .map {row =>
    val searchId = row.getString(0)
    val searchText = row.getString(1)
    var utcActionTimestamp = extractTime(row, 2)
    val ap = row.getString(3)
    var dayTimestamp = new DateTime(utcActionTimestamp)
    dayTimestamp = new DateTime(dayTimestamp.getYear, dayTimestamp.getMonthOfYear, dayTimestamp.getDayOfMonth, 0, 0)
    (new Timestamp(dayTimestamp.getMillis), searchId, searchText, ap)
  }
  .toDF("ymd", "searchId", "term", "ap")


// views/shares
val searchxUniqueContentDf = searchxRawDF.join(conxSharesDF,
  searchxRawDF.col("searchId") === conxSharesDF.col("cs") && searchxRawDF.col("ap") === conxSharesDF.col("ap"),
  "right_outer"
).filter($"term".isNotNull)
.drop(conxSharesDF.col("ap"))
.select("ymd", "term", "ap", "co")
.distinct()

val searchContentsDF = searchxUniqueContentDf
.rdd
.map{ row =>
    val ymd = row.getTimestamp(0)
    val searchText = row.getString(1)
    val ap = row.getString(2)
    ((ymd, searchText, ap), 1l)
}
.reduceByKey((a, b) => a + b)
.map { row =>
  val ymd = row._1._1
  val searchText = row._1._2
  val appId = row._1._3
  val contentNumber = row._2
  (ymd, appId, searchText, contentNumber)
}
.toDF("ymd","ap","term","shared_contents")


val searchxUniqueDF = searchxRawDF
.select("ymd", "term", "ap")
.distinct()

val resultsDF = searchxUniqueDF
  .join(searchContentsDF, Seq("ymd","ap", "term"),"left_outer")
  .select("ymd", "term", "ap", "shared_contents")
  .rdd
  .map{ row =>
    val ymd = row.getTimestamp(0)
    val ap = row.getString(1)
    val searchText = row.getString(2)
    val shared_contents = if (row.isNullAt(3)) 0l else row.getLong(3)
    (ymd,ap,searchText,shared_contents)
  }.toDF("ymd", "term", "ap", "shared_contents")
  .filter($"shared_contents" > 0)



// delete existing rows
// list of days in the new stats report
val datesList = resultsDF.select($"ymd").distinct().collect()
val table = "search_content"

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

  resultsDF
    .write
    .mode(saveMode)
    .jdbc(jdbcDipCts1ProductAnalytics, table, new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")
} else {
  display(resultsDF)
}



// COMMAND ----------

dbutils.notebook.exit("success")