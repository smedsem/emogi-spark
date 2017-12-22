// Databricks notebook source
// MAGIC %run "/Users/s.medvedev@zazmic.com/daily_jobs_s3/Config (S3)"

// COMMAND ----------

// MAGIC %run "/Users/s.medvedev@zazmic.com/daily_jobs_s3/Utils"

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

val saveMode = SaveMode.Append //SaveMode.Overwrite - we need this mode to create mysql table

val s3Path = getS3PathForDates(tsFrom, tsTo, 0)

// COMMAND ----------

val elementsDF = spark.read.jdbc(jdbcUrlDIP, "elements", new Properties()).drop("active").toDF("element_id","topic","category_id")
val categoriesDF = spark.read.jdbc(jdbcUrlDIP, "categories", new Properties()).drop("lang_model_id").toDF("category_id","category")
val appsDF = spark.read.jdbc(jdbcUrlPUB, "pub_platform", new Properties()).toDF().select("pub_platform_id","name").toDF("app_id","app_name")

val categoriesSeq = Seq("ALL") ++ categoriesDF.map(_.getString(1)).collect().toSeq
val appsSeq = Seq("ALL") ++ appsDF.map(_.getString(1)).collect().toSeq

val elementsFullDFNotFiltered = elementsDF.join(categoriesDF, elementsDF.col("category_id") === categoriesDF.col("category_id")).drop(categoriesDF.col("category_id"))
val elementsFullDF = elementsFullDFNotFiltered.filter(!(
      $"category" === "Sentiment" || 
      $"category" === "Emoticons" || 
      $"topic" === "Not found" || 
      ($"category" === "Location" && ($"topic" === "Food" || $"topic" === "Well" || $"topic" === "Fair")) || 
      ($"category" === "Food" && $"topic" === "Feed")))

// display(appsDF)

// COMMAND ----------


import com.datastax.spark.connector._
import java.util.TimeZone
import java.util.Date
import org.joda.time.{DateTime, DateTimeZone}
import java.sql.Timestamp
import java.text.SimpleDateFormat

// clearCache()

val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")

// val cassandraRdd = sc.cassandraTable("emogi", "dst")
//   .select("ap", "el", "me", "dt")
// //   .limit(10000)
// //   .where("dt >= ?", sdf.format(tsFrom))  //TODO FIX THIS!!!
// //   .where("dt <= ?", sdf.format(tsTo))
//   .filter{row=>
//     val dt = row.getDateOption("dt").orNull
//     dt != null && dt.after(new Date(tsFrom)) && dt.before(new Date(tsTo))
//   }

val cassandraRdd = sqlContext
    .read
    .json(getS3PathsForType(s3Folder, getS3PathForDates(tsFrom, tsTo, 0), "dst"):_*) 
    .select("ap", "el", "me", "dt")

val cassandraDF = cassandraRdd.map { row =>
  if (!row.isNullAt(0) && !row.isNullAt(1)  && !row.isNullAt(2) && !row.isNullAt(3)) {
    val ap = row.getString(0)
    val el = row.getString(1)
    val me = row.getString(2)
    val dt = row.getLong(3)

//     val timezoneOffset = -5 //todo read tz from devapp row.getIntOption("tz").getOrElse(-5) 
//     val timezone = TimeZone.getAvailableIDs(timezoneOffset * 60 * 60 * 1000).head
    var dayTs = new DateTime(dt) //.withZone(DateTimeZone.forID(timezone))
    
    val year = dayTs.getYear
    val month = dayTs.getMonthOfYear
    val day = dayTs.getDayOfMonth
    val dow = dayTs.dayOfWeek().get
    val hour = dayTs.getHourOfDay
    
    dayTs = new DateTime(year, month, day, hour, 0)
    (ap, el, me, new Timestamp(dayTs.getMillis), year, month, day, dow, hour)
  } else {
    null
  }  
}.filter(_!=null)
 .toDF("ap", "el", "me", "ts", "year", "month", "day", "dow", "hour")

val elementsCountLevel1DF = cassandraDF.map { row =>
  val ap = row.getString(0)
  val el = row.getString(1)
  val ts = row.getTimestamp(3)
  val year = row.getInt(4)
  val month = row.getInt(5)
  val day = row.getInt(6)
  val dow = row.getInt(7)
  val hour = row.getInt(8)
  ((ap, el, ts, year, month, day, dow, hour), 1l)
}.rdd
.reduceByKey((a, b) => a + b)
.map{row =>
 (row._1._1,row._1._3,null.asInstanceOf[String], row._1._2,row._2, 1)
//  (row._1._1,row._1._3,row._1._4,row._1._5,row._1._6,row._1._7,row._1._8, null.asInstanceOf[String], row._1._2,row._2, 1)
}
.toDF("ap", "ts", "parent_el", "el", "count", "level")
// .toDF("ap", "ts", "year", "month", "day", "dow", "hour", "parent_el", "el", "count", "level")

val cassandraDF2 = cassandraDF

val elementsLevel2DF = cassandraDF.join(cassandraDF2,
  cassandraDF.col("ap") === cassandraDF2.col("ap") && cassandraDF.col("me") === cassandraDF2.col("me"),
  "right_outer"
).drop(cassandraDF.col("ap")).drop(cassandraDF2.col("me")).drop(cassandraDF.col("ts"))
.drop(cassandraDF.col("year")).drop(cassandraDF.col("month")).drop(cassandraDF.col("day")).drop(cassandraDF.col("dow")).drop(cassandraDF.col("hour"))
.toDF("parent_el", "ap", "el",  "me", "ts", "year", "month", "day", "dow", "hour")
.filter($"el" =!= $"parent_el")
// .sort($"parent_el".asc)

val elementsCountLevel2DF = elementsLevel2DF.map{row =>
  val parent_el = row.getString(0)
  val ap = row.getString(1)
  val el = row.getString(2)
  val ts = row.getTimestamp(4)
  val year = row.getInt(5)
  val month = row.getInt(6)
  val day = row.getInt(7)
  val dow = row.getInt(8)
  val hour = row.getInt(9)
   ((ap, parent_el, el, ts, year, month, day, dow, hour), 1l)
}.rdd
.reduceByKey((a, b) => a + b)
.map{row =>
//  (row._1._1,row._1._4,row._1._5,row._1._6,row._1._7,row._1._8,row._1._9, row._1._2, row._1._3,row._2, 2)
 (row._1._1,row._1._4, row._1._2, row._1._3,row._2, 2)
}
.toDF("ap", "ts", "parent_el", "el", "count", "level")
// .toDF("ap", "ts", "year", "month", "day", "dow", "hour", "parent_el", "el", "count", "level")
// .sort($"count".desc)

// val elementsLevel3DF = elementsLevel2DF.join(cassandraDF,
//   cassandraDF.col("ap") === elementsLevel2DF.col("ap") && cassandraDF.col("me") === elementsLevel2DF.col("me"),
//   "right_outer"
// ).drop(cassandraDF.col("ap")).drop(elementsLevel2DF.col("me")).drop(cassandraDF.col("ts")).drop(elementsLevel2DF.col("parent_el"))
// .toDF("ap","parent_el", "ts", "el",  "me")
// .filter($"ap".isNotNull && $"el" =!= $"parent_el")
// // .sort($"parent_el".asc)

// val elementsCountLevel3DF = elementsLevel3DF.map{row =>
//   val ap = row.getString(0)
//   val parent_el = row.getString(1)
//   val ts = row.getTimestamp(2)
//   val el = row.getString(3)
//   ((ap, parent_el, el, ts), 1l)
// }.rdd
// .reduceByKey((a, b) => a + b)
// .map{row =>
//  (row._1._1,row._1._4, row._1._2, row._1._3,row._2, 3)
// }.toDF("ap", "ts", "parent_el", "el", "count", "level")
// // .sort($"count".desc)

val unionDF = elementsCountLevel1DF.union(elementsCountLevel2DF) //.union(elementsCountLevel3DF)

val startTs = DateTime.now()

// delete existing rows
// list of days in the new stats report
val datesList = unionDF.select($"ts").distinct().collect()
val tableCircle = "dip_circle_element_counts"

if (storeResultsToMySQL) {
  print(s"Starting writing results to $tableCircle...")

  import org.apache.spark.sql.execution.datasources.jdbc._

  val deleteQuery = s"DELETE FROM $tableCircle WHERE ts=?;"
  val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1,tableCircle,Map()))()
  try {
    datesList.foreach { d =>
      val stmt = mysqlConnection.prepareStatement(deleteQuery)
      stmt.setTimestamp(1, d(0).asInstanceOf[Timestamp])
      stmt.execute()
      println(s"Deleted records from $tableCircle for day=$d")
    }
  }
  finally {
    if (null != mysqlConnection) {
      mysqlConnection.close()
    }
  }

  //write to MySQL

  unionDF
  .write.mode(SaveMode.Append)
  .jdbc(jdbcDipCts1, tableCircle, new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")
} else {
  display(unionDF)
}


// COMMAND ----------


// import com.datastax.spark.connector._
// import java.util.TimeZone
// import java.util.Date
// import org.joda.time.{DateTime, DateTimeZone}
// import java.sql.Timestamp
// import java.text.SimpleDateFormat

// // clearCache()

// val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")

// // val cassandraRdd = sc.cassandraTable("emogi", "dst")
// //   .select("ap", "el", "me", "dt")
// // //   .limit(10000)
// // //   .where("dt >= ?", sdf.format(tsFrom))  //TODO FIX THIS!!!
// // //   .where("dt <= ?", sdf.format(tsTo))
// //   .filter{row=>
// //     val dt = row.getDateOption("dt").orNull
// //     dt != null && dt.after(new Date(tsFrom)) && dt.before(new Date(tsTo))
// //   }

// val cassandraRdd = sqlContext
//     .read
//     .json(getS3PathsForType(s3Folder, getS3PathForDates(tsFrom, tsTo, 0), "dst"):_*) 
//     .select("ap", "el", "me", "dt")

// val cassandraDF = cassandraRdd.map { row =>
//   if (!row.isNullAt(0) && !row.isNullAt(1)  && !row.isNullAt(2) && !row.isNullAt(3)) {
//     val ap = row.getString(0)
//     val el = row.getString(1)
//     val me = row.getString(2)
//     val dt = row.getLong(3)
//     var dayTs = new DateTime(dt)  //.withZone(DateTimeZone.forID(timezone))
//     dayTs = new DateTime(dayTs.getYear, dayTs.getMonthOfYear, dayTs.getDayOfMonth, 0, 0)
//     (ap, el, me, new Timestamp(dayTs.getMillis))
//   } else {
//     null
//   }  
// }.filter(_!=null)
//  .toDF("ap", "el", "me", "ts")

// val elementsCountLevel1DF = cassandraDF.map { row =>
//   val ap = row.getString(0)
//   val el = row.getString(1)
//   val ts = row.getTimestamp(3)
//   ((ap, el, ts), 1l)
// }.rdd
// .reduceByKey((a, b) => a + b)
// .map{row =>
//  (row._1._1,row._1._3, null.asInstanceOf[String], row._1._2,row._2, 1)
// }.toDF("ap", "ts", "parent_el", "el", "count", "level")

// val cassandraDF2 = cassandraDF

// val elementsLevel2DF = cassandraDF.join(cassandraDF2,
//   cassandraDF.col("ap") === cassandraDF2.col("ap") && cassandraDF.col("me") === cassandraDF2.col("me"),
//   "right_outer"
// ).drop(cassandraDF.col("ap")).drop(cassandraDF2.col("me")).drop(cassandraDF.col("ts"))
// .toDF("parent_el", "ap", "el",  "me", "ts")
// .filter($"el" =!= $"parent_el")
// // .sort($"parent_el".asc)

// val elementsCountLevel2DF = elementsLevel2DF.map{row =>
//   val parent_el = row.getString(0)
//   val ap = row.getString(1)
//   val el = row.getString(2)
//   val ts = row.getTimestamp(4)
//    ((ap, parent_el, el, ts), 1l)
// }.rdd
// .reduceByKey((a, b) => a + b)
// .map{row =>
//  (row._1._1,row._1._4, row._1._2, row._1._3,row._2, 2)
// }.toDF("ap", "ts", "parent_el", "el", "count", "level")
// // .sort($"count".desc)

// // val elementsLevel3DF = elementsLevel2DF.join(cassandraDF,
// //   cassandraDF.col("ap") === elementsLevel2DF.col("ap") && cassandraDF.col("me") === elementsLevel2DF.col("me"),
// //   "right_outer"
// // ).drop(cassandraDF.col("ap")).drop(elementsLevel2DF.col("me")).drop(cassandraDF.col("ts")).drop(elementsLevel2DF.col("parent_el"))
// // .toDF("ap","parent_el", "ts", "el",  "me")
// // .filter($"ap".isNotNull && $"el" =!= $"parent_el")
// // // .sort($"parent_el".asc)

// // val elementsCountLevel3DF = elementsLevel3DF.map{row =>
// //   val ap = row.getString(0)
// //   val parent_el = row.getString(1)
// //   val ts = row.getTimestamp(2)
// //   val el = row.getString(3)
// //   ((ap, parent_el, el, ts), 1l)
// // }.rdd
// // .reduceByKey((a, b) => a + b)
// // .map{row =>
// //  (row._1._1,row._1._4, row._1._2, row._1._3,row._2, 3)
// // }.toDF("ap", "ts", "parent_el", "el", "count", "level")
// // // .sort($"count".desc)

// val unionDF = elementsCountLevel1DF.union(elementsCountLevel2DF) //.union(elementsCountLevel3DF)

// val startTs = DateTime.now()

// // delete existing rows
// // list of days in the new stats report
// val datesList = unionDF.select($"ts").distinct().collect()

// if (storeResultsToMySQL) {
//   print("Starting writing results to dip_circle_element_counts...")

//   import org.apache.spark.sql.execution.datasources.jdbc._

//   val deleteQuery = "DELETE FROM dip_circle_element_counts WHERE ts=?;"
//   val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1,"dip_circle_element_counts",Map()))()
//   try {
//     datesList.foreach { d =>
//       val stmt = mysqlConnection.prepareStatement(deleteQuery)
//       stmt.setTimestamp(1, d(0).asInstanceOf[Timestamp])
//       stmt.execute()
//       println(s"Deleted records from dip_circle_element_counts for day=$d")
//     }
//   }
//   finally {
//     if (null != mysqlConnection) {
//       mysqlConnection.close()
//     }
//   }

//   //write to MySQL

//   unionDF
//   .write.mode(SaveMode.Append)
//   .jdbc(jdbcDipCts1, "dip_circle_element_counts", new java.util.Properties())

//   val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
//   println(s" - DONE, it took $durationMinutes minutes")
// } else {
//   display(unionDF)
// }


// COMMAND ----------

dbutils.notebook.exit("success")
