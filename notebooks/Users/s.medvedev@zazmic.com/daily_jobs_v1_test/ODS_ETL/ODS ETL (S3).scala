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

val saveMode = SaveMode.Append //SaveMode.Overwrite - we need this mode to create mysql table

val s3Path = getS3PathForDates(tsFrom, tsTo, 0)

// COMMAND ----------

val contentDF = spark.read.jdbc(jdbcUrlCMS, "content", new Properties()).select("content_id","name").toDF("content_id","content_name")
val appsDF = spark.read.jdbc(jdbcUrlPUB, "pub_platform", new Properties()).toDF().select("pub_platform_id","name").toDF("app_id","app_name")



// COMMAND ----------

import java.util.TimeZone
import org.joda.time.{DateTime, DateTimeZone}
import java.sql.Timestamp
import org.apache.spark.util.StatCounter

val conxDF = sqlContext.read.json(getS3PathsForType(s3Folder, s3Path, "conx"):_*) 

val conxRDDRaw = conxDF
.filter($"co".isNotNull)
.select("ap", "co", "at", "dt")  

val conxRdd = conxRDDRaw.rdd 

val conxHourlyByContentDF = conxRdd.map { row =>
  val ap = row.getString(0)
  val co = row.getString(1)
  val at = row.getString(2)
  var utcActionTimestamp = extractTime(row, 3)
  var hourTimestamp = new DateTime(utcActionTimestamp)
  hourTimestamp = new DateTime(hourTimestamp.getYear, hourTimestamp.getMonthOfYear, hourTimestamp.getDayOfMonth, hourTimestamp.getHourOfDay, 0)
  val dow = hourTimestamp.dayOfWeek().get
  val hour = hourTimestamp.getHourOfDay
  
  val view = if (at.equals("v")) 1l else 0
  val share = if (at.equals("s")) 1l else 0
  //cut off bogus dates
  if (hourTimestamp.isAfter(DateTime.now())) {
    null
  } else {
    ((new Timestamp(hourTimestamp.getMillis), ap, co, dow, hour), (view, share))
  }
}.filter(x=> x != null)
.reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
.map { row =>
  (row._1._1, row._1._2, row._1._3, row._1._4, row._1._5, row._2._1.toLong, row._2._2.toLong)
}
.toDF("date", "app_id", "content_id",  "dow", "hour", "views", "shares")

val conxHourlyByContentWithNamesDF = conxHourlyByContentDF
  .join(contentDF, contentDF.col("content_id") === conxHourlyByContentDF.col("content_id"), "left_outer")
  .drop(conxHourlyByContentDF.col("content_id"))
  .join(appsDF, appsDF.col("app_id") === conxHourlyByContentDF.col("app_id"), "left_outer")
  .drop(appsDF.col("app_id")).drop(conxHourlyByContentDF.col("app_id"))


// // delete existing rows
// // list of days in the new stats report
val datesList = conxHourlyByContentWithNamesDF.select($"date").distinct().collect()

if (storeResultsToMySQL) {
  import org.apache.spark.sql.execution.datasources.jdbc._
  val tableName = "content_performance"
  val deleteQuery = s"DELETE FROM $tableName WHERE date=?;"
  datesList.foreach { d =>
    val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1,tableName,Map()))()
    try {
      val stmt = mysqlConnection.prepareStatement(deleteQuery)
      stmt.setTimestamp(1, d(0).asInstanceOf[Timestamp])
      stmt.execute()
      println(s"Deleted records from $tableName for day=$d")
    }
    finally {
      if (null != mysqlConnection) {
        mysqlConnection.close()
      }
    }  
  }

  //write to MySQL
  print(s"Starting writing results to $tableName...")

  val startTs = DateTime.now()
  conxHourlyByContentWithNamesDF
  .write.mode(saveMode)
  .jdbc(jdbcDipCts1, tableName, new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")
} else {
  display(conxHourlyByContentWithNamesDF)
}




// COMMAND ----------

import org.apache.spark.sql.types._
// //APPX
val tsToDay = new DateTime(tsTo)
val tsToDayMs =  new DateTime(tsToDay.getYear, tsToDay.getMonthOfYear, tsToDay.getDayOfMonth, 0, 0).getMillis

val from30DaysAgoMs = new DateTime(tsTo).minusDays(30).getMillis
val from7DaysAgoMs = new DateTime(tsTo).minusDays(7).getMillis
val from1DayAgoMs = new DateTime(tsTo).minusDays(1).getMillis

val s3Path30DaysAgo = getS3PathForDates(from30DaysAgoMs, tsTo, 0)
val appxDF = sqlContext.read
  .schema(StructType(List(
        StructField("ap", StringType),
        StructField("at", StringType),
        StructField("de", StringType),
        StructField("dt", LongType),
        StructField("v", IntegerType),
        StructField("kv", DoubleType),
        StructField("mv", DoubleType)
      )))
  .json(getS3PathsForType(s3Folder, s3Path30DaysAgo, "appx"):_*)  
  .filter(
      $"v" > 2 || 
      ($"v" === 2 && 
         (($"mv".isNotNull && $"mv" >= 3.6) || $"kv" >= 3.6 ))
    )

val appxRDDRaw = appxDF
.filter($"at" === "a") //$"dt" >= from30DaysAgoMs && $"dt" <= tsTo && 
.select("ap", "dt", "de")

// appxDF.cache()

val deviceCountFor30DaysDF = appxDF
  .filter($"at" === "a" && $"v" > 1)   //$"dt" >= from30DaysAgoMs && $"dt" <= tsTo &&   // && $"v" > 1
  .select("ap", "de")
  .rdd 
  .map { row =>  //do we need this at all??
      val ap = row.getString(0)
      val de = row.getString(1)
      (ap, de)
  }
  .distinct()
  .toDF
  .rdd
  .map { row =>
    val ap = row.getString(0)
   (ap, 1l)
  }.reduceByKey((a, b) => a + b)
  .map { row =>
    (new Timestamp(tsToDayMs), row._1, row._2, 30)
  }
  .toDF("date", "app_id", "count", "length")


val deviceCountFor7DaysDF = appxDF
  .filter($"dt" >= from7DaysAgoMs && $"at" === "a" && $"v" > 1) 
  .select("ap", "de")
  .rdd 
  .map { row =>
      val ap = row.getString(0)
      val de = row.getString(1)
      (ap, de)
  }
  .distinct()
  .toDF
  .rdd
  .map { row =>
    val ap = row.getString(0)
   (ap, 1l)
  }.reduceByKey((a, b) => a + b)
  .map { row =>
    (new Timestamp(tsToDayMs), row._1, row._2, 7)
  }
  .toDF("date", "app_id", "count", "length")


val deviceCountFor1DayDF = appxDF
  .filter($"dt" >= from1DayAgoMs && $"at" === "a" && $"v" > 1) 
  .select("ap", "de")
  .rdd 
  .map { row =>
      val ap = row.getString(0)
      val de = row.getString(1)
      (ap, de)
  }
  .distinct()
  .toDF
  .rdd
  .map { row =>
    val ap = row.getString(0)
   (ap, 1l)
  }.reduceByKey((a, b) => a + b)
  .map { row =>
    (new Timestamp(tsToDayMs), row._1, row._2, 1)
  }
  .toDF("date", "app_id", "count", "length")

val deviceCountDF = deviceCountFor30DaysDF.union(deviceCountFor7DaysDF).union(deviceCountFor1DayDF)

val deviceCountWithNamesDF = deviceCountDF
  .join(appsDF, appsDF.col("app_id") === deviceCountDF.col("app_id"))
  .drop(deviceCountDF.col("app_id"))
  .select("date","app_id","app_name","count","length")


// // delete existing rows
// // list of days in the new stats report
val datesList = deviceCountWithNamesDF.select($"date").distinct().collect()
if (storeResultsToMySQL) {
  import org.apache.spark.sql.execution.datasources.jdbc._

  val deleteQuery1 = "DELETE FROM device_counts WHERE date=?;"
  datesList.foreach { d =>
    val mysqlConnection1 = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1,"device_counts",Map()))()
    try {
      val stmt = mysqlConnection1.prepareStatement(deleteQuery1)
      stmt.setTimestamp(1, d(0).asInstanceOf[Timestamp])
      stmt.execute()
      println(s"Deleted records from device_counts for day=$d")
    }
    finally {
      if (null != mysqlConnection1) {
        mysqlConnection1.close()
      }
    }  
  }



  //write to MySQL
  print("Starting writing results to device_counts...")

  val startTs = DateTime.now()
  deviceCountWithNamesDF
  .write.mode(saveMode)
  .jdbc(jdbcDipCts1, "device_counts", new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")
} else {
  display(deviceCountWithNamesDF)
}


// COMMAND ----------

import java.sql.Timestamp
import org.apache.spark.util.StatCounter


val appx2DF = sqlContext.read.json(getS3PathsForType(s3Folder, s3Path, "appx"):_*)

val appxRDDRaw2 = appx2DF
//.filter($"dt" >= tsFrom && $"dt" <= tsTo)
.select("ap", "de", "ss", "at", "dt")


// val appxRDDRaw2 = sqlContext.read.format("org.elasticsearch.spark.sql")
//   .option("es.nodes", resolveHostToIpIfNecessary(esHost))
//   .option("es.port", "9200")
//   .option("es.nodes.wan.only", "true")
//   .option("es.query", s"""{"bool": { "must": [ { "range": { "as": { "gte": ${tsFrom}, "lte": ${tsTo} } } } ] }}""")
//   .load(s"$esIndexes/appx")
//   .select("ap", "de", "ss", "at", "as")
// //todo we need to use dt here

val apppxRdd1 = appxRDDRaw2.rdd //sc.parallelize(appxRDDRaw.take(100000)) //
val sessionsHourlyByDeviceDF = apppxRdd1.map { row =>
  if (!row.isNullAt(0) && !row.isNullAt(1) && !row.isNullAt(2) && !row.isNullAt(3) && !row.isNullAt(4)) {
    val ap = row.getString(0)
    val de = row.getString(1)
    val ss = row.getString(2)
    val at = row.getString(3)
    var utcActionTimestamp = extractTime(row, 4)
    var hourTimestamp = new DateTime(utcActionTimestamp)
    hourTimestamp = new DateTime(hourTimestamp.getYear, hourTimestamp.getMonthOfYear, hourTimestamp.getDayOfMonth, hourTimestamp.getHourOfDay, 0)
    val dow = hourTimestamp.dayOfWeek().get
    val hour = hourTimestamp.getHourOfDay
    
    val range = if (at.equals("a")) {
      (utcActionTimestamp, -1l, -1l)
    } else {
      (-1l, utcActionTimestamp, -1l)
    }
    
    //cut off bogus dates
    if (hourTimestamp.isAfter(DateTime.now())) {
      null
    } else {
      ((ap, de, new Timestamp(hourTimestamp.getMillis), ss, dow, hour), range)
    }  
  } else {
    null
  }
}.filter(x=> x != null && x._1 != null)
  .reduceByKey { (a: Tuple3[Long, Long, Long], b: Tuple3[Long, Long, Long]) =>
    if (a == null || b == null) {
      null
    } else if (a._1 > 0) {
      (a._1, b._2, b._2 - a._1)
    } else if (b._1 > 0) {
      (b._1, a._2, a._2 - b._1)
    } else null
  }
   .filter(_._2 != null)
  .map { row =>
    val session = if (row._2._3>0) {
      row._2._3
    } else {
      30000 //default session
    }
    (row._1._1, row._1._2, row._1._3, row._1._5, row._1._6, session.toLong) //(ap,de,ts),sessionLength
  }
  .toDF("ap", "de", "ts", "dow", "hour", "session")

sessionsHourlyByDeviceDF.write.mode(SaveMode.Overwrite).saveAsTable("sessionsHourlyByDeviceDF")

val sessionsHourlyDF = sqlContext.sql("select * from sessionsHourlyByDeviceDF").rdd.map { row =>
  val ap = row.getString(0)
  val ts = extractTime(row, 2)
  val dow = row.getInt(3)
  val hour = row.getInt(4)
  val session = row.getLong(5)
  
  //cut off bogus dates
  if (new DateTime(ts).isAfter(DateTime.now())) {
    null
  } else {
   ((ap, new Timestamp(ts), dow, hour), session)
  }
}.filter(x=> x != null)
  .groupByKey
  .mapValues { value =>
    val sessionCounter = StatCounter(value.map(_.toDouble))
    ((sessionCounter.sum / sessionCounter.count).toLong, sessionCounter.count.toLong)
  }
  .map { row =>
    (row._1._1, row._1._2,
      row._1._3,row._1._4, //dow, hour
      (row._2._1/1000).toLong, //avg session
      row._2._2 //count
    )
  }
.toDF("ap", "ts", "dow", "hour", "avg_length", "count")

sessionsHourlyDF.write.mode(SaveMode.Overwrite).saveAsTable("sessionsHourlyDF")

val sessionsHourlyDFRead = sqlContext.sql("select * from sessionsHourlyDF")
val sessionsHourlyWithNamesDF = sessionsHourlyDFRead
  .join(appsDF, appsDF.col("app_id") === sessionsHourlyDFRead.col("ap"))
  .drop(sessionsHourlyDFRead.col("ap"))
  .select("ts","app_id","app_name", "dow", "hour","count","avg_length")
  .toDF("date","app_id","app_name", "dow", "hour","count","avg_length")

// delete existing rows
// list of days in the new stats report
val datesList = sessionsHourlyWithNamesDF.select($"date").distinct().collect()

if (storeResultsToMySQL) {
  import org.apache.spark.sql.execution.datasources.jdbc._

  val deleteQuery2 = "DELETE FROM session_counts WHERE date=?;"
    datesList.foreach { d =>
    val mysqlConnection2 = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1,"session_counts",Map()))()
    try {
      val stmt = mysqlConnection2.prepareStatement(deleteQuery2)
      stmt.setTimestamp(1, d(0).asInstanceOf[Timestamp])
      stmt.execute()
      println(s"Deleted records from device_counts for day=$d")
    }
    finally {
      if (null != mysqlConnection2) {
        mysqlConnection2.close()
      }
    }  
  }


  //write to MySQL
  print("Starting writing results to session_counts...")

  val startTs = DateTime.now()
  sessionsHourlyWithNamesDF
  .write.mode(saveMode)
  .jdbc(jdbcDipCts1, "session_counts", new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")

} else {
  display(sessionsHourlyWithNamesDF)
}


// COMMAND ----------

dbutils.notebook.exit("success")