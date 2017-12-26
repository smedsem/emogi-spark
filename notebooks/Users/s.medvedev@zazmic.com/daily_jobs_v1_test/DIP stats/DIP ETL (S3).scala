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

val elementsDF = spark.read.jdbc(jdbcUrlDIP, "elements", new Properties()).drop("active").toDF("element_id","topic","category_id")
val categoriesDF = spark.read.jdbc(jdbcUrlDIP, "categories", new Properties()).drop("lang_model_id").toDF("category_id","category")
val appsDF = spark.read.jdbc(jdbcUrlPUB, "pub_platform", new Properties()).toDF().select("pub_platform_id", "name").toDF("app_id","app_name")

val categoriesSeq = Seq("ALL") ++ categoriesDF.map(_.getString(1)).collect().toSeq
val appsSeq = Seq("ALL") ++ appsDF.map(_.getString(1)).collect().toSeq

// dbutils.widgets.removeAll()

val elementsFullDFNotFiltered = elementsDF.join(categoriesDF, elementsDF.col("category_id") === categoriesDF.col("category_id")).drop(categoriesDF.col("category_id"))
val elementsFullDF = elementsFullDFNotFiltered.filter(!(
      $"category" === "Sentiment" || 
      $"category" === "Emoticons" || 
      $"topic" === "Not found" || 
      ($"category" === "Location" && ($"topic" === "Food" || $"topic" === "Well" || $"topic" === "Fair")) || 
      ($"category" === "Food" && $"topic" === "Feed")))

// display(appsDF)

// COMMAND ----------


// BY DAY/GEO
import com.datastax.spark.connector._
import java.util.{TimeZone,Date}
import org.joda.time.{DateTime, DateTimeZone}
import java.sql.Timestamp
import java.text.SimpleDateFormat

val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")

val cassandraRdd = sqlContext
    .read
    .json(getS3PathsForType(s3Folder, getS3PathForDates(tsFrom, tsTo, 1), "dst"):_*) 
    .select("ap", "el", "dt", "gp")
    .filter($"dt" >= tsFrom && $"dt" <= tsTo)


//pull Cassandra table as DF
val cassandraDF = cassandraRdd.map { row =>
  if (!row.isNullAt(0) && !row.isNullAt(1)  && !row.isNullAt(2)) {
    val ap = row.getString(0)
    val el = row.getString(1)
    val dt = row.getLong(2)
    
    val latLonTuple = if (row.isNullAt(3)) {
      (0.0, 0.0)
    } else {
      val coord = row.getString(3)
      if (coord.contains(",")) {
        val numbers = coord.split(",")
        (numbers(0).toDouble,numbers(1).toDouble)
      } else {
        (0.0, 0.0)
      }
    }
    
    val lat : Double = {
      val rounded = BigDecimal(latLonTuple._1).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble.toInt.toDouble
      if (rounded <= 90 && rounded >= -90) {
        rounded
      } else {
        0.0
      }
    }
    
    val lon : Double = {
      val rounded = BigDecimal(latLonTuple._2).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble.toInt.toDouble
      if (rounded <= 90 && rounded >= -90) {
        rounded
      } else {
        0.0
      }
    }

    var localActionTimestamp = new DateTime(dt) //.withZone(DateTimeZone.forID(timezone))
    
    val day = localActionTimestamp.getDayOfMonth
    val month = localActionTimestamp.getMonthOfYear
    val year = localActionTimestamp.getYear
    val dow = localActionTimestamp.dayOfWeek().get
    val hour = localActionTimestamp.getHourOfDay
    
    localActionTimestamp = new DateTime(year, month, day, hour, 0)

    (ap, el, new Timestamp(localActionTimestamp.getMillis), lat, lon, year, month, day, dow, hour)
  } else {
    null
  }
}.filter(_!=null)
 .toDF("ap", "el", "ts", "lat", "lon", "year", "month", "day", "dow", "hour")



// COMMAND ----------


// // BY DAY/GEO
// import com.datastax.spark.connector._
// import java.util.{TimeZone,Date}
// import org.joda.time.{DateTime, DateTimeZone}
// import java.sql.Timestamp
// import java.text.SimpleDateFormat

// val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S")

// // val cassandraRdd = sc.cassandraTable("emogi", "dst")
// //   .select("ap", "el", "dt", "gp")
// // //   .where("dt >= ?", sdf.format(tsFrom))  //TODO FIX THIS!!!
// // //   .where("dt <= ?", sdf.format(tsTo))
// //   .filter{x=>
// //     val dt = x.getDateOption("dt").orNull
// //     dt != null && dt.after(new Date(tsFrom)) && dt.before(new Date(tsTo))
// //   }


// val cassandraRdd = sqlContext
//     .read
//     .json(getS3PathsForType(s3Folder, getS3PathForDates(tsFrom, tsTo, 1), "dst"):_*) 
//     .select("ap", "el", "dt", "gp.lat", "gp.lng")
//     .filter($"dt" >= tsFrom && $"dt" <= tsTo)


// //pull Cassandra table as DF
// val cassandraDF = cassandraRdd.map { row =>
//   if (!row.isNullAt(0) && !row.isNullAt(1)  && !row.isNullAt(2)) {
//     val ap = row.getString(0)
//     val el = row.getString(1)
//     val dt = row.getLong(2)
    
//     val lat : Double = if (row.isNullAt(3)) {
//       0.0
//     } else {
//       val coord = row.getDouble(3)
//       val rounded = BigDecimal(coord).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble.toInt.toDouble
//       if (rounded <= 90 && rounded >= -90) {
//         rounded
//       } else {
//         0.0
//       }
//     }
    
//     val lon : Double = if (row.isNullAt(4)) {
//       0.0
//     } else {
//       val coord = row.getDouble(4)
//       val rounded = BigDecimal(coord).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble.toInt.toDouble
//       if (rounded <= 180 && rounded >= -180) {
//         rounded
//       } else {
//         0.0
//       }
//     }
    
// //     val gp = if (row.isNullAt(3)) {
// //       null 
// //     } else {  
// //         val geo = row.getString(3)
// //         val g = geo.replaceAll(" ","")
// //         val lat = g.substring(g.indexOf("lat:")+4, g.indexOf(",")).toDouble
// //         val lon = g.substring(g.indexOf("lng:")+4, g.lastIndexOf("}")).toDouble
// //         val latRounded = BigDecimal(lat).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble.toInt.toDouble
// //         val lonRounded = BigDecimal(lon).setScale(1, BigDecimal.RoundingMode.HALF_UP).toDouble.toInt.toDouble
// //         if (latRounded <= 90 && latRounded >= -90 && lonRounded <= 180 && lonRounded >= -180) (latRounded,lonRounded) else null
// //         (latRounded,lonRounded)
// //     }
// //     val lat : Double = if (gp!=null) gp._1 else -1
// //     val lon : Double = if (gp!=null) gp._2 else -1

// //     val timezoneOffset = -5 //todo read tz from devapp row.getIntOption("tz").getOrElse(-5) 
// //     val timezone = TimeZone.getAvailableIDs(timezoneOffset * 60 * 60 * 1000).head
//     var localActionTimestamp = new DateTime(dt) //.withZone(DateTimeZone.forID(timezone))
    
//     val day = localActionTimestamp.getDayOfMonth
//     val month = localActionTimestamp.getMonthOfYear
//     val year = localActionTimestamp.getYear
//     val dow = localActionTimestamp.dayOfWeek().get
//     val hour = localActionTimestamp.getHourOfDay
    
//     localActionTimestamp = new DateTime(year, month, day, hour, 0)

//     (ap, el, new Timestamp(localActionTimestamp.getMillis), lat, lon, year, month, day, dow, hour)
//   } else {
//     null
//   }
// }.filter(_!=null)
//  .toDF("ap", "el", "ts", "lat", "lon", "year", "month", "day", "dow", "hour")



// COMMAND ----------

// BY HOUR
import com.datastax.spark.connector._
import java.util.TimeZone
import org.joda.time.{DateTime, DateTimeZone}
import java.sql.Timestamp
import java.text.SimpleDateFormat

val topicCountByHourCountRdd = cassandraDF.map { row =>
  val ap = row.getString(0)
  val el = row.getString(1)
  val ts = row.getTimestamp(2)
  val year = row.getInt(5)
  val month = row.getInt(6)
  val day = row.getInt(7)
  val dow = row.getInt(8)
  val hour = row.getInt(9)
  ((ap, el, ts, year, month, day, dow, hour), 1l)
}.rdd
.reduceByKey((a, b) => a + b)
val topicCountByHourCountExpandedRdd = topicCountByHourCountRdd.map { row =>
  (row._1._1,row._1._2,row._1._3,row._1._4,row._1._5,row._1._6,row._1._7,row._1._8,row._2)
}
val topicCountByHourCountExpandedDF = topicCountByHourCountExpandedRdd.toDF("ap","el","ts","year","month","day","dow","hour","cnt")
val topicCountByHourCountWithNamesDFNotFiltered = topicCountByHourCountExpandedDF
  .join(elementsFullDF, elementsFullDF.col("element_id") === topicCountByHourCountExpandedDF.col("el"))
  .drop(topicCountByHourCountExpandedDF.col("el"))
  .join(appsDF, appsDF.col("app_id") === topicCountByHourCountExpandedDF.col("ap"))
  .drop(topicCountByHourCountExpandedDF.col("ap"))
val topicCountByHourCountWithNamesDF = topicCountByHourCountWithNamesDFNotFiltered.filter($"topic".isNotNull)

// topicCountByHourCountWithNamesDF.cache
// topicCountByHourCountWithNamesDF.write.mode(SaveMode.Overwrite).saveAsTable("topics_by_hour")
val topicCountByHourCountWithNamesDFResult = topicCountByHourCountWithNamesDF
.toDF("date","year","month","day","dow","hour","count","element_id","element_name","category_id","category_name","app_id","app_name")

// delete existing rows
// list of days in the new stats report
val datesList = topicCountByHourCountWithNamesDFResult.select($"year",$"month",$"day").distinct().collect()
val tableElCount = "dip_element_counts"

import org.apache.spark.sql.execution.datasources.jdbc._

if (storeResultsToMySQL) {

  val deleteQuery = s"DELETE FROM $tableElCount WHERE year=? and month=? and day=?;"
  
  val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1,tableElCount,scala.Predef.Map()))()
  try {
    datesList.foreach { d =>
      val stmt = mysqlConnection.prepareStatement(deleteQuery)
      stmt.setInt(1, d(0).asInstanceOf[Int])
      stmt.setInt(2, d(1).asInstanceOf[Int])
      stmt.setInt(3, d(2).asInstanceOf[Int])
      stmt.execute()
      println(s"Deleted records from $tableElCount for day=$d")
    }
  }
  finally {
    if (null != mysqlConnection) {
      mysqlConnection.close()
    }
  }


  //write to MySQL
  print(s"Starting writing results to $tableElCount...")

  val startTs = DateTime.now()
  topicCountByHourCountWithNamesDFResult
  .write.mode(saveMode)
  .jdbc(jdbcDipCts1, tableElCount, new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")
} else {
  display(topicCountByHourCountWithNamesDFResult)
}


// COMMAND ----------

// BY DAY/GEO
import com.datastax.spark.connector._
import java.util.TimeZone
import org.joda.time.{DateTime, DateTimeZone}
import java.sql.Timestamp
import java.text.SimpleDateFormat

val topicCountByDayGeoCountRdd = cassandraDF.filter($"lat">=0 && $"lon">=0).map { row =>
  val ap = row.getString(0)
  val el = row.getString(1)
  val ts = row.getTimestamp(2)
  val lat = row.getDouble(3)
  val lon = row.getDouble(4)
  val year = row.getInt(5)
  val month = row.getInt(6)
  val day = row.getInt(7)
  val dow = row.getInt(8)
  
  ((ap, el, ts, year, month, day, dow, lat, lon), 1l)
}.rdd
.reduceByKey((a, b) => a + b)
val topicCountByDayGeoCountExpandedRdd = topicCountByDayGeoCountRdd.map { row =>
  (row._1._1,row._1._2,row._1._3,row._1._4,row._1._5,row._1._6,row._1._7,row._1._8,row._1._9,row._2)
}
val topicCountByDayGeoCountExpandedDF = topicCountByDayGeoCountExpandedRdd
  .toDF("ap","el","ts","year","month","day","dow","lat","lon","cnt")
  .filter($"lat" > 0.0 && $"lon" > 0.0)
val topicCountByDayGeoCountWithNamesDFNotFiltered = topicCountByDayGeoCountExpandedDF
  .join(elementsFullDF, elementsFullDF.col("element_id") === topicCountByDayGeoCountExpandedDF.col("el"))
  .drop(topicCountByDayGeoCountExpandedDF.col("el"))
  .join(appsDF, appsDF.col("app_id") === topicCountByDayGeoCountExpandedDF.col("ap"))
  .drop(topicCountByDayGeoCountExpandedDF.col("ap"))
val topicCountByDayGeoCountWithNamesDF = topicCountByDayGeoCountWithNamesDFNotFiltered.filter($"topic".isNotNull)
// topicCountByDayGeoCountWithNamesDF.cache
// topicCountByDayGeoCountWithNamesDF.write.mode(SaveMode.Overwrite).saveAsTable("topics_by_day_geo")

// // delete existing rows
// // list of days in the new stats report
val datesListGeo = topicCountByDayGeoCountWithNamesDF.select($"year",$"month",$"day").distinct().collect()

if (storeResultsToMySQL) {
  import org.apache.spark.sql.execution.datasources.jdbc._

  val deleteQuery = "DELETE FROM dip_element_geo_counts WHERE year=? and month=? and day=?;"
  val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1,"dip_element_geo_counts",Map()))()
  try {
    datesListGeo.foreach { d =>
      val stmt = mysqlConnection.prepareStatement(deleteQuery)
      stmt.setInt(1, d(0).asInstanceOf[Int])
      stmt.setInt(2, d(1).asInstanceOf[Int])
      stmt.setInt(3, d(2).asInstanceOf[Int])
      stmt.execute()
      println(s"Deleted records from dip_element_geo_counts for day=$d")
    }
  }
  finally {
    if (null != mysqlConnection) {
      mysqlConnection.close()
    }
  }


  // //write to MySQL
  topicCountByDayGeoCountWithNamesDF
  .toDF("date","year","month","day","dow","lat", "lon","count","element_id","element_name","category_id","category_name","app_id","app_name")
  .write.mode(saveMode)
  .jdbc(jdbcDipCts1, "dip_element_geo_counts", new java.util.Properties())
} else {
  display(topicCountByDayGeoCountWithNamesDF)
}

// COMMAND ----------

import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import java.util.TimeZone
import java.sql.Timestamp
import scala.Option.{apply => ?}

val msgDF = sqlContext.read.json(getS3PathsForType(s3Folder, s3Path, "msg"):_*) 

val msgRDDRaw = msgDF
.filter($"dt" >= tsFrom && $"dt" <= tsTo)
.select("ap","de","mc","dt")

// val msgRDDRaw = sqlContext.read.format("org.elasticsearch.spark.sql")
// .option("es.nodes" , resolveHostToIpIfNecessary(esHost))
// .option("es.port","9200")
// .option("es.nodes.wan.only","true")
// .option("es.query", s"""{"bool": { "must": [ { "range": { "dt": { "gte": ${tsFrom.getTime}, "lte": ${tsTo.getTime} } } } ] }}""")
// .load("wink-imp-v2-*/msg")
// .select("ap","de","mc","dt")

val msgDailyByDeviceRDD = msgRDDRaw.rdd.map{ row =>
  var ap = row.getString(0)
  var mc = if (row.isNullAt(2)) 0 else row.get(2).toString.toInt
  var utcActionTimestamp = row.getLong(3)
  val timezoneOffset = -5 //todo read tz from devapp row.getIntOption("tz").getOrElse(-5) 
  val timezone = TimeZone.getAvailableIDs(timezoneOffset * 60 * 60 * 1000).head
  var hourTimestamp = new DateTime(utcActionTimestamp).withZone(DateTimeZone.forID(timezone))
  hourTimestamp = new DateTime(hourTimestamp.getYear, hourTimestamp.getMonthOfYear, hourTimestamp.getDayOfMonth, hourTimestamp.getHourOfDay, 0)

  val day = hourTimestamp.getDayOfMonth
  val month = hourTimestamp.getMonthOfYear
  val year = hourTimestamp.getYear
  val dowInt = hourTimestamp.dayOfWeek().get
  val hour = hourTimestamp.getHourOfDay

  ((ap,new Timestamp(hourTimestamp.getMillis), year, month, day, dowInt, hour),mc)
}.reduceByKey((a, b) => a + b)

val msgDailyByDeviceCountExpandedRdd = msgDailyByDeviceRDD.map { row =>
  (row._1._1,row._1._2,row._1._3,row._1._4,row._1._5,row._1._6,row._1._7,row._2)
}

val msgDailyByDeviceCountExpandedDF = msgDailyByDeviceCountExpandedRdd.toDF("ap","ts","year","month","day","dow","hour","cnt")
val msgDailyByDeviceCountWithNamesDF = msgDailyByDeviceCountExpandedDF
  .join(appsDF, appsDF.col("app_id") === msgDailyByDeviceCountExpandedDF.col("ap"))
  .drop(msgDailyByDeviceCountExpandedDF.col("ap"))
// msgDailyByDeviceCountWithNamesDF.cache
// msgDailyByDeviceCountWithNamesDF.write.mode(SaveMode.Overwrite).saveAsTable("messages_by_day")

// // delete existing rows
// // list of days in the new stats report
val datesListMsg = msgDailyByDeviceCountWithNamesDF.select($"year",$"month",$"day").distinct().collect()

if (storeResultsToMySQL) {
  import org.apache.spark.sql.execution.datasources.jdbc._

  val deleteQuery = "DELETE FROM dip_message_counts WHERE year=? and month=? and day=?;"
  val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1,"dip_message_counts",Map()))()
  try {
    datesListMsg.foreach { d =>
      val stmt = mysqlConnection.prepareStatement(deleteQuery)
      stmt.setInt(1, d(0).asInstanceOf[Int])
      stmt.setInt(2, d(1).asInstanceOf[Int])
      stmt.setInt(3, d(2).asInstanceOf[Int])
      stmt.execute()
      println(s"Deleted records from dip_message_counts for day=$d")
    }
  }
  finally {
    if (null != mysqlConnection) {
      mysqlConnection.close()
    }
  }

  msgDailyByDeviceCountWithNamesDF
  .toDF("date","year","month","day","dow","hour","count","app_id","app_name")
  .write.mode(saveMode)
  .jdbc(jdbcDipCts1, "dip_message_counts", new java.util.Properties())
} else {
  display(msgDailyByDeviceCountWithNamesDF)
}

// COMMAND ----------

dbutils.notebook.exit("success")