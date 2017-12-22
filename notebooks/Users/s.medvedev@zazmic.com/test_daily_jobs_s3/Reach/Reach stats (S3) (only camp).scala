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

val saveMode = SaveMode.Append //SaveMode.Overwrite // // // // // - we need this mode to create mysql table


// COMMAND ----------

val contentFlightQuery = """(select c.content_id as content_id, max(fp.start_ts) as ts_from, min(fp.end_ts) as ts_to from content c, xpla_content xc, xpla xp, flight_plan fp where xc.content_id=c.content_id and xc.xpla_id=xp.xpla_id and xp.flight_plan_id=fp.flight_plan_id group by c.content_id) flight"""
val contentFlightPlansDF = spark.read.jdbc(jdbcUrlCMS, contentFlightQuery, new Properties())
contentFlightPlansDF.cache()

val campFlightQuery = """(select camp_id, start_ts as ts_from, end_ts as ts_to from camp) flight"""
val campFlightPlansDF = spark.read.jdbc(jdbcUrlCMS, campFlightQuery, new Properties())
campFlightPlansDF.cache()


// COMMAND ----------

import org.apache.spark.sql.DataFrame

def extractContentReachStats(conxDataDF: DataFrame, targetDayTimestamp:Timestamp) : DataFrame = {
  
  // Preparing data
  val conxDF = conxDataDF
    .select("ap", "at", "co", "ch", "av", "de", "dt")
    .rdd
    .map { row =>
        val ap = row.getString(0)
        val at = row.getString(1)
        val co = row.getString(2)
        val ch = if (!row.isNullAt(3)) row.getString(3) else null 
        val av = if (!row.isNullAt(4)) row.getString(4) else null
        val de = row.getString(5)   
        var dayTimestamp = new DateTime(extractTime(row, 6))
        //cut off bogus dates
        if (dayTimestamp.isAfter(DateTime.now())) {
          null
        } else {
          (ap, at, av, co, ch, de, targetDayTimestamp)
        }
    }
    .filter(x => x != null)
    .toDF("ap", "at", "av", "co", "ch", "de", "date")

  //views/shares and caching to the memory
  val viewsAndSharesDF = conxDF.filter($"at" === "v" || $"at" === "s")
  viewsAndSharesDF.cache

  //shares
  val sharesDF = viewsAndSharesDF.filter($"at" === "s")

  // distinct devices that viewed/shared content
  val distinctViewsDF = viewsAndSharesDF
    .select("date", "ap", "av", "co", "de")
    .distinct()

  // count of devices that viewed/shared content
  val df1Views = distinctViewsDF
    .rdd
    .map { row =>
      val date = row.getTimestamp(0)
      val ap = row.getString(1)
      val av = if (!row.isNullAt(2)) row.getString(2) else null
      val co = row.getString(3) 
      ((date, ap, av, co), 1l)
    }.reduceByKey((a, b) =>  a+b)
    .map { row =>
      (row._1._1, row._1._2, row._1._3, row._1._4, row._2, row._2)
    }
    .toDF("date", "app_id", "adv_id", "content_id", "unique_views", "paid_reach")

   // distinct devices that shared content
  val distinctSharesDF = sharesDF
    .select("date", "ap", "av", "co", "de")
    .distinct()

   // count of devices that shared content
  val df2Shares = distinctSharesDF
    .rdd
    .map { row =>
      val date = row.getTimestamp(0)
      val ap = row.getString(1)
      val av = if (!row.isNullAt(2)) row.getString(2) else null
      val co = row.getString(3) 
      ((date, ap, av, co), 1l)
    }.reduceByKey((a, b) =>  a+b)
    .map { row =>
      (row._1._1, row._1._2, row._1._3, row._1._4, row._2)
    }
    .toDF("date2", "app_id2", "adv_id2", "content_id2", "unique_shares")


  //chat readers
  val chatReadersDF = conxDF
    .filter($"ch".isNotNull)
    .select("ap","ch","de")
    .toDF("ap1","ch1","de1")
    .distinct()

  //content shares joined with chat readers, excepts original shares
 val receivesJoinedDF = sharesDF
   .join(
      chatReadersDF, 
      sharesDF.col("ap") === chatReadersDF.col("ap1") &&
      sharesDF.col("ch") === chatReadersDF.col("ch1") &&
      sharesDF.col("de") =!= chatReadersDF.col("de1"),
      "left_outer"
  )

  //distinct devices that received content
  val receivesDF = receivesJoinedDF
    .select("date", "ap", "av", "co", "de1")
    .toDF("date", "ap", "av", "co", "de")
    .distinct()

  //count of distinct devices that received content
  val df3Receives = receivesDF
    .rdd
    .map { row =>
      val date = row.getTimestamp(0)
      val ap = row.getString(1)
      val av = if (!row.isNullAt(2)) row.getString(2) else null
      val co = row.getString(3) 
      ((date, ap, av, co), 1l)
    }.reduceByKey((a, b) =>  a + b)
    .map { row =>
      (row._1._1, row._1._2, row._1._3, row._1._4, row._2)
    }
    .toDF("date3", "app_id3", "adv_id3", "content_id3", "unique_receives")

  //distinct devices that received content combined with distinct devices that viewed/shared content
   val totalReachDF = receivesDF
      .union(distinctViewsDF)
      .distinct()

  //count of devices that received + viewed/shared
  val df4TotalReach = totalReachDF
    .rdd
    .map { row =>
      val date = row.getTimestamp(0)
      val ap = row.getString(1)
      val av = if (!row.isNullAt(2)) row.getString(2) else null
      val co = row.getString(3) 
      ((date, ap, av, co), 1l)
    }.reduceByKey((a, b) =>  a + b)
    .map { row =>
      (row._1._1, row._1._2, row._1._3, row._1._4, row._2)
    }
    .toDF("date4", "app_id4", "adv_id4", "content_id4", "total_reach")

  //distinct devices that received content minus devices that viewed/shared content
  val earnedReachDF = receivesDF
    .except(distinctViewsDF)
    .distinct() //may be not needed here

  //count of devices that received content but not viewed/shared it
  val df5EarnedReach = earnedReachDF
    .rdd
    .map { row =>
      val date = row.getTimestamp(0)
      val ap = row.getString(1)
      val av = if (!row.isNullAt(2)) row.getString(2) else null
      val co = row.getString(3) 
      ((date, ap, av, co), 1l)
    }.reduceByKey((a, b) =>  a + b)
    .map { row =>
      (row._1._1, row._1._2, row._1._3, row._1._4, row._2)
    }
    .toDF("date5", "app_id5", "adv_id5", "content_id5", "earned_reach")

  //Grouping results into one table
  val resultsDF1 = df1Views
    .join(df2Shares,
         df1Views.col("date") === df2Shares.col("date2") &&
         df1Views.col("app_id") === df2Shares.col("app_id2") &&
         ((df1Views.col("adv_id").isNull && df2Shares.col("adv_id2").isNull) || (df1Views.col("adv_id") === df2Shares.col("adv_id2"))) &&
         df1Views.col("content_id") === df2Shares.col("content_id2"),
          "left_outer"
    )

  val resultsDF2 = resultsDF1
    .join(df3Receives,
         resultsDF1.col("date") === df3Receives.col("date3") &&
         resultsDF1.col("app_id") === df3Receives.col("app_id3") &&
         ((resultsDF1.col("adv_id").isNull && df3Receives.col("adv_id3").isNull) || (resultsDF1.col("adv_id") === df3Receives.col("adv_id3"))) &&
         resultsDF1.col("content_id") === df3Receives.col("content_id3"),
          "left_outer"
    )

  val resultsDF3 = resultsDF2
    .join(df4TotalReach,
         resultsDF2.col("date") === df4TotalReach.col("date4") &&
         resultsDF2.col("app_id") === df4TotalReach.col("app_id4") &&
         ((resultsDF2.col("adv_id").isNull && df4TotalReach.col("adv_id4").isNull) || (resultsDF2.col("adv_id") === df4TotalReach.col("adv_id4"))) &&
         resultsDF2.col("content_id") === df4TotalReach.col("content_id4"),
          "left_outer"
    )

  val resultsDF4 = resultsDF3
    .join(df5EarnedReach,
         resultsDF3.col("date") === df5EarnedReach.col("date5") &&
         resultsDF3.col("app_id") === df5EarnedReach.col("app_id5") &&
         ((resultsDF3.col("adv_id").isNull && df5EarnedReach.col("adv_id5").isNull) || (resultsDF3.col("adv_id") === df5EarnedReach.col("adv_id5"))) &&
         resultsDF3.col("content_id") === df5EarnedReach.col("content_id5"),
          "left_outer"
    )

  val resultsDF = resultsDF4
      .select("date", "app_id", "adv_id", "content_id", "total_reach", "paid_reach", "earned_reach", "unique_views", "unique_shares", "unique_receives")
      .rdd
      .map { row =>
        val date = row.getTimestamp(0)
        val app_id = row.getString(1)
        val adv_id = if (!row.isNullAt(2)) row.getString(2) else null
        val content_id = row.getString(3) 
        val total_reach = if (!row.isNullAt(4)) row.getLong(4) else 0l
        val paid_reach = if (!row.isNullAt(5)) row.getLong(5) else 0l
        val earned_reach = if (!row.isNullAt(6)) row.getLong(6) else 0l
        val unique_views = if (!row.isNullAt(7)) row.getLong(7) else 0l
        val unique_shares = if (!row.isNullAt(8)) row.getLong(8) else 0l
        val unique_receives = if (!row.isNullAt(9)) row.getLong(9) else 0l
        (date, app_id, adv_id, content_id, total_reach, paid_reach, earned_reach, unique_views, unique_shares, unique_receives, adv_id!=null)
      }
     .toDF("date", "app_id", "adv_id", "content_id", "total_reach", "paid_reach","earned_reach", "unique_views", "unique_shares", "unique_receives", "is_branded")

  resultsDF
}

// COMMAND ----------

import org.apache.spark.sql.DataFrame

def extractCampaignReachStats(conxDataDF: DataFrame, targetDayTimestamp:Timestamp) : DataFrame = {
  
  // Preparing data
  val conxDF = conxDataDF
    .select("ap", "at", "ca", "ch", "av", "de", "dt")
    .rdd
    .map { row =>
        val ap = row.getString(0)
        val at = row.getString(1)
        val ca = if (!row.isNullAt(2)) row.getString(2) else null 
        val ch = if (!row.isNullAt(3)) row.getString(3) else null 
        val av = if (!row.isNullAt(4)) row.getString(4) else null
        val de = row.getString(5)   
        var dayTimestamp = new DateTime(extractTime(row, 6))
        //cut off bogus dates
        if (dayTimestamp.isAfter(DateTime.now())) {
          null
        } else {
          (ap, at, av, ca, ch, de, targetDayTimestamp)
        }
    }
    .filter(x => x != null)
    .toDF("ap", "at", "av", "ca", "ch", "de", "date")

  //views/shares and caching to the memory
  val viewsAndSharesDF = conxDF.filter($"at" === "v" || $"at" === "s")
  viewsAndSharesDF.cache

  //shares
  val sharesDF = viewsAndSharesDF.filter($"at" === "s")

  // distinct devices that viewed/shared content
  val distinctViewsDF = viewsAndSharesDF
    .select("date", "ap", "av", "ca", "de")
    .distinct()

  // count of devices that viewed/shared content
  val df1Views = distinctViewsDF
    .rdd
    .map { row =>
      val date = row.getTimestamp(0)
      val ap = row.getString(1)
      val av = if (!row.isNullAt(2)) row.getString(2) else null
      val ca = if (!row.isNullAt(3)) row.getString(3) else null
      ((date, ap, av, ca), 1l)
    }.reduceByKey((a, b) =>  a+b)
    .map { row =>
      (row._1._1, row._1._2, row._1._3, row._1._4, row._2, row._2)
    }
    .toDF("date", "app_id", "adv_id", "camp_id", "unique_views", "paid_reach")

   // distinct devices that shared content
  val distinctSharesDF = sharesDF
    .select("date", "ap", "av", "ca", "de")
    .distinct()

   // count of devices that shared content
  val df2Shares = distinctSharesDF
    .rdd
    .map { row =>
      val date = row.getTimestamp(0)
      val ap = row.getString(1)
      val av = if (!row.isNullAt(2)) row.getString(2) else null
      val ca = if (!row.isNullAt(3)) row.getString(3) else null
      ((date, ap, av, ca), 1l)
    }.reduceByKey((a, b) =>  a+b)
    .map { row =>
      (row._1._1, row._1._2, row._1._3, row._1._4, row._2)
    }
    .toDF("date2", "app_id2", "adv_id2", "camp_id2", "unique_shares")


  //chat readers
  val chatReadersDF = conxDF
    .filter($"ch".isNotNull)
    .select("ap","ch","de")
    .toDF("ap1","ch1","de1")
    .distinct()

  //content shares joined with chat readers, excepts original shares
 val receivesJoinedDF = sharesDF
   .join(
      chatReadersDF, 
      sharesDF.col("ap") === chatReadersDF.col("ap1") &&
      sharesDF.col("ch") === chatReadersDF.col("ch1") &&
      sharesDF.col("de") =!= chatReadersDF.col("de1"),
      "left_outer"
  )

  //distinct devices that received content
  val receivesDF = receivesJoinedDF
    .select("date", "ap", "av", "ca", "de1")
    .toDF("date", "ap", "av", "ca", "de")
    .distinct()

  //count of distinct devices that received content
  val df3Receives = receivesDF
    .rdd
    .map { row =>
      val date = row.getTimestamp(0)
      val ap = row.getString(1)
      val av = if (!row.isNullAt(2)) row.getString(2) else null
      val ca = if (!row.isNullAt(3)) row.getString(3) else null 
      ((date, ap, av, ca), 1l)
    }.reduceByKey((a, b) =>  a + b)
    .map { row =>
      (row._1._1, row._1._2, row._1._3, row._1._4, row._2)
    }
    .toDF("date3", "app_id3", "adv_id3", "camp_id3", "unique_receives")

  //distinct devices that received content combined with distinct devices that viewed/shared content
   val totalReachDF = receivesDF
      .union(distinctViewsDF)
      .distinct()

  //count of devices that received + viewed/shared
  val df4TotalReach = totalReachDF
    .rdd
    .map { row =>
      val date = row.getTimestamp(0)
      val ap = row.getString(1)
      val av = if (!row.isNullAt(2)) row.getString(2) else null
      val ca = if (!row.isNullAt(3)) row.getString(3) else null
      ((date, ap, av, ca), 1l)
    }.reduceByKey((a, b) =>  a + b)
    .map { row =>
      (row._1._1, row._1._2, row._1._3, row._1._4, row._2)
    }
    .toDF("date4", "app_id4", "adv_id4", "camp_id4", "total_reach")

  //distinct devices that received content minus devices that viewed/shared content
  val earnedReachDF = receivesDF
    .except(distinctViewsDF)
    .distinct() //may be not needed here

  //count of devices that received content but not viewed/shared it
  val df5EarnedReach = earnedReachDF
    .rdd
    .map { row =>
      val date = row.getTimestamp(0)
      val ap = row.getString(1)
      val av = if (!row.isNullAt(2)) row.getString(2) else null
      val ca = if (!row.isNullAt(3)) row.getString(3) else null 
      ((date, ap, av, ca), 1l)
    }.reduceByKey((a, b) =>  a + b)
    .map { row =>
      (row._1._1, row._1._2, row._1._3, row._1._4, row._2)
    }
    .toDF("date5", "app_id5", "adv_id5", "camp_id5", "earned_reach")

  //Grouping results into one table
  val resultsDF1 = df1Views
    .join(df2Shares,
         df1Views.col("date") === df2Shares.col("date2") &&
         df1Views.col("app_id") === df2Shares.col("app_id2") &&
         ((df1Views.col("adv_id").isNull && df2Shares.col("adv_id2").isNull) || (df1Views.col("adv_id") === df2Shares.col("adv_id2"))) &&
          ((df1Views.col("camp_id").isNull && df2Shares.col("camp_id2").isNull) || (df1Views.col("camp_id") === df2Shares.col("camp_id2"))),
          "left_outer"
    )

  val resultsDF2 = resultsDF1
    .join(df3Receives,
         resultsDF1.col("date") === df3Receives.col("date3") &&
         resultsDF1.col("app_id") === df3Receives.col("app_id3") &&
         ((resultsDF1.col("adv_id").isNull && df3Receives.col("adv_id3").isNull) || (resultsDF1.col("adv_id") === df3Receives.col("adv_id3"))) &&
          ((resultsDF1.col("camp_id").isNull && df3Receives.col("camp_id3").isNull) || (resultsDF1.col("camp_id") === df3Receives.col("camp_id3"))),
          "left_outer"
    )

  val resultsDF3 = resultsDF2
    .join(df4TotalReach,
         resultsDF2.col("date") === df4TotalReach.col("date4") &&
         resultsDF2.col("app_id") === df4TotalReach.col("app_id4") &&
         ((resultsDF2.col("adv_id").isNull && df4TotalReach.col("adv_id4").isNull) || (resultsDF2.col("adv_id") === df4TotalReach.col("adv_id4"))) &&
          ((resultsDF2.col("camp_id").isNull && df4TotalReach.col("camp_id4").isNull) || (resultsDF2.col("camp_id") === df4TotalReach.col("camp_id4"))),
          "left_outer"
    )

  val resultsDF4 = resultsDF3
    .join(df5EarnedReach,
         resultsDF3.col("date") === df5EarnedReach.col("date5") &&
         resultsDF3.col("app_id") === df5EarnedReach.col("app_id5") &&
         ((resultsDF3.col("adv_id").isNull && df5EarnedReach.col("adv_id5").isNull) || (resultsDF3.col("adv_id") === df5EarnedReach.col("adv_id5"))) &&
          ((resultsDF3.col("camp_id").isNull && df5EarnedReach.col("camp_id5").isNull) || (resultsDF3.col("camp_id") === df5EarnedReach.col("camp_id5"))),
          "left_outer"
    )

  val resultsDF = resultsDF4
      .select("date", "app_id", "adv_id", "camp_id", "total_reach", "paid_reach", "earned_reach", "unique_views", "unique_shares", "unique_receives")
      .rdd
      .map { row =>
        val date = row.getTimestamp(0)
        val app_id = row.getString(1)
        val adv_id = if (!row.isNullAt(2)) row.getString(2) else null
        val camp_id = if (!row.isNullAt(3)) row.getString(3) else null
        val total_reach = if (!row.isNullAt(4)) row.getLong(4) else 0l
        val paid_reach = if (!row.isNullAt(5)) row.getLong(5) else 0l
        val earned_reach = if (!row.isNullAt(6)) row.getLong(6) else 0l
        val unique_views = if (!row.isNullAt(7)) row.getLong(7) else 0l
        val unique_shares = if (!row.isNullAt(8)) row.getLong(8) else 0l
        val unique_receives = if (!row.isNullAt(9)) row.getLong(9) else 0l
        (date, app_id, adv_id, camp_id, total_reach, paid_reach, earned_reach, unique_views, unique_shares, unique_receives, adv_id!=null)
      }
     .toDF("date", "app_id", "adv_id", "camp_id", "total_reach", "paid_reach","earned_reach", "unique_views", "unique_shares", "unique_receives", "is_branded")

  resultsDF
}

// COMMAND ----------

import java.util.TimeZone
import org.joda.time.{DateTime, DateTimeZone}
import java.sql.Timestamp
import org.apache.spark.util.StatCounter
import org.apache.spark.sql.functions._
import scala.Option.{apply => ?}
import org.apache.spark.sql.types._

val tsToDay = new DateTime(tsTo).plusMinutes(1)
val tsToDayMs =  new DateTime(tsToDay.getYear, tsToDay.getMonthOfYear, tsToDay.getDayOfMonth, 0, 0).getMillis
val tsToDayTimestamp = new Timestamp(tsToDayMs)

val from1DayAgoMs = new DateTime(tsTo).minusDays(1).getMillis
val from7DaysAgoMs = new DateTime(tsTo).minusDays(7).getMillis
val from30DaysAgoMs = new DateTime(tsTo).minusDays(30).getMillis

val emptyContentDF = Seq
      .empty[(Timestamp, String, String, String, Long, Long, Long, Long, Long, Long, Boolean)]
      .toDF("date", "app_id", "adv_id", "content_id", "total_reach", "paid_reach","earned_reach", "unique_views", "unique_shares", "unique_receives", "is_branded")

val conxSchema = StructType(List(
        StructField("ap", StringType),
        StructField("at", StringType),
        StructField("co", StringType),
        StructField("ca", StringType),
        StructField("ch", StringType),
        StructField("av", StringType),
        StructField("de", StringType),
        StructField("dt", LongType),
        StructField("v", IntegerType),
        StructField("kv", DoubleType),
        StructField("mv", DoubleType)
      ))

// //30 days 
// val conxDataFor30DaysDFOpt : Option[DataFrame] = getS3PathForDatesOpt(from30DaysAgoMs, tsTo, 0) match {
//   case Some(paths) => ?(
//     sqlContext.read
//     .schema(conxSchema)
//     .json(getS3PathsForType(s3Folder, paths, "conx"):_*)
//     .filter(
//       $"ca" === "7RW2SM5M" &&
//       $"co".isNotNull && (
//       $"v" > 2 || 
//       ($"v" === 2 && 
//          (($"mv".isNotNull && $"mv" >= 3.6) || $"kv" >= 3.6 ))
//        ) 
//     )
//   )
//   case None => None
// }
                                       
// //7 days
// val conxDataFor7DaysDFOpt : Option[DataFrame] = conxDataFor30DaysDFOpt match {
//     case Some(df) => ?(df.filter($"dt" >= from7DaysAgoMs && $"dt" <= tsTo))
//     case None => None
// }  

// //1 day
// val conxDataFor1DayDFOpt : Option[DataFrame] = conxDataFor7DaysDFOpt match {
//   case Some(df) => ?(df.filter($"dt" >= from1DayAgoMs && $"dt" <= tsTo))
//   case None => None
// }


// COMMAND ----------

import org.apache.spark.sql.execution.datasources.jdbc._
/////////////////////
//Campaign
/////////////////////

val emptyCampDF = Seq
      .empty[(Timestamp, String, String, String, Long, Long, Long, Long, Long, Long, Boolean)]
      .toDF("date", "app_id", "adv_id", "camp_id", "total_reach", "paid_reach","earned_reach", "unique_views", "unique_shares", "unique_receives", "is_branded")
// //1 day
// val campaignReachFor1DayDF = conxDataFor1DayDFOpt match {
//   case Some(df) => extractCampaignReachStats(df, tsToDayTimestamp).withColumn("length", lit(1))
//   case None => emptyCampDF
// }


// //7 days
// val campaignReachFor7DayDF = conxDataFor7DaysDFOpt match {
//   case Some(df) => extractCampaignReachStats(df, tsToDayTimestamp).withColumn("length", lit(7))
//   case None => emptyCampDF
// }

// //30 days
// val campaignReachFor30DayDF = conxDataFor30DaysDFOpt match {
//   case Some(df) => extractCampaignReachStats(df, tsToDayTimestamp).withColumn("length", lit(30))
//   case None => emptyCampDF
// }

//lifetime
//Collecting all camp_ids
val campaignsDataCurrentDayDFOpt : Option[DataFrame] = getS3PathForDatesOpt(tsFrom, tsToDayMs, 0) match {
  case Some(paths) => 
      ?(sqlContext.read.json(getS3PathsForType(s3Folder, paths, "conx"):_*)
      .filter($"ca" === "7RW2SM5M") 
      .select("ca")
      .distinct
      .limit(100)
       )
  case None => None
}

//combining camp ids with timeranges from camp flight table
val campaignsWithTimeRangeMap : Map[String, (Long, Long)] = campaignsDataCurrentDayDFOpt match {
  case Some(df) =>
      df
      .join(campFlightPlansDF, df.col("ca") === campFlightPlansDF.col("camp_id"))
      .drop(df.col("ca"))
      .map{ row =>
        val ca = row.getString(0)
        val from = if (!row.isNullAt(1)) {
          val ts = row.getLong(1)
          if (ts < earliestDayWeHaveDataFor.getMillis) earliestDayWeHaveDataFor.getMillis else ts
        } else earliestDayWeHaveDataFor.getMillis
        val to = if (!row.isNullAt(2)) {
          val ts = row.getLong(2)
          if (ts > latestDayWeHaveDataFor.getMillis) latestDayWeHaveDataFor.getMillis else ts
        } else latestDayWeHaveDataFor.getMillis
        (ca, (from, to))
      }
      .collect
      .toMap
  case None => Map()
}

//Building the map of all paths by camp_id
val campaignPathMap = (for (ca <- campaignsWithTimeRangeMap.keys) yield {
  val rangeTuple = campaignsWithTimeRangeMap(ca)
  val tsRangeFrom = rangeTuple._1
  val tsRangeTo = rangeTuple._2
  getS3PathForDatesOpt(tsRangeFrom, tsRangeTo, 0) match {
    case Some(paths) => (ca, paths)
    case None => null
  }  
}).filter(_!=null)
.toMap

//finally, loading DF from the S3 Index
val campaignDF = sqlContext.read
  .schema(StructType(List(
        StructField("ap", StringType),
        StructField("at", StringType),
        StructField("ca", StringType),
        StructField("ch", StringType),
        StructField("av", StringType),
        StructField("de", StringType),
        StructField("dt", LongType)
      )))
  .json(getS3PathsForIndex(s3Folder, "conx", "ca", campaignPathMap):_*)
  .select("ap", "at", "ca", "ch", "av", "de", "dt")

//And extracting stats from that DF
val campaignReachLifetimeDF = extractCampaignReachStats(campaignDF, tsToDayTimestamp).withColumn("length", lit(0))

//Collecting results for all lengths
val campaignReachDF = campaignReachLifetimeDF //campaignReachFor1DayDF.union(campaignReachFor7DayDF).union(campaignReachFor30DayDF).union(campaignReachLifetimeDF)

//Converting to the final DF
val campaignReachDFResult = campaignReachDF
  .rdd
  .map { row =>
      val date = row.getTimestamp(0)
      val app_id = row.getString(1)
      val adv_id = row.getString(2)
      val camp_id = row.getString(3)
      val total_reach = if (row.isNullAt(4)) 0  else row.getLong(4)
      val paid_reach = if (row.isNullAt(5)) 0  else row.getLong(5)
      val earned_reach = if (row.isNullAt(6)) 0  else row.getLong(6)
      val unique_views = if (row.isNullAt(7)) 0  else row.getLong(7)
      val unique_shares = if (row.isNullAt(8)) 0  else row.getLong(8)
      val unique_receives = if (row.isNullAt(9)) 0  else row.getLong(9)
      val is_branded = row.getBoolean(10)
      val length = row.getInt(11)
      (date, app_id, adv_id, camp_id, total_reach, paid_reach, earned_reach, unique_views,  unique_shares, unique_receives, length, is_branded)
    }
 .toDF("date", "app_id", "adv_id", "camp_id", "total_reach", "paid_reach","earned_reach", "unique_views", "unique_shares", "unique_receives","length","is_branded")

val datesList = campaignReachDFResult.select($"date").distinct().collect()

if (storeResultsToMySQL) { 

  val deleteQuery = "DELETE FROM campaign_reach WHERE date=?;"
  
  val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1,"campaign_reach",scala.Predef.Map()))()
  try {
    datesList.foreach { d =>
      val stmt = mysqlConnection.prepareStatement(deleteQuery)
      stmt.setTimestamp(1, d(0).asInstanceOf[Timestamp])
      stmt.execute()
      println(s"Deleted records from campaign_reach for day=$d")
    }
  }
  finally {
    if (null != mysqlConnection) {
      mysqlConnection.close()
    }
  }

  //write to MySQL
  print("Starting writing results to campaign_reach...")

  val startTs = DateTime.now()
  
  campaignReachDFResult
  .write.mode(saveMode)
  .jdbc(jdbcDipCts1, "campaign_reach", new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")
} else {
  display(campaignReachDFResult)
}


// COMMAND ----------



// COMMAND ----------

// val from = new DateTime(2017, 9, 20, 0, 0).getMillis
// val to = new DateTime(2017, 9, 21, 0, 0).getMillis

// val tsToDay = new DateTime(to).plusMinutes(1)
// val tsToDayMs =  new DateTime(tsToDay.getYear, tsToDay.getMonthOfYear, tsToDay.getDayOfMonth, 0, 0).getMillis
// val tsToDayTimestamp = new Timestamp(tsToDayMs)

// val from1DayAgoMs = new DateTime(tsToDayMs).minusDays(1).getMillis
// val from7DaysAgoMs = new DateTime(tsToDayMs).minusDays(7).getMillis
// val from30DaysAgoMs = new DateTime(tsToDayMs).minusDays(30).getMillis

// val paths  = getS3PathsForType(s3Folder, getS3PathForDates(from1DayAgoMs, tsToDayMs, 0), "conx")
// val conxDataDF = sqlContext
//   .read
//   .json( paths:_*)
// val targetDayTimestamp = tsToDayTimestamp

//   val conxDF = conxDataDF
//     .select("ap", "at", "co", "ch", "av", "de", "dt")
//     .rdd
//     .map { row =>
//         val ap = row.getString(0)
//         val at = row.getString(1)
//         val co = row.getString(2)
//         val ch = if (!row.isNullAt(3)) row.getString(3) else null 
//         val av = if (!row.isNullAt(4)) row.getString(4) else null
//         val de = row.getString(5)   
//         var dayTimestamp = new DateTime(extractTime(row, 6))
//         //cut off bogus dates
//         if (dayTimestamp.isAfter(DateTime.now())) {
//           null
//         } else {
//           (ap, at, av, co, ch, de, targetDayTimestamp)
//         }
//     }
//     .filter(x => x != null)
//     .toDF("ap", "at", "av", "co", "ch", "de", "date")


//   val viewsAndSharesDF = conxDF.filter($"at" === "v" || $"at" === "s")
//   viewsAndSharesDF.cache

//   val sharesDF = viewsAndSharesDF.filter($"at" === "s")

//   val distinctViewsDF = viewsAndSharesDF
//     .select("date", "ap", "av", "co", "de")
//     .distinct()

//   val df1Views = distinctViewsDF
//     .rdd
//     .map { row =>
//       val date = row.getTimestamp(0)
//       val ap = row.getString(1)
//       val av = if (!row.isNullAt(2)) row.getString(2) else null
//       val co = row.getString(3) 
//       ((date, ap, av, co), 1l)
//     }.reduceByKey((a, b) =>  a+b)
//     .map { row =>
//       (row._1._1, row._1._2, row._1._3, row._1._4, row._2, row._2)
//     }
//     .toDF("date", "app_id", "adv_id", "content_id", "unique_views", "paid_reach")

//   val distinctSharesDF = sharesDF
//     .select("date", "ap", "av", "co", "de")
//     .distinct()

//   val df2Shares = distinctSharesDF
//     .rdd
//     .map { row =>
//       val date = row.getTimestamp(0)
//       val ap = row.getString(1)
//       val av = if (!row.isNullAt(2)) row.getString(2) else null
//       val co = row.getString(3) 
//       ((date, ap, av, co), 1l)
//     }.reduceByKey((a, b) =>  a+b)
//     .map { row =>
//       (row._1._1, row._1._2, row._1._3, row._1._4, row._2)
//     }
//     .toDF("date2", "app_id2", "adv_id2", "content_id2", "unique_shares")



//   val chatReadersDF = conxDF
//     .filter($"ch".isNotNull)
//     .select("ap","ch","de")
//     .toDF("ap1","ch1","de1")
//     .distinct()

//  val receivesJoinedDF = sharesDF
//    .join(
//       chatReadersDF, 
//       sharesDF.col("ap") === chatReadersDF.col("ap1") &&
//       sharesDF.col("ch") === chatReadersDF.col("ch1") &&
//       sharesDF.col("de") =!= chatReadersDF.col("de1"),
//       "left_outer"
//   )

//   val receivesDF = receivesJoinedDF
//     .select("date", "ap", "av", "co", "de1")
//     .toDF("date", "ap", "av", "co", "de")
//     .distinct()

//   val df3Receives = receivesDF
//     .rdd
//     .map { row =>
//       val date = row.getTimestamp(0)
//       val ap = row.getString(1)
//       val av = if (!row.isNullAt(2)) row.getString(2) else null
//       val co = row.getString(3) 
//       ((date, ap, av, co), 1l)
//     }.reduceByKey((a, b) =>  a + b)
//     .map { row =>
//       (row._1._1, row._1._2, row._1._3, row._1._4, row._2)
//     }
//     .toDF("date3", "app_id3", "adv_id3", "content_id3", "unique_receives")

  
//    val totalReachDF = receivesDF
//       .union(distinctViewsDF)
//       .distinct()

//   val df4TotalReach = totalReachDF
//     .rdd
//     .map { row =>
//       val date = row.getTimestamp(0)
//       val ap = row.getString(1)
//       val av = if (!row.isNullAt(2)) row.getString(2) else null
//       val co = row.getString(3) 
//       ((date, ap, av, co), 1l)
//     }.reduceByKey((a, b) =>  a + b)
//     .map { row =>
//       (row._1._1, row._1._2, row._1._3, row._1._4, row._2)
//     }
//     .toDF("date4", "app_id4", "adv_id4", "content_id4", "total_reach")


//   val earnedReachDF = receivesDF
//     .except(distinctViewsDF)
//     .distinct() //may be not needed here

//   val df5EarnedReach = earnedReachDF
//     .rdd
//     .map { row =>
//       val date = row.getTimestamp(0)
//       val ap = row.getString(1)
//       val av = if (!row.isNullAt(2)) row.getString(2) else null
//       val co = row.getString(3) 
//       ((date, ap, av, co), 1l)
//     }.reduceByKey((a, b) =>  a + b)
//     .map { row =>
//       (row._1._1, row._1._2, row._1._3, row._1._4, row._2)
//     }
//     .toDF("date5", "app_id5", "adv_id5", "content_id5", "earned_reach")

//   val resultsDF1 = df1Views
//     .join(df2Shares,
//          df1Views.col("date") === df2Shares.col("date2") &&
//          df1Views.col("app_id") === df2Shares.col("app_id2") &&
//          ((df1Views.col("adv_id").isNull && df2Shares.col("adv_id2").isNull) || (df1Views.col("adv_id") === df2Shares.col("adv_id2"))) &&
//          df1Views.col("content_id") === df2Shares.col("content_id2"),
//           "left_outer"
//     )

//   val resultsDF2 = resultsDF1
//     .join(df3Receives,
//          resultsDF1.col("date") === df3Receives.col("date3") &&
//          resultsDF1.col("app_id") === df3Receives.col("app_id3") &&
//          ((resultsDF1.col("adv_id").isNull && df3Receives.col("adv_id3").isNull) || (resultsDF1.col("adv_id") === df3Receives.col("adv_id3"))) &&
//          resultsDF1.col("content_id") === df3Receives.col("content_id3"),
//           "left_outer"
//     )

//   val resultsDF3 = resultsDF2
//     .join(df4TotalReach,
//          resultsDF2.col("date") === df4TotalReach.col("date4") &&
//          resultsDF2.col("app_id") === df4TotalReach.col("app_id4") &&
//          ((resultsDF2.col("adv_id").isNull && df4TotalReach.col("adv_id4").isNull) || (resultsDF2.col("adv_id") === df4TotalReach.col("adv_id4"))) &&
//          resultsDF2.col("content_id") === df4TotalReach.col("content_id4"),
//           "left_outer"
//     )

//   val resultsDF4 = resultsDF3
//     .join(df5EarnedReach,
//          resultsDF3.col("date") === df5EarnedReach.col("date5") &&
//          resultsDF3.col("app_id") === df5EarnedReach.col("app_id5") &&
//          ((resultsDF3.col("adv_id").isNull && df5EarnedReach.col("adv_id5").isNull) || (resultsDF3.col("adv_id") === df5EarnedReach.col("adv_id5"))) &&
//          resultsDF3.col("content_id") === df5EarnedReach.col("content_id5"),
//           "left_outer"
//     )

//   val resultsDF = resultsDF4
//       .select("date", "app_id", "adv_id", "content_id", "total_reach", "paid_reach", "earned_reach", "unique_views", "unique_shares", "unique_receives")
//       .rdd
//       .map { row =>
//         val date = row.getTimestamp(0)
//         val app_id = row.getString(1)
//         val adv_id = if (!row.isNullAt(2)) row.getString(2) else null
//         val content_id = row.getString(3) 
//         val total_reach = if (!row.isNullAt(4)) row.getLong(4) else 0l
//         val paid_reach = if (!row.isNullAt(5)) row.getLong(5) else 0l
//         val earned_reach = if (!row.isNullAt(6)) row.getLong(6) else 0l
//         val unique_views = if (!row.isNullAt(7)) row.getLong(7) else 0l
//         val unique_shares = if (!row.isNullAt(8)) row.getLong(8) else 0l
//         val unique_receives = if (!row.isNullAt(9)) row.getLong(9) else 0l
//         (date, app_id, adv_id, content_id, total_reach, paid_reach, earned_reach, unique_views, unique_shares, unique_receives, adv_id!=null)
//       }
//      .toDF("date", "app_id", "adv_id", "content_id", "total_reach", "paid_reach","earned_reach", "unique_views", "unique_shares", "unique_receives", "is_branded")

// display(resultsDF)
// // display(resultsDF.filter($"content_id" === "365RA0" && $"app_id" === "15A42C"))

// COMMAND ----------

// import org.apache.spark.sql.DataFrame

// def extractContentReachStats(conxDataDF: DataFrame, targetDayTimestamp:Timestamp) : DataFrame = {
// //Prepare conx DF
//   val conxDF = conxDataDF
//     .select("ap", "at", "co", "ch", "av", "de", "dt")
//     .rdd
//     .map { row =>
//         val ap = row.getString(0)
//         val at = row.getString(1)
//         val co = row.getString(2)
//         val ch = if (!row.isNullAt(3)) row.getString(3) else null 
//         val av = if (!row.isNullAt(4)) row.getString(4) else null
//         val de = row.getString(5)   
//         var dayTimestamp = new DateTime(extractTime(row, 6))
//         //cut off bogus dates
//         if (dayTimestamp.isAfter(DateTime.now())) {
//           null
//         } else {
//           (ap, at, av, co, ch, de, targetDayTimestamp)
//         }
//     }
//     .filter(x => x != null)
//     .toDF("ap", "at", "av", "co", "ch", "de", "date")

  
//     //Unique device counts for Views & Shares
//   val distinctDeviceEventsDF = conxDF
//     .rdd
//     .map { row =>
//       val ap = row.getString(0)
//       val at = row.getString(1)
//       val av = if (!row.isNullAt(2)) row.getString(2) else null
//       val co = row.getString(3)
//       val de = row.getString(5)   
//       val date = row.getTimestamp(6)
//       (date, ap, av, co, de, at)
//     }
//     .toDF("date", "ap", "av", "co", "de", "at")
//     .distinct()
  
  
//   //Unique device counts for Views & Shares
//   val uniqueViewsSharesDF = distinctDeviceEventsDF
//     .rdd
//     .map { row =>
//       val date = row.getTimestamp(0)
//       val ap = row.getString(1)
//       val av = if (!row.isNullAt(2)) row.getString(2) else null
//       val co = row.getString(3) 
//       val at = row.getString(5)
      
//       val view = if (at.equals("v")) 1l else 0
//       val share = if (at.equals("s")) 1l else 0 
//       ((date, ap, av, co), (view, share))
//     }.reduceByKey((a, b) =>  (a._1 + b._1, a._2 + b._2))
//     .map { row =>
//       (row._1._1, row._1._2, row._1._3, row._1._4, row._2._1, row._2._1, row._2._2, row._1._3 != null)
//     }
//     .toDF("date", "app_id", "adv_id", "content_id", "paid_reach", "unique_views", "unique_shares", "is_branded")

//   //views/shares with chat ids
//   val chatReadersDF = conxDF
//     .filter($"ch".isNotNull)
//     .select("ap","ch","de")
//     .toDF("ap1","ch1","de1")
//     .distinct()

//   //joining chat ids from shares with all views/shares by chat_id
//   val viewsAndReceiversDF = conxDF
//     .filter($"at" === "v")
//     .join(
//       chatReadersDF, 
//       conxDF.col("ap") === chatReadersDF.col("ap1") &&
//       conxDF.col("ch") === chatReadersDF.col("ch1"),
//       "left_outer"
//     )


//   //counting all unique device ids who viewed by themselves or received view (by chat id)
//     val uniqueViewsAndReceiverDF = viewsAndReceiversDF
//     .select("date","ap","av","co","at","de","de1")
//     .toDF("date","ap","av","co","at","de","de1")
//     .distinct()
//     .rdd
//     .map { row =>
//       val ts = row.getTimestamp(0)
//       val ap = row.getString(1)
//       val av = if (!row.isNullAt(2)) row.getString(2) else null 
//       val co = row.getString(3)
//       val at = row.getString(4)

//       val view = if (at.equals("v")) 1l else 0
//       ((ts, ap, av, co), (view, 0l))
//     }.reduceByKey((a, b) =>  (a._1 + b._1, 0))
//     .map { row =>
//       (row._1._1, row._1._2, row._1._3, row._1._4, row._2._1)
//     }
//     .toDF("date_v", "app_id_v", "adv_id_v", "content_id_v", "total_reach")
  
//   //counting only devices that recieved views
//   val receiversDF = viewsAndReceiversDF
//         .filter($"ch".isNotNull && $"ap"===$"ap1" && $"ch"===$"ch1" && $"de"=!=$"de1")
//         .select("date","ap","av","co","de1")
//         .toDF("date","ap","av","co","de")

//   //counting unique device counts that received views
//   val uniqueReceiversDF = receiversDF
//     .distinct()
//     .rdd
//     .map { row =>
//       val ts = row.getTimestamp(0)
//       val ap = row.getString(1)
//       val av = if (!row.isNullAt(2)) row.getString(2) else null 
//       val co = row.getString(3)

//       ((ts, ap, av, co), 1l)
//     }.reduceByKey((a, b) =>  a + b)
//     .map { row =>
//       (row._1._1, row._1._2, row._1._3, row._1._4, row._2, row._2)
//     }
//     .toDF("date_r", "app_id_r", "adv_id_r", "content_id_r", "unique_receives","earned_reach")

//   //collecting day1 into one DF
//   val resultsDFPart1 = uniqueViewsSharesDF
//     .join(uniqueViewsAndReceiverDF,
//          uniqueViewsSharesDF.col("date") === uniqueViewsAndReceiverDF.col("date_v") &&
//          uniqueViewsSharesDF.col("app_id") === uniqueViewsAndReceiverDF.col("app_id_v") &&
//          ((uniqueViewsSharesDF.col("adv_id").isNull && uniqueViewsAndReceiverDF.col("adv_id_v").isNull) || (uniqueViewsSharesDF.col("adv_id") === uniqueViewsAndReceiverDF.col("adv_id_v"))) &&
//          uniqueViewsSharesDF.col("content_id") === uniqueViewsAndReceiverDF.col("content_id_v"),
//          "left_outer"
//       )
//   val resultsDF = resultsDFPart1
//     .join(uniqueReceiversDF,
//          resultsDFPart1.col("date") === uniqueReceiversDF.col("date_r") &&
//          resultsDFPart1.col("app_id") === uniqueReceiversDF.col("app_id_r") &&
//          ((resultsDFPart1.col("adv_id").isNull && uniqueReceiversDF.col("adv_id_r").isNull) || (resultsDFPart1.col("adv_id") === uniqueReceiversDF.col("adv_id_r"))) &&
//          resultsDFPart1.col("content_id") === uniqueReceiversDF.col("content_id_r"),
//          "left_outer"
//       )
//    .select("date", "app_id", "adv_id", "content_id", "total_reach", "paid_reach","earned_reach", "unique_views", "unique_shares", "unique_receives", "is_branded")
//    .toDF("date", "app_id", "adv_id", "content_id", "total_reach", "paid_reach","earned_reach", "unique_views", "unique_shares", "unique_receives", "is_branded")
//   resultsDF
// }

// COMMAND ----------

// import org.apache.spark.sql.DataFrame

// def extractCampaignReachStats(conxDataDF: DataFrame, targetDayTimestamp:Timestamp) : DataFrame = {
  
//     //Prepare conx DF
//   val conxDF = conxDataDF
//     .select("ap", "at", "ca", "ch", "av", "de", "dt")
//     .rdd
//     .map { row =>
//         val ap = row.getString(0)
//         val at = row.getString(1)
//         val ca = if (!row.isNullAt(2)) row.getString(2) else null
//         val ch = if (!row.isNullAt(3)) row.getString(3) else null 
//         val av = if (!row.isNullAt(4)) row.getString(4) else null
//         val de = row.getString(5)   
//         var dayTimestamp = new DateTime(extractTime(row, 6))
//         //cut off bogus dates
//         if (dayTimestamp.isAfter(DateTime.now())) {
//           null
//         } else {
//           (ap, at, av, ca, ch, de, targetDayTimestamp)
//         }
//     }
//     .filter(x => x != null)
//     .toDF("ap", "at", "av", "ca", "ch", "de", "date")

  
//     //Unique device counts for Views & Shares
//   val distinctDeviceEventsDF = conxDF
//     .rdd
//     .map { row =>
//       val ap = row.getString(0)
//       val at = row.getString(1)
//       val av = if (!row.isNullAt(2)) row.getString(2) else null
//       val ca = if (!row.isNullAt(3)) row.getString(3) else null
//       val de = row.getString(5)   
//       val date = row.getTimestamp(6)
//       (date, ap, av, ca, de, at)
//     }
//     .toDF("date", "ap", "av", "ca", "de", "at")
//     .distinct()
  
  
//   //Unique device counts for Views & Shares
//   val uniqueViewsSharesDF = distinctDeviceEventsDF
//     .rdd
//     .map { row =>
//       val date = row.getTimestamp(0)
//       val ap = row.getString(1)
//       val av = if (!row.isNullAt(2)) row.getString(2) else null
//       val ca = if (!row.isNullAt(3)) row.getString(3) else null
//       val at = row.getString(5)
      
//       val view = if (at.equals("v")) 1l else 0
//       val share = if (at.equals("s")) 1l else 0 
//       ((date, ap, av, ca), (view, share))
//     }.reduceByKey((a, b) =>  (a._1 + b._1, a._2 + b._2))
//     .map { row =>
//       (row._1._1, row._1._2, row._1._3, row._1._4, row._2._1, row._2._1, row._2._2, row._1._3 != null)
//     }
//     .toDF("date", "app_id", "adv_id", "camp_id", "paid_reach", "unique_views", "unique_shares", "is_branded")

//   //views/shares with chat ids
//   val chatReadersDF = conxDF
//     .filter($"ch".isNotNull)
//     .select("ap","ch","de")
//     .toDF("ap1","ch1","de1")
//     .distinct()

//   //joining chat ids from shares with all views/shares by chat_id
//   val viewsAndReceiversDF = conxDF
//   .filter($"at" === "v")
//   .join(
//     chatReadersDF, 
//     conxDF.col("ap") === chatReadersDF.col("ap1") &&
//     conxDF.col("ch") === chatReadersDF.col("ch1"),
//     "left_outer"
//   )


//   //counting all unique device ids who viewed by themselves or received view (by chat id)
//     val uniqueViewsAndReceiverDF = viewsAndReceiversDF
//     .select("date","ap","av","ca","at","de","de1")
//     .toDF("date","ap","av","ca","at","de","de1")
//     .distinct()
//     .rdd
//     .map { row =>
//       val ts = row.getTimestamp(0)
//       val ap = row.getString(1)
//       val av = if (!row.isNullAt(2)) row.getString(2) else null 
//       val ca = if (!row.isNullAt(3)) row.getString(3) else null 
//       val at = row.getString(4)

//       val view = if (at.equals("v")) 1l else 0
//       ((ts, ap, av, ca), (view, 0l))
//     }.reduceByKey((a, b) =>  (a._1 + b._1, 0))
//     .map { row =>
//       (row._1._1, row._1._2, row._1._3, row._1._4, row._2._1)
//     }
//     .toDF("date_v", "app_id_v", "adv_id_v", "camp_id_v", "total_reach")
  

//   //counting only devices that recieved views
//   val receiversDF = viewsAndReceiversDF
//         .filter($"ch".isNotNull && $"ap"===$"ap1" && $"ch"===$"ch1" && $"de"=!=$"de1")
//         .select("date","ap","av","ca","de1")
//         .toDF("date","ap","av","ca","de")

//   //counting unique device counts that received views
//   val uniqueReceiversDF = receiversDF
//     .distinct()
//     .rdd
//     .map { row =>
//       val ts = row.getTimestamp(0)
//       val ap = row.getString(1)
//       val av = if (!row.isNullAt(2)) row.getString(2) else null 
//       val ca = if (!row.isNullAt(3)) row.getString(3) else null 

//       ((ts, ap, av, ca), 1l)
//     }.reduceByKey((a, b) =>  a + b)
//     .map { row =>
//       (row._1._1, row._1._2, row._1._3, row._1._4, row._2, row._2)
//     }
//     .toDF("date_r", "app_id_r", "adv_id_r", "camp_id_r", "unique_receives","earned_reach")

//   //collecting day1 into one DF
//     val resultsDFPart1 = uniqueViewsSharesDF
//     .join(uniqueViewsAndReceiverDF,
//          uniqueViewsSharesDF.col("date") === uniqueViewsAndReceiverDF.col("date_v") &&
//          uniqueViewsSharesDF.col("app_id") === uniqueViewsAndReceiverDF.col("app_id_v") &&
//          ((uniqueViewsSharesDF.col("adv_id").isNull && uniqueViewsAndReceiverDF.col("adv_id_v").isNull) || (uniqueViewsSharesDF.col("adv_id") === uniqueViewsAndReceiverDF.col("adv_id_v"))) &&
//          uniqueViewsSharesDF.col("camp_id") === uniqueViewsAndReceiverDF.col("camp_id_v"),
//          "left_outer"
//       )
//   val resultsDF = resultsDFPart1
//     .join(uniqueReceiversDF,
//          resultsDFPart1.col("date") === uniqueReceiversDF.col("date_r") &&
//          resultsDFPart1.col("app_id") === uniqueReceiversDF.col("app_id_r") &&
//          ((resultsDFPart1.col("adv_id").isNull && uniqueReceiversDF.col("adv_id_r").isNull) || (resultsDFPart1.col("adv_id") === uniqueReceiversDF.col("adv_id_r"))) &&
//          resultsDFPart1.col("camp_id") === uniqueReceiversDF.col("camp_id_r"),
//          "left_outer"
//       )
//    .select("date", "app_id", "adv_id", "camp_id", "total_reach", "paid_reach","earned_reach", "unique_views", "unique_shares", "unique_receives", "is_branded")
//    .toDF("date", "app_id", "adv_id", "camp_id", "total_reach", "paid_reach","earned_reach", "unique_views", "unique_shares", "unique_receives", "is_branded")
//   resultsDF
// }

// COMMAND ----------

dbutils.notebook.exit("success")