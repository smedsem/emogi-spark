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
        val ch = extractNullableString(row, 3)
        val av = extractNullableString(row, 4)
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
      val av = extractNullableString(row, 2)
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
      val av = extractNullableString(row, 2)
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
      val av = extractNullableString(row, 2)
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
      val av = extractNullableString(row, 2)
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
      val av = extractNullableString(row, 2)
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
        val adv_id = extractNullableString(row, 2)
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
        val ca = extractNullableString(row, 2)
        val ch = extractNullableString(row, 3)
        val av = extractNullableString(row, 4)
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
      val av = extractNullableString(row, 2)
      val ca = extractNullableString(row, 3)
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
      val av = extractNullableString(row, 2)
      val ca = extractNullableString(row, 3)
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
      val av = extractNullableString(row, 2)
      val ca = extractNullableString(row, 3)
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
      val av = extractNullableString(row, 2)
      val ca = extractNullableString(row, 3)
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
      val av = extractNullableString(row, 2)
      val ca = extractNullableString(row, 3)
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
        val adv_id = extractNullableString(row, 2)
        val camp_id = extractNullableString(row, 3)
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

import org.apache.spark.sql.DataFrame

def extractCampaignReachStatsForAllApps(conxDataDF: DataFrame, targetDayTimestamp:Timestamp) : DataFrame = {
  
  // Preparing data
  val conxDF = conxDataDF
    .select("ap", "at", "ca", "ch", "av", "de", "dt")
    .rdd
    .map { row =>
        val ap = row.getString(0)
        val at = row.getString(1)
        val ca = extractNullableString(row, 2) 
        val ch = extractNullableString(row, 3)
        val av = extractNullableString(row, 4)
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
    .select("date", "av", "ca", "de")
    .distinct()

  // count of devices that viewed/shared content
  val df1Views = distinctViewsDF
    .rdd
    .map { row =>
      val date = row.getTimestamp(0)
//       val ap = row.getString(1)
      val av = extractNullableString(row, 1)
      val ca = extractNullableString(row, 2)
      ((date, av, ca), 1l)
    }.reduceByKey((a, b) =>  a+b)
    .map { row =>
      (row._1._1, row._1._2, row._1._3, row._2, row._2)
    }
    .toDF("date", "adv_id", "camp_id", "unique_views", "paid_reach")

   // distinct devices that shared content
  val distinctSharesDF = sharesDF
    .select("date", "av", "ca", "de")
    .distinct()

   // count of devices that shared content
  val df2Shares = distinctSharesDF
    .rdd
    .map { row =>
      val date = row.getTimestamp(0)
//       val ap = row.getString(1)
      val av = extractNullableString(row, 1)
      val ca = extractNullableString(row, 2)
      ((date, av, ca), 1l)
    }.reduceByKey((a, b) =>  a+b)
    .map { row =>
      (row._1._1, row._1._2, row._1._3, row._2)
    }
    .toDF("date2", "adv_id2", "camp_id2", "unique_shares")


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
    .select("date", "av", "ca", "de1")
    .toDF("date", "av", "ca", "de")
    .distinct()

  //count of distinct devices that received content
  val df3Receives = receivesDF
    .rdd
    .map { row =>
      val date = row.getTimestamp(0)
//       val ap = row.getString(1)
      val av = extractNullableString(row, 1)
      val ca = extractNullableString(row, 2) 
      ((date, av, ca), 1l)
    }.reduceByKey((a, b) =>  a + b)
    .map { row =>
      (row._1._1, row._1._2, row._1._3, row._2)
    }
    .toDF("date3", "adv_id3", "camp_id3", "unique_receives")

  //distinct devices that received content combined with distinct devices that viewed/shared content
   val totalReachDF = receivesDF
      .union(distinctViewsDF)
      .distinct()

  //count of devices that received + viewed/shared
  val df4TotalReach = totalReachDF
    .rdd
    .map { row =>
      val date = row.getTimestamp(0)
//       val ap = row.getString(1)
      val av = extractNullableString(row, 1)
      val ca = extractNullableString(row, 2)
      ((date, av, ca), 1l)
    }.reduceByKey((a, b) =>  a + b)
    .map { row =>
      (row._1._1, row._1._2, row._1._3, row._2)
    }
    .toDF("date4", "adv_id4", "camp_id4", "total_reach")

  //distinct devices that received content minus devices that viewed/shared content
  val earnedReachDF = receivesDF
    .except(distinctViewsDF)
    .distinct() //may be not needed here

  //count of devices that received content but not viewed/shared it
  val df5EarnedReach = earnedReachDF
    .rdd
    .map { row =>
      val date = row.getTimestamp(0)
//       val ap = row.getString(1)
      val av = extractNullableString(row, 1)
      val ca = extractNullableString(row, 2) 
      ((date, av, ca), 1l)
    }.reduceByKey((a, b) =>  a + b)
    .map { row =>
      (row._1._1, row._1._2, row._1._3, row._2)
    }
    .toDF("date5", "adv_id5", "camp_id5", "earned_reach")

  //Grouping results into one table
  val resultsDF1 = df1Views
    .join(df2Shares,
         df1Views.col("date") === df2Shares.col("date2") &&
         ((df1Views.col("adv_id").isNull && df2Shares.col("adv_id2").isNull) || (df1Views.col("adv_id") === df2Shares.col("adv_id2"))) &&
          ((df1Views.col("camp_id").isNull && df2Shares.col("camp_id2").isNull) || (df1Views.col("camp_id") === df2Shares.col("camp_id2"))),
          "left_outer"
    )

  val resultsDF2 = resultsDF1
    .join(df3Receives,
         resultsDF1.col("date") === df3Receives.col("date3") &&
         ((resultsDF1.col("adv_id").isNull && df3Receives.col("adv_id3").isNull) || (resultsDF1.col("adv_id") === df3Receives.col("adv_id3"))) &&
          ((resultsDF1.col("camp_id").isNull && df3Receives.col("camp_id3").isNull) || (resultsDF1.col("camp_id") === df3Receives.col("camp_id3"))),
          "left_outer"
    )

  val resultsDF3 = resultsDF2
    .join(df4TotalReach,
         resultsDF2.col("date") === df4TotalReach.col("date4") &&
         ((resultsDF2.col("adv_id").isNull && df4TotalReach.col("adv_id4").isNull) || (resultsDF2.col("adv_id") === df4TotalReach.col("adv_id4"))) &&
          ((resultsDF2.col("camp_id").isNull && df4TotalReach.col("camp_id4").isNull) || (resultsDF2.col("camp_id") === df4TotalReach.col("camp_id4"))),
          "left_outer"
    )

  val resultsDF4 = resultsDF3
    .join(df5EarnedReach,
         resultsDF3.col("date") === df5EarnedReach.col("date5") &&
         ((resultsDF3.col("adv_id").isNull && df5EarnedReach.col("adv_id5").isNull) || (resultsDF3.col("adv_id") === df5EarnedReach.col("adv_id5"))) &&
          ((resultsDF3.col("camp_id").isNull && df5EarnedReach.col("camp_id5").isNull) || (resultsDF3.col("camp_id") === df5EarnedReach.col("camp_id5"))),
          "left_outer"
    )

  val resultsDF = resultsDF4
      .select("date", "adv_id", "camp_id", "total_reach", "paid_reach", "earned_reach", "unique_views", "unique_shares", "unique_receives")
      .rdd
      .map { row =>
        val date = row.getTimestamp(0)
        val app_id = "ALL_APPS"
        val adv_id = extractNullableString(row, 1)
        val camp_id = extractNullableString(row, 2)
        val total_reach = if (!row.isNullAt(3)) row.getLong(3) else 0l
        val paid_reach = if (!row.isNullAt(4)) row.getLong(4) else 0l
        val earned_reach = if (!row.isNullAt(5)) row.getLong(5) else 0l
        val unique_views = if (!row.isNullAt(6)) row.getLong(6) else 0l
        val unique_shares = if (!row.isNullAt(7)) row.getLong(7) else 0l
        val unique_receives = if (!row.isNullAt(8)) row.getLong(8) else 0l
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

//30 days 
val conxDataFor30DaysDFOpt : Option[DataFrame] = getS3PathForDatesOpt(from30DaysAgoMs, tsTo, 0) match {
  case Some(paths) => ?(
    sqlContext.read
    .schema(conxSchema)
    .json(getS3PathsForType(s3Folder, paths, "conx"):_*)
    .filter(
      $"co".isNotNull && (
      $"v" > 2 || 
      ($"v" === 2 && 
         (($"mv".isNotNull && $"mv" >= 3.6) || $"kv" >= 3.6 ))
       ) 
    )
  )
  case None => None
}

val contentReachFor30DayDF = conxDataFor30DaysDFOpt match {
  case Some(df) => extractContentReachStats(df, tsToDayTimestamp).withColumn("length", lit(30))
  case None => emptyContentDF
}
                                           

//7 days
val conxDataFor7DaysDFOpt : Option[DataFrame] = conxDataFor30DaysDFOpt match {
    case Some(df) => ?(df.filter($"dt" >= from7DaysAgoMs && $"dt" <= tsTo))
    case None => None
}  
val contentReachFor7DayDF = conxDataFor7DaysDFOpt match {
    case Some(df) => extractContentReachStats(df, tsToDayTimestamp).withColumn("length", lit(7))
    case None => emptyContentDF
}

//1 day
val conxDataFor1DayDFOpt : Option[DataFrame] = conxDataFor7DaysDFOpt match {
  case Some(df) => ?(df.filter($"dt" >= from1DayAgoMs && $"dt" <= tsTo))
  case None => None
}

val contentReachFor1DayDF = conxDataFor1DayDFOpt match {
  case Some(df) => extractContentReachStats(df, tsToDayTimestamp).withColumn("length", lit(1))
  case None => emptyContentDF
}

/*
//lifetime
//Collecting all content_ids
val conxDataCurrentDayDFOpt : Option[DataFrame] = getS3PathForDatesOpt(tsFrom, tsToDayMs, 0) match {
  case Some(paths) => 
      ?(sqlContext.read.json(getS3PathsForType(s3Folder, paths, "conx"):_*)
      //.filter($"co".isNotNull) //$"dt" >= tsFrom && $"dt" <= tsToDayMs && 
      .select("co")
      .distinct
      .limit(100)
       )
  case None => None
}

//combining content ids with timeranges from content flight table
val contentsWithTimeRangeMap : Map[String, (Long, Long)] = conxDataCurrentDayDFOpt match {
  case Some(df) =>
      df
      .join(contentFlightPlansDF, df.col("co") === contentFlightPlansDF.col("content_id"))
      .drop(df.col("co"))
      .map{ row =>
        val co = row.getString(0)
        val from = if (!row.isNullAt(1)) {
          val ts = row.getLong(1)
          if (ts < earliestDayWeHaveDataFor.getMillis) earliestDayWeHaveDataFor.getMillis else ts
        } else earliestDayWeHaveDataFor.getMillis
        val to = if (!row.isNullAt(2)) {
          val ts = row.getLong(2)
          if (ts > latestDayWeHaveDataFor.getMillis) latestDayWeHaveDataFor.getMillis else ts
        } else latestDayWeHaveDataFor.getMillis
        (co, (from, to))
      }
      .collect
      .toMap
  case None => Map()
}

//extracting reach for each content in the loop and union into the final
val contentPathMap = (for (co <- contentsWithTimeRangeMap.keys) yield {
  val rangeTuple = contentsWithTimeRangeMap(co)
  val tsRangeFrom = rangeTuple._1
  val tsRangeTo = rangeTuple._2
  getS3PathForDatesOpt(tsRangeFrom, tsRangeTo, 0) match {
    case Some(paths) => (co, paths)
    case None => null
  }  
}).filter(_!=null)
.toMap

val contentsAllLifetimeDF = sqlContext.read
  .schema(conxSchema)
  .json(getS3PathsForIndex(s3Folder, "conx", "co", contentPathMap):_*)
  .filter(
        $"v" > 2 || 
        ($"v" === 2 && 
           (($"mv".isNotNull && $"mv" >= 3.6) || $"kv" >= 3.6 ))
      )
  .select("ap", "at", "co", "ch", "av", "de", "dt")

val contentReachForLifetimeDF = extractContentReachStats(contentsAllLifetimeDF, tsToDayTimestamp).withColumn("length", lit(0))

*/

val contentReachDF = contentReachFor1DayDF.union(contentReachFor7DayDF).union(contentReachFor30DayDF) //.union(contentReachForLifetimeDF)

val contentReachDFResult = contentReachDF
  .rdd
  .map { row =>
      val date = row.getTimestamp(0)
      val app_id = row.getString(1)
      val adv_id = row.getString(2)
      val content_id = row.getString(3)
      val total_reach = if (row.isNullAt(4)) 0  else row.getLong(4)
      val paid_reach = if (row.isNullAt(5)) 0  else row.getLong(5)
      val earned_reach = if (row.isNullAt(6)) 0  else row.getLong(6)
      val unique_views = if (row.isNullAt(7)) 0  else row.getLong(7)
      val unique_shares = if (row.isNullAt(8)) 0  else row.getLong(8)
      val unique_receives = if (row.isNullAt(9)) 0  else row.getLong(9)
      val is_branded = row.getBoolean(10)
      val length = row.getInt(11)
      (date, app_id, adv_id, content_id, total_reach, paid_reach, earned_reach, unique_views,  unique_shares, unique_receives, length, is_branded)
    }
 .toDF("date", "app_id", "adv_id", "content_id", "total_reach", "paid_reach","earned_reach", "unique_views", "unique_shares", "unique_receives","length","is_branded")


// delete existing rows
// list of days in the new stats report
val datesList = contentReachDFResult.select($"date").distinct().collect()

import org.apache.spark.sql.execution.datasources.jdbc._

if (storeResultsToMySQL) { 
  val deleteQuery = "DELETE FROM content_reach WHERE date=?;"
  val mysqlConnection = JdbcUtils.createConnectionFactory(new JDBCOptions(jdbcDipCts1,"content_reach",scala.Predef.Map()))()
  try {
    datesList.foreach { d =>
      val stmt = mysqlConnection.prepareStatement(deleteQuery)
      stmt.setTimestamp(1, d(0).asInstanceOf[Timestamp])
      stmt.execute()
      println(s"Deleted records from content_reach for day=$d")
    }
  }
  finally {
    if (null != mysqlConnection) {
      mysqlConnection.close()
    }
  }
  //write to MySQL
  print("Starting writing results to content_reach...")
  val startTs = DateTime.now()

  contentReachDFResult
  .write.mode(saveMode)
  .jdbc(jdbcDipCts1, "content_reach", new java.util.Properties())

  val durationMinutes = (((DateTime.now().getMillis - startTs.getMillis) / 1000 )/ 60).toInt
  println(s" - DONE, it took $durationMinutes minutes")
} else {
  display(contentReachDFResult)
}


// COMMAND ----------

import org.apache.spark.sql.execution.datasources.jdbc._
/////////////////////
//Campaign
/////////////////////

val emptyCampDF = Seq
      .empty[(Timestamp, String, String, String, Long, Long, Long, Long, Long, Long, Boolean)]
      .toDF("date", "app_id", "adv_id", "camp_id", "total_reach", "paid_reach","earned_reach", "unique_views", "unique_shares", "unique_receives", "is_branded")
//1 day
val campaignReachFor1DayDF = conxDataFor1DayDFOpt match {
  case Some(df) => extractCampaignReachStats(df, tsToDayTimestamp).withColumn("length", lit(1))
  case None => emptyCampDF
}


//7 days
val campaignReachFor7DayDF = conxDataFor7DaysDFOpt match {
  case Some(df) => extractCampaignReachStats(df, tsToDayTimestamp).withColumn("length", lit(7))
  case None => emptyCampDF
}

//30 days
val campaignReachFor30DayDF = conxDataFor30DaysDFOpt match {
  case Some(df) => extractCampaignReachStats(df, tsToDayTimestamp).withColumn("length", lit(30))
  case None => emptyCampDF
}

//1 day for ALL apps
val campaignReachFor1DayALLAppsDF = conxDataFor1DayDFOpt match {
  case Some(df) => extractCampaignReachStatsForAllApps(df, tsToDayTimestamp).withColumn("length", lit(1))
  case None => emptyCampDF
}


//7 days for ALL apps
val campaignReachFor7DayALLAppsDF = conxDataFor7DaysDFOpt match {
  case Some(df) => extractCampaignReachStatsForAllApps(df, tsToDayTimestamp).withColumn("length", lit(7))
  case None => emptyCampDF
}

//30 days for ALL apps
val campaignReachFor30DayALLAppsDF = conxDataFor30DaysDFOpt match {
  case Some(df) => extractCampaignReachStatsForAllApps(df, tsToDayTimestamp).withColumn("length", lit(30))
  case None => emptyCampDF
}



//lifetime
//Collecting all camp_ids
val campaignsDataCurrentDayDFOpt : Option[DataFrame] = getS3PathForDatesOpt(tsFrom, tsToDayMs, 0) match {
  case Some(paths) => 
      ?(sqlContext.read.json(getS3PathsForType(s3Folder, paths, "conx"):_*)
      .filter($"ca".isNotNull) 
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
  .filter($"at".isNotNull)

//And extracting stats from that DF
val campaignReachLifetimeDF = extractCampaignReachStats(campaignDF, tsToDayTimestamp).withColumn("length", lit(0))

//And extracting stats from that DF for ALL apps
val campaignReachLifetimeALLAppsDF = extractCampaignReachStatsForAllApps(campaignDF, tsToDayTimestamp).withColumn("length", lit(0))

//Collecting results for all lengths
val campaignReachDF = campaignReachFor1DayDF.union(campaignReachFor7DayDF).union(campaignReachFor30DayDF).union(campaignReachLifetimeDF).union(campaignReachFor1DayALLAppsDF).union(campaignReachFor7DayALLAppsDF).union(campaignReachFor30DayALLAppsDF).union(campaignReachLifetimeALLAppsDF)

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

dbutils.notebook.exit("success")