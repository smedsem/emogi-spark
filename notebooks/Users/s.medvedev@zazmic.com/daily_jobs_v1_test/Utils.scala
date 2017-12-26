// Databricks notebook source
import org.joda.time._  
// import org.elasticsearch.spark._
// import org.elasticsearch.spark.sql._
import scala.Option.{apply => ?}
import java.sql.Timestamp

val earliestDayWeHaveDataFor = new DateTime(2017, 5, 1, 0, 0)
val latestDayWeHaveDataFor = DateTime.now() // new DateTime(2017, 9, 25, 0, 0) 
//todo add skip day functionality - since we don't have data for some days like 2017-07-10



//not including tsTo day
def getS3PathForDates(tsFrom: Long, tsTo: Long, s3FoldersPlusMinus: Int = 1, includeLastDay : Boolean = false, isTest : Boolean = false) : String = {

  println("getS3PathForDates isTest = $isTest")
  if(isTest){
    return s"dt=$testDate"
  } 
  
  val dtFromRaw = new DateTime(tsFrom).minusDays(s3FoldersPlusMinus)
  val dtToRaw = new DateTime(tsTo).plusDays(s3FoldersPlusMinus).plusMinutes(1) //if 23:59 then it's next day

  val dtFrom = if (dtFromRaw.isBefore(earliestDayWeHaveDataFor)) earliestDayWeHaveDataFor else dtFromRaw
  val dtTo = if (dtToRaw.isBefore(earliestDayWeHaveDataFor)) {
      earliestDayWeHaveDataFor 
    } else {
      if (dtToRaw.isAfter(latestDayWeHaveDataFor)) {
        latestDayWeHaveDataFor
      } else {
        dtToRaw
      }
    }
  
  val days = Days.daysBetween(dtFrom, dtTo).getDays
  val list = if (includeLastDay) {
    for (day <- 0 to days) yield {
      val dt = dtFrom.plusDays(day)
      s"dt=${dt.getYear}-${"%02d".format(dt.getMonthOfYear)}-${"%02d".format(dt.getDayOfMonth)}"
    }
  } else {
    for (day <- 0 until days) yield {
      val dt = dtFrom.plusDays(day)
      s"dt=${dt.getYear}-${"%02d".format(dt.getMonthOfYear)}-${"%02d".format(dt.getDayOfMonth)}"
    }
  }

  if (list.size>0) {
    list.mkString(",")
  } else {
    s"NOT_FOUND for period: $dtFromRaw to $dtToRaw"
  }  
}

def getS3PathForDatesOpt(tsFrom: Long, tsTo: Long, s3FoldersPlusMinus: Int = 1) : Option[String] = {

  val dtFromRaw = new DateTime(tsFrom).minusDays(s3FoldersPlusMinus)
  val dtToRaw = new DateTime(tsTo).plusDays(s3FoldersPlusMinus).plusMinutes(1) //if 23:59 then it's next day

  val dtFrom = if (dtFromRaw.isBefore(earliestDayWeHaveDataFor)) earliestDayWeHaveDataFor else dtFromRaw
  val dtTo = if (dtToRaw.isBefore(earliestDayWeHaveDataFor)) {
      earliestDayWeHaveDataFor 
    } else {
      if (dtToRaw.isAfter(latestDayWeHaveDataFor)) {
        latestDayWeHaveDataFor
      } else {
        dtToRaw
      }
    }
  
  val days = Days.daysBetween(dtFrom, dtTo).getDays
  val list = for (day <- 0 until days) yield {
    val dt = dtFrom.plusDays(day)
    s"dt=${dt.getYear}-${"%02d".format(dt.getMonthOfYear)}-${"%02d".format(dt.getDayOfMonth)}"
  }

  if (list.size>0) {
    Some(list.mkString(","))
  } else {
    None
  }  
}

def getS3PathsForType(s3Folder: String, s3Paths:String, esType:String) = {
  for (path <- s3Paths.split(",")) yield s"$s3Folder/${esType}_s3_v2/$path/*"
}

  def getS3PathsForIndex(s3Folder: String, esType: String, indexByName: String, indexByValuesPathsMap: Map[String, String]) : List[String] = {
    (for (indexValue <- indexByValuesPathsMap.keys) yield {
      for (path <- indexByValuesPathsMap(indexValue).split(",").par) yield {
        val fullPath = s"$s3Folder/$indexByName/${esType}_s3_v2/$indexValue/$path"
        if (isPathExists(fullPath)) {
          s"$fullPath/*"
        } else {
          null
        }
      }
    }).flatten.filter(_ != null).toList
  }

def isPathExists(path:String) = {
  (try {
    dbutils.fs.ls(path).length
  } catch {
    case ex: org.apache.spark.sql.AnalysisException => 0
    case ex2: java.io.FileNotFoundException => 0
  }) > 0
}


def resolveHostToIpIfNecessary(host:String ):String= {
  val hostAddress = java.net.InetAddress.getByName(host).getHostAddress() 
  return hostAddress 
}

def extractTime(row: Row, idx: Int): Long = {
  if (row.isNullAt(idx)) {
    0l
  } else {
    if (row.get(idx).isInstanceOf[Timestamp]) {
      row.getTimestamp(idx).getTime
    } else if (row.get(idx).isInstanceOf[String]) {
      row.getString(idx).toLong
    } else {
      row.get(idx).asInstanceOf[Long]
    }
  }
}

def extractLong(row: Row, idx: Int): Long = {
  if (row.isNullAt(idx)) {
    0l
  } else {
    row.getLong(idx)
  } 
}

def extractString(row: Row, idx: Int): String = {
  if (row.isNullAt(idx)) {
    null
  } else {
    row.getString(idx)
  } 
}

def extractTimestamp(row: Row, idx: Int): Timestamp = {
  if (row.isNullAt(idx)) {
    null
  } else {
    row.getTimestamp(idx)
  } 
}

def roundToPrecision(number:Double, precision: Int) : Double = {
  scala.math.BigDecimal(number).setScale(precision, scala.math.BigDecimal.RoundingMode.HALF_UP).toDouble
}

def extractNullableString(row: Row, index: Int) = {
  if (row.isNullAt(index)) {
    null
  } else {
    val value = row.getString(index)
    if (value.equals("null")) {
      null
    } else {
      value
    }
  }
}

def roundConxTimestamp(actionTs: Long, at: String) = {
  if (at.equals("v")) {
    val ms = 600000 //10 min in ms
    ((actionTs / ms).toLong * ms).toLong
  } else {
    actionTs
  }
}


// COMMAND ----------

// val tsFrom = new DateTime(2017, 11, 10, 0, 0).getMillis
// val tsTo = new DateTime(2017, 11, 16, 0, 0, 0).getMillis
// getS3PathForDates(
//   tsFrom,
//   tsTo,  
//   0, true
// )
// // val path = getS3PathForDates(1510272000000l, 1510876800000l, 0)
// // getS3PathsForType("/mnt/non_prod_events/qa/raw_logs", path, "searchx")


// val s3FoldersPlusMinus = 0

//   val dtFromRaw = new DateTime(tsFrom).minusDays(s3FoldersPlusMinus)
//   val dtToRaw = new DateTime(tsTo).plusDays(s3FoldersPlusMinus).plusMinutes(1) //if 23:59 then it's next day

//   val dtFrom = if (dtFromRaw.isBefore(earliestDayWeHaveDataFor)) earliestDayWeHaveDataFor else dtFromRaw
//   val dtTo = if (dtToRaw.isBefore(earliestDayWeHaveDataFor)) {
//       earliestDayWeHaveDataFor 
//     } else {
//       if (dtToRaw.isAfter(latestDayWeHaveDataFor)) {
//         latestDayWeHaveDataFor
//       } else {
//         dtToRaw
//       }
//     }
  
//   val days = Days.daysBetween(dtFrom, dtTo).getDays
//   val list = for (day <- 0 to days) yield {
//     val dt = dtFrom.plusDays(day)
//     s"dt=${dt.getYear}-${"%02d".format(dt.getMonthOfYear)}-${"%02d".format(dt.getDayOfMonth)}"
//   }

//   if (list.size>0) {
//     list.mkString(",")
//   } else {
//     s"NOT_FOUND for period: $dtFromRaw to $dtToRaw"
//   } 