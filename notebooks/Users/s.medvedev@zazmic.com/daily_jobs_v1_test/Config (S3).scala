// Databricks notebook source
import java.util.Properties

// dip_cts_1
// host: 172.18.240.158
// login: db_cluster
// pass: 8FymtV6FZEzzgbE

// Connect to MySQLs
val jdbcUsername = "alex"
val jdbcPassword = "8e6CVCG3vT3Gkks"
val jdbcHostname = "sds.emogi.com"
val jdbcPort = 3306
val jdbcDatabaseDIP ="dip_prod"
val jdbcDatabasePUB ="pub_prod"
val jdbcDatabaseCMS ="cms_prod"

val jdbcUrlDIP = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabaseDIP}?user=${jdbcUsername}&password=${jdbcPassword}"
val jdbcUrlPUB = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabasePUB}?user=${jdbcUsername}&password=${jdbcPassword}"
val jdbcUrlCMS = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabaseCMS}?user=${jdbcUsername}&password=${jdbcPassword}"

// PROD SETTINGS!
val jdbcDipCts1 = s"jdbc:mysql://172.18.240.158:3306/stats_prod?user=db_cluster&password=8FymtV6FZEzzgbE"
val jdbcDipCts1ProductAnalytics = s"jdbc:mysql://172.18.240.158:3306/product_analytics?user=db_cluster&password=8FymtV6FZEzzgbE"
val jdbcStatsProdV2 = s"jdbc:mysql://172.18.240.158:3306/stats_v2_prod?user=db_cluster&password=8FymtV6FZEzzgbE"
val storeResultsToMySQL = true


//DEV SETTINGS
// val jdbcDipCts1 = s"jdbc:mysql://172.18.240.158:3306/stats_dev?user=db_cluster&password=8FymtV6FZEzzgbE"
// val jdbcDipCts1ProductAnalytics = s"jdbc:mysql://172.18.240.158:3306/product_analytics?user=db_cluster&password=8FymtV6FZEzzgbE"
// val jdbcStatsProdV2 = s"jdbc:mysql://172.18.240.158:3306/stats_v2_prod?user=db_cluster&password=8FymtV6FZEzzgbE"
// val storeResultsToMySQL = false


// Creating dropdowns with filters
// val df = spark.read.jdbc(jdbcDipCts1, "content_reach", new Properties())
// display(df)
// val appsDF = spark.read.jdbc(jdbcUrlPUB, "pub_platform", new Properties()).toDF().drop("pub_id").toDF("app_id","app_name")

// val categoriesSeq = Seq("ALL") ++ categoriesDF.map(_.getString(1)).collect().toSeq
// dbutils.widgets.dropdown("category", categoriesSeq.head, categoriesSeq, "Category")
// val appsSeq = Seq("ALL") ++ appsDF.map(_.getString(1)).collect().toSeq
// dbutils.widgets.dropdown("app", appsSeq.head, appsSeq, "Apps")
