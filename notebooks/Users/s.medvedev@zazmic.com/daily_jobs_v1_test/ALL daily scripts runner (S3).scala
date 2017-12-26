// Databricks notebook source
// Errors in workflows thrown a WorkflowException. For now, WorkflowException provides a simple
// message of why the run failed. We are evaluating a more comprehensive errors API - please
// file/upvote a feature request at feedback.databricks.com.
import com.databricks.WorkflowException

val notebookTimeoutSec = 24*60*60 //1 day
val notebook = "/Users/s.medvedev@zazmic.com/daily_jobs_v1_test/ALL daily scripts (S3)"

// Since dbutils.notebook.run() is just a function call, you can retry failures using standard Scala try-catch
// control flow. Here we show an example of retrying a notebook a number of times.
def runRetry(notebook: String, timeoutMinutes: Int, args: Map[String, String] = Map.empty, maxTries: Int = 3): String = {
  var numTries = 0
  while (true) {
    try {
      println(s"Starting notebook=$notebook, numTries=$numTries")
      return dbutils.notebook.run(notebook, notebookTimeoutSec, args)
    } catch {
      case e: WorkflowException if numTries < maxTries =>
        println(s"Error, retrying in $timeoutMinutes minutes: " + e)
        Thread.sleep(timeoutMinutes * 60 * 1000) 
    }
    numTries += 1
  }
  "" // not reached
}

runRetry(notebook, timeoutMinutes = 5, maxTries = 10)