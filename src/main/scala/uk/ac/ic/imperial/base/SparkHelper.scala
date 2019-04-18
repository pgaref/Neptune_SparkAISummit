package uk.ac.ic.imperial.base

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}


object SparkHelper {
  def getAndConfigureSparkSession(appName: String, parallelism: Int = 1, neptuneCoroutines: Boolean = true) = {
    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.default.parallelism", s"${parallelism}")
      .setIfMissing("spark.master", s"local[${parallelism}]")

//    conf.set("spark.eventLog.enabled", "true")
    if (neptuneCoroutines) {
      conf.set("spark.scheduler.mode", "NEPTUNE")
      conf.set("spark.neptune.scheduling.policy", "load_balance")
      conf.enableNeptuneCoroutines()
    }

    new SparkContext(conf)

    SparkSession
      .builder()
      .getOrCreate()
  }

  def getSparkSession() = {
    SparkSession
      .builder()
      .getOrCreate()
  }
}