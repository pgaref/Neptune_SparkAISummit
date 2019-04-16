package uk.ac.ic.imperial.query

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import uk.ac.ic.imperial.base.SparkHelper

object JoinQuery {

  def rddJoin(sc: SparkContext, leftRDD: RDD[(Int, Int)], rightRDD: RDD[(Int, Int)]): Unit = {
    rightRDD.join(leftRDD).count()
  }

  def SparkMultiJob(sc: SparkContext): Unit = {

    // Warm up JVM
    val execIds = sc.parallelize(0 until sc.getExecutorMemoryStatus.size,
      sc.getExecutorMemoryStatus.size).foreach { x =>
      Thread.sleep(1)
    }

    // Creating data
    val oneKiloInts = sc.parallelize(1 to 1000).map(x => (x, x + 1)).cache()
    val oneKiloIntsl = sc.parallelize(1 to 1000).map(x => (x, x)).cache()
    val oneMillionInts = sc.parallelize(1 to 1000000).map(x => (x, x)).cache()
    val twoMillionInts = sc.parallelize(1 to 2000000).map(x => (x, x + 1)).cache()
    println("Done creating Data")


    val queryStartTime = System.nanoTime()
    val batchJobStartTime = System.nanoTime()
    var batchJobEndTime = 0L

    val thread = new Thread {
      override def run {

        sc.setLocalProperty("neptune_pri", "2")
        rddJoin(sc, oneMillionInts, twoMillionInts)

        batchJobEndTime = System.nanoTime()
        println(s"Batch Join took: ${(batchJobEndTime - batchJobStartTime) / 1e6} ms")
      }
    }
    thread.start
    // Second job submitted after 0.5s
    Thread.sleep(500)
    val streamJobStartTime = System.nanoTime()

    sc.setLocalProperty("neptune_pri", "1")
    rddJoin(sc, oneKiloIntsl, oneKiloInts)

    val streamJobEndTime = System.nanoTime()
    println(s"Stream Join took ${(streamJobEndTime - streamJobStartTime) / 1e6} ms")
  }

  def main(args: Array[String]): Unit = {

    val ss = SparkHelper.
      getAndConfigureSparkSession(appName = "MultiJoin", neptuneCoroutines = true)
    SparkMultiJob(ss.sparkContext)
  }

}

