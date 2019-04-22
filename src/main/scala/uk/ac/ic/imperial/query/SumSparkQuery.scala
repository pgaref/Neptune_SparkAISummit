package uk.ac.ic.imperial.query

import org.apache.spark.SparkContext
import uk.ac.ic.imperial.base.SparkHelper

object SumSparkQuery {

  def SparkMultiJob(sc: SparkContext): Unit = {
    // Warm up JVM
    val execIds = sc.parallelize(0 until sc.getExecutorMemoryStatus.size,
      sc.getExecutorMemoryStatus.size).foreach { x =>
      Thread.sleep(1)
    }


    val hundredMillionElems = 100000000L
    val oneMillionElems = 1000000L
    val numIters = 1

    val thread = new Thread {
      override def run {
        val itStartTime = System.nanoTime()
        (0 until numIters).map { i =>

          sc.parallelize(1L to hundredMillionElems).map(x => x + 1).sum
        }
        val itEndTime = System.nanoTime()
        println(s"Batch Sum took: ${(itEndTime - itStartTime) / 1e6} ms")
      }
    }
    thread.start
    // Second job submitted after 0.5 seconds
    Thread.sleep(500)
    val itStartTime = System.nanoTime()

    (0 until numIters).map { i =>

        sc.parallelize(1L to oneMillionElems).map(x => x + 1).sum
    }
    val itEndTime = System.nanoTime()
    println(s"Stream Sum took: ${(itEndTime - itStartTime) / 1e6} ms")
  }

  def main(args: Array[String]): Unit = {
    val ss = SparkHelper.
      getAndConfigureSparkSession(appName = "MultiSumSpark", neptuneCoroutines = false)
    SparkMultiJob(ss.sparkContext)
  }

}

