package scala.uk.ac.ic.imperial.query

import org.apache.spark.{SparkContext, TaskContext}
import org.coroutines.{coroutine, yieldval, ~>}
import uk.ac.ic.imperial.base.SparkHelper

object SumSparkQuery {

  val sumFunc = (iter: Iterator[Long]) => iter.reduceLeft(_ + _)

  val sumCoFunc: (TaskContext, Iterator[Long]) ~> (Int, Long) =
    coroutine { (context: TaskContext, itr: Iterator[Long]) => {
      var first = true
      var acc: Long = 0L
      while (itr.hasNext) {
        if (context.isPaused()) {
          yieldval(0)
        }
        if (first) {
          acc = itr.next()
          first = false
        }
        else {
          acc = acc + itr.next()
        }
      }
      acc
    }
    }

  def SparkMultiJob(sc: SparkContext): Unit = {
    // Warm up JVM
    val execIds = sc.parallelize(0 until sc.getExecutorMemoryStatus.size,
      sc.getExecutorMemoryStatus.size).foreach { x =>
      Thread.sleep(1)
    }


    val hundredMillionElems = 100000000L
    val hundredKiloElems = 10000L
    val numIters = 1

    val appStartTime = System.nanoTime()

    val thread = new Thread {
      override def run {
        val itStartTime = System.nanoTime()
        (0 until numIters).map { i =>

          sc.setLocalProperty("neptune_pri", "2")
          val rdd = sc.parallelize(1L to hundredMillionElems).map(x => x + 1)
          sc.runJob(rdd, sumCoFunc)
        }
        val itEndTime = System.nanoTime()
        println(s"Batch Sum took: ${(itEndTime - itStartTime) / 1e6} ms")
      }
    }
    thread.start
    // Second job submitted after 0.5 seconds
    Thread.sleep( 500)
    val itStartTime = System.nanoTime()

    (0 until numIters).map { i =>

        sc.setLocalProperty("neptune_pri", "1")
        val rdd = sc.parallelize(1L to hundredKiloElems).map(x => x + 1)
        sc.runJob(rdd, sumCoFunc)
    }
    val itEndTime = System.nanoTime()
    println(s"Stream Sum took: ${(itEndTime - itStartTime) / 1e6} ms")
  }

  def main(args: Array[String]): Unit = {
    val ss = SparkHelper.
      getAndConfigureSparkSession(appName = "MultiSumNeptune", neptuneCoroutines = true)
    SparkMultiJob(ss.sparkContext)
  }

}

