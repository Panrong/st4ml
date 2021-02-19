package examples

import geometry.Rectangle
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import preprocessing.ReadTrajFile
import selection.partitioner._
import selection.selector.{RTreeSelector, TemporalSelector}

import java.lang.System.nanoTime
import scala.io.Source
import scala.math.{max, sqrt}

object SelectorTest {
  def main(args: Array[String]): Unit = {
    var t = nanoTime()
    /** set up Spark environment */
    var config: Map[String, String] = Map()
    val f = Source.fromFile("config")
    f.getLines
      .filterNot(_.startsWith("//"))
      .filterNot(_.startsWith("\n"))
      .foreach(l => {
        val p = l.split(" ")
        config = config + (p(0) -> p(1))
      })
    f.close()
    val spark = SparkSession
      .builder()
      .master(config("master"))
      .appName(config("appName"))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val trajectoryFile = args(0)
    val numPartitions = args(1).toInt
    val dataSize = args(2).toInt
    val limit = args(3).toBoolean
    val count = args(4).toBoolean

    val trajRDD = ReadTrajFile(trajectoryFile, num = dataSize, numPartitions, count = count, limit = limit)
      .cache()
    val sQuery = Rectangle(Array(-8.682329739182336, 41.16930767535641, -8.553892156181982, 41.17336956864337))
    val tQuery = (1372700000L, 1372750000L)
    //val tQuery = (1399900000L, 1400000000L)
    val rTreeCapacity = max(sqrt(dataSize / numPartitions).toInt, 100)

    println("\n*-*-*-*-*-*-*-*-*-*-*-*")

    /** benchmark */
    t = nanoTime()
    val fullSRDD = trajRDD.filter(x => x.intersect(sQuery))
    val fullSTRDD = fullSRDD.filter(x => {
      val (ts, te) = x.timeStamp
      ts <= tQuery._2 && ts >= tQuery._1 || te <= tQuery._2 && te >= tQuery._1
    })
    fullSTRDD.take(1)
    println(s"... Full scanning takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    if (count) {
      t = nanoTime()
      println(s"*- Full scan S: ${fullSRDD.count} ")
      println(s"*- Full scan ST: ${fullSTRDD.count} ")
      println(s"... Counting takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.\n")
    }
    println("*-*-*-*-*-*-*-*-*-*-*-*")

    /** test hash partitioner */
    println("==== HASH Partitioner ====")

    t = nanoTime()
    val hashPartitioner = new HashPartitioner(numPartitions)
    val pRDDHash = hashPartitioner.partition(trajRDD).cache()
    val partitionRangeHash = hashPartitioner.partitionRange
    val selectorHash = new RTreeSelector(sQuery, partitionRangeHash, Some(rTreeCapacity))

    pRDDHash.take(1)
    println(s"... Partitioning takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    t = nanoTime()
    val queriedRDD2Hash = selectorHash.query(pRDDHash).cache()
    queriedRDD2Hash.take(1)
    println(s"... Querying with index takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    if (count) {
      t = nanoTime()
      println(s"==== Queried dataset contains ${queriedRDD2Hash.count} entries (RTree)")
      println(s"... Counting takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.\n")
    }
    t = nanoTime()
    val temporalSelectorH = new TemporalSelector(tQuery)
    val queriedRDD3Hash = temporalSelectorH.query(queriedRDD2Hash)
    queriedRDD3Hash.take(1)
    println(s"... Temporal querying takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    if (count) {
      t = nanoTime()
      println(s"==== Queried dataset contains ${queriedRDD3Hash.count} entries (ST)")
      println(s"... Counting takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.\n")
    }
    pRDDHash.unpersist()
    queriedRDD2Hash.unpersist()

    //    /** test quadTree partitioner */
    //    println("\n==== quadTree ====")
    //
    //    t = nanoTime()
    //    val quadTreePartitioner = new QuadTreePartitioner(numPartitions, Some(0.2))
    //    val pRDDQt = quadTreePartitioner.partition(trajRDD).cache()
    //    val partitionRangeQt = quadTreePartitioner.partitionRange
    //
    //    val selectorQt = new RTreeSelector(sQuery, partitionRangeQt, Some(rTreeCapacity))
    //    pRDDQt.count()
    //    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")
    //
    //    t = nanoTime()
    //    val queriedRDD2Qt = selectorQt.query(pRDDQt).cache()
    //    println(s"==== Queried dataset contains ${queriedRDD2Qt.count} entries (RTree)")
    //    println(s"... Querying with index takes ${(nanoTime() - t) * 1e-9} s.")
    //
    //    t = nanoTime()
    //    val temporalSelectorQt = new TemporalSelector(tQuery)
    //    val queriedRDD3Qt = temporalSelectorQt.query(queriedRDD2Qt)
    //    println(s"==== Queried dataset contains ${queriedRDD3Qt.count} entries (ST)")
    //    println(s"... Temporal querying takes ${(nanoTime() - t) * 1e-9} s.")

    sc.stop()
  }
}
