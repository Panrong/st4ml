package examples

import geometry.Rectangle
import operators.selection.partitioner.{HashPartitioner, QuadTreePartitioner}
import operators.selection.selectionHandler.{RTreeHandler, TemporalSelector}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import preprocessing.{ReadTrajFile, ReadTrajJson}
import utils.Config

import java.lang.System.nanoTime
import scala.math.{max, sqrt}

object SelectorDev {
  def main(args: Array[String]): Unit = {
    var t = nanoTime()
    /** set up Spark environment */
    val spark = SparkSession
      .builder()
      .master(Config.get("master"))
      .appName("SelectorDev")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val trajectoryFile = Config.get("hzData")
    val numPartitions = Config.get("numPartitions").toInt

//    val trajRDD = ReadTrajJson(trajectoryFile, numPartitions)
//      .persist(StorageLevel.MEMORY_AND_DISK)
    val trajRDD = ReadTrajFile(Config.get("portoData"),10000000)
    println(trajRDD.count)
    val dataSize = trajRDD.count

    val sQuery = Rectangle(Array(-9.020497248267002, 39.65487571786815, -8.72821462082292, 42.006813716041854))
    val tQuery = (1372684617L, 1372701688L)
    val rTreeCapacity = max(sqrt(dataSize / numPartitions).toInt, 100)

    println("\n*-*-*-*-*-*-*-*-*-*-*-*")

    /** benchmark */
    t = nanoTime()
    val fullSRDD = trajRDD.filter(x => x.intersect(sQuery))
    println(s"*- Full scan S: ${fullSRDD.count} -*")
    val fullSTRDD = fullSRDD.filter(x => {
      val (ts, te) = x.timeStamp
      (ts <= tQuery._2 && ts >= tQuery._1) || (te <= tQuery._2 && te >= tQuery._1)
    })
    println(s"*- Full scan ST: ${fullSTRDD.count} -*")
    println("*-*-*-*-*-*-*-*-*-*-*-*")
    println(s"... Full scanning takes ${(nanoTime() - t) * 1e-9} s.\n")

    /** test hash partitioner */
    println("==== HASH ====")

    t = nanoTime()
    val hashPartitioner = new HashPartitioner(numPartitions)
    val pRDDHash = hashPartitioner.partition(trajRDD).cache()
    val partitionRangeHash = hashPartitioner.partitionRange

    val selectorHash = RTreeHandler(partitionRangeHash, Some(rTreeCapacity))
    pRDDHash.count()
    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val queriedRDD2Hash = selectorHash.query(pRDDHash)(sQuery).cache()
    println(s"==== Queried dataset contains ${queriedRDD2Hash.count} entries (RTree)")
    println(s"... Querying with index takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val temporalSelectorH = new TemporalSelector
    val queriedRDD3Hash = temporalSelectorH.query(queriedRDD2Hash)(tQuery)
    println(s"==== Queried dataset contains ${queriedRDD3Hash.count} entries (ST)")
    println(s"... Temporal querying takes ${(nanoTime() - t) * 1e-9} s.")

    /** test quadTree partitioner */
    println("\n==== quadTree ====")

    t = nanoTime()
    val quadTreePartitioner = new QuadTreePartitioner(numPartitions, Some(Config.get("samplingRate").toDouble))
    val pRDDQt = quadTreePartitioner.partition(trajRDD).cache()
    val partitionRangeQt = quadTreePartitioner.partitionRange

    val selectorQt = RTreeHandler(partitionRangeQt, Some(rTreeCapacity))
    pRDDQt.count()
    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val queriedRDD2Qt = selectorQt.query(pRDDQt)(sQuery).cache()
    println(s"==== Queried dataset contains ${queriedRDD2Qt.count} entries (RTree)")
    println(s"... Querying with index takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val temporalSelectorQt = new TemporalSelector
    val queriedRDD3Qt = temporalSelectorQt.query(queriedRDD2Qt)(tQuery)
    println(s"==== Queried dataset contains ${queriedRDD3Qt.count} entries (ST)")
    println(s"... Temporal querying takes ${(nanoTime() - t) * 1e-9} s.")

    sc.stop()
  }
}
