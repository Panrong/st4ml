package examples

import experiments.SelectionExp.readQueries
import geometry.{Rectangle, Trajectory}
import operators.convertion.Traj2PointConverter
import operators.selection.partitioner.{HashPartitioner, QuadTreePartitioner, STRPartitioner}
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
    val numPartitions = args(0).toInt

    //    val trajRDD = ReadTrajJson(trajectoryFile, numPartitions)
    //      .persist(StorageLevel.MEMORY_AND_DISK)

    // val trajRDD = ReadTrajFile(Config.get("portoData"), 10000000)
    val trajRDD = new Traj2PointConverter().convert(ReadTrajFile(Config.get("portoData"), 10000000))
    println(trajRDD.count)
    val dataSize = trajRDD.count

    val queries = readQueries(Config.get("portoQuery")).take(10)

    val rTreeCapacity = max(sqrt(dataSize / numPartitions).toInt, 100)

    println("\n*-*-*-*-*-*-*-*-*-*-*-*")

    /** benchmark */
    t = nanoTime()
    for (query <- queries) {
      val tQuery = (query(4).toLong, query(5).toLong)
      val sQuery = Rectangle(query.slice(0, 4))
      val fullSRDD = trajRDD.filter(x => x.intersect(sQuery))
//      println(s"*- Full scan S: ${fullSRDD.count} -*")
      val fullSTRDD = fullSRDD.filter(x => {
        val (ts, te) = x.timeStamp
        (ts <= tQuery._2 && ts >= tQuery._1) || (te <= tQuery._2 && te >= tQuery._1)
      })
      println(s"*- Full scan ST: ${fullSTRDD.count} -*")
    }
    println("*-*-*-*-*-*-*-*-*-*-*-*")
    println(s"... Full scanning takes ${(nanoTime() - t) * 1e-9} s.\n")

    /** test hash partitioner */
    println("==== HASH ====")

    t = nanoTime()
    val hashPartitioner = new HashPartitioner(numPartitions)
    val pRDDHash = hashPartitioner.partition(trajRDD)
    val partitionRangeHash = hashPartitioner.partitionRange

    val selectorHash = RTreeHandler(partitionRangeHash, Some(rTreeCapacity))
    pRDDHash.count()
    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val temporalSelectorH = new TemporalSelector

    for (query <- queries) {
      val tQuery = (query(4).toLong, query(5).toLong)
      val sQuery = Rectangle(query.slice(0, 4))
      val queriedRDD2Hash = selectorHash.query(pRDDHash)(sQuery)
      val queriedRDD3Hash = temporalSelectorH.query(queriedRDD2Hash)(tQuery)
      println(s"==== Queried dataset contains ${queriedRDD3Hash.count} entries (ST)")
    }
    println(s"... Querying takes ${(nanoTime() - t) * 1e-9} s.")

    /** test quadTree partitioner */
    println("\n==== quadTree ====")

    t = nanoTime()
    val quadTreePartitioner = new QuadTreePartitioner(numPartitions, Some(Config.get("samplingRate").toDouble))
    val pRDDQt = quadTreePartitioner.partition(trajRDD)
    val partitionRangeQt = quadTreePartitioner.partitionRange
    val selectorQt = RTreeHandler(partitionRangeQt, Some(rTreeCapacity))
    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val temporalSelectorQt = new TemporalSelector
    for (query <- queries) {
      val tQuery = (query(4).toLong, query(5).toLong)
      val sQuery = Rectangle(query.slice(0, 4))
      val queriedRDD2Qt = selectorQt.query(pRDDQt)(sQuery)
      val queriedRDD3Qt = temporalSelectorQt.query(queriedRDD2Qt)(tQuery)
      println(s"==== Queried dataset contains ${queriedRDD3Qt.count} entries (ST)")
    }
    println(s"... Querying takes ${(nanoTime() - t) * 1e-9} s.")

    /** test STR partitioner */
    println("\n==== STR ====")

    t = nanoTime()
    val strPartitioner = new STRPartitioner(numPartitions, Some(Config.get("samplingRate").toDouble))
    val pRDDSTR = strPartitioner.partition(trajRDD)
    val partitionRangeSTR = strPartitioner.partitionRange

    val selectorSTR = RTreeHandler(partitionRangeSTR, Some(rTreeCapacity))
    pRDDSTR.count()
    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")
    val temporalSelectorSTR = new TemporalSelector

    t = nanoTime()
    for (query <- queries) {
      val tQuery = (query(4).toLong, query(5).toLong)
      val sQuery = Rectangle(query.slice(0, 4))
      val queriedRDD2STR = selectorSTR.query(pRDDSTR)(sQuery).cache()
      val queriedRDD3STR = temporalSelectorSTR.query(queriedRDD2STR)(tQuery)
      println(s"==== Queried dataset contains ${queriedRDD3STR.count} entries (ST)")
    }
    println(s"... Querying takes ${(nanoTime() - t) * 1e-9} s.")

    sc.stop()
  }
}
