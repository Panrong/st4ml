package examples

import geometry.{Rectangle, mmTrajectory}
import org.apache.spark.sql.{Dataset, SparkSession}
import preprocessing.ReadMMTrajFile
import selection.partitioner.{HashPartitioner, STRPartitioner}
import selection.selector.{FilterSelector, RTreeSelector, TemporalSelector}
import convertion.Converter
import extraction.SMExtractor
import road.RoadGrid

import scala.io.Source

object SMTest extends App {
  override def main(args: Array[String]): Unit = {

    import java.lang.System.nanoTime

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
    val numPartitions = args(0).toInt
    val trajectoryFile = args(1)
    val mapFile = args(2)

    /** **********************************
     * test map-matched trajectory dataset
     * ********************************** */

    val trajDS: Dataset[mmTrajectory] = ReadMMTrajFile(trajectoryFile, mapFile)
    val trajRDD = trajDS.rdd
    val sQuery = Rectangle(Array(-8.682329739182336, 41.16930767535641, -8.553892156181982, 41.17336956864337))
    val tQuery = (1372630000L, 1372660000L)

    println(s"\nOriginal trajectory dataset contains ${trajRDD.count} entries")
    println("\n*-*-*-*-*-*-*-*-*-*-*-*")

    /** benchmark */
    t = nanoTime()
    val fullSRDD = trajRDD.filter(x => x.intersect(sQuery))
    println(s"*- Full scan S: ${fullSRDD.count} -*")
    val fullSTRDD = fullSRDD.filter(x => {
      val (ts, te) = x.timeStamp
      ts <= tQuery._2 && ts >= tQuery._1 || te <= tQuery._2 && te >= tQuery._1
    })
    println(s"*- Full scan ST: ${fullSTRDD.count} -*")
    println("*-*-*-*-*-*-*-*-*-*-*-*")
    println(s"... Full scanning takes ${(nanoTime() - t) * 1e-9} s.\n")

    /** partition */
    println("==== STR ====")
    t = nanoTime()
    val partitioner = new STRPartitioner(numPartitions)
    val pRDD = partitioner.partition(trajRDD)
    val partitionRange = partitioner.partitionRange
    val filterSelector = new FilterSelector(sQuery, partitionRange)
    val rtreeSelector = new RTreeSelector(sQuery, partitionRange)

    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")

    /** spatial query by filtering */
    t = nanoTime()
    val queriedRDD1 = filterSelector.query(pRDD) //.map(x => (x._2.id, x)).groupByKey().flatMap(x => x._2.take(1))
    println(s"==== Queried dataset contains ${queriedRDD1.count} entries (filtering)")
    println(s"... Querying by filtering takes ${(nanoTime() - t) * 1e-9} s.")

    /** spatial query with index */
    t = nanoTime()
    val queriedRDD2 = rtreeSelector.query(pRDD) //.map(x => (x._2.id, x)).groupByKey().flatMap(x => x._2.take(1))


    println(s"==== Queried dataset contains ${queriedRDD2.count} entries (RTree)")
    println(s"... Querying with index takes ${(nanoTime() - t) * 1e-9} s.")

    /** temporal query by filtering */
    t = nanoTime()
    val temporalSelector = new TemporalSelector(tQuery)
    val queriedRDD3 = temporalSelector.query(queriedRDD2)
    //    queriedRDD3.take(6).foreach(println(_))
    println(s"==== Queried dataset contains ${queriedRDD3.count} entries (ST)")
    println(s"... Temporal querying takes ${(nanoTime() - t) * 1e-9} s.")

    /** test hash partitioner */
    println("\n==== HASH ====")

    t = nanoTime()
    val hashPartitioner = new HashPartitioner(numPartitions)
    val pRDDHash = hashPartitioner.partition(trajRDD)
    val partitionRangeHash = hashPartitioner.partitionRange
    val selectorHash = new FilterSelector(sQuery, partitionRangeHash)
    val rtreeselectorHash = new FilterSelector(sQuery, partitionRangeHash)

    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val queriedRDD1Hash = selectorHash.query(pRDDHash)
    println(s"==== Queried dataset contains ${queriedRDD1Hash.count} entries (filtering)")
    println(s"... Querying by filtering takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val queriedRDD2Hash = rtreeselectorHash.query(pRDDHash)
    println(s"==== Queried dataset contains ${queriedRDD2Hash.count} entries (RTree)")
    println(s"... Querying with index takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val temporalSelectorH = new TemporalSelector(tQuery)
    val queriedRDD3Hash = temporalSelectorH.query(queriedRDD2Hash)
    println(s"==== Queried dataset contains ${queriedRDD3Hash.count} entries (ST)")
    println(s"... Temporal querying takes ${(nanoTime() - t) * 1e-9} s.")

    /** add speed info */
    val roadGrid = RoadGrid(mapFile)
    val speedRDD = queriedRDD3.map(x => (x._1, x._2.getRoadSpeed(roadGrid)))

    /** test conversion */
    val converter = new Converter
    val convertedRDD = converter.trajSpeed2SpatialMap(speedRDD)

    /** test extraction */
    val extractor = new SMExtractor
    val avgSpeed = extractor.extractRoadSpeed(convertedRDD)
    println("\n=== Average Speed === :")
    for (i <- avgSpeed.take(5)) {
      println(s"Road ID: ${i._1} --- Average speed ${i._2.formatted("%.3f")} km/h")
    }
    sc.stop()
  }
}

object SimpleTest extends App {

  override def main(args: Array[String]): Unit = {
    /** set up Spark environment and prepare data */
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
    val numPartitions = args(0).toInt
    val trajectoryFile = args(1)
    val mapFile = args(2)

    val trajDS: Dataset[mmTrajectory] = ReadMMTrajFile(trajectoryFile, mapFile)
    val trajRDD = trajDS.rdd
    val sQuery = Rectangle(Array(-8.682329739182336, 41.16930767535641, -8.553892156181982, 41.17336956864337))
    val tQuery = (1372630000L, 1372660000L)

    /** initialise operators */

    val partitioner = new STRPartitioner(numPartitions)
    val pRDD = partitioner.partition(trajRDD)
    val partitionRange = partitioner.partitionRange
    val spatialSelector = new RTreeSelector(sQuery, partitionRange)
    val temporalSelector = new TemporalSelector(tQuery)
    val converter = new Converter
    val extractor = new SMExtractor

    /** step 1: selection */

    val sRDD = spatialSelector.query(pRDD)
    val stRDD = temporalSelector.query(sRDD)

    /** step 2: conversion */

    val roadGrid = RoadGrid(mapFile)
    val speedRDD = stRDD.map(x => (x._1, x._2.getRoadSpeed(roadGrid)))
    val convertedRDD = converter.trajSpeed2SpatialMap(speedRDD)

    /** step 3: extraction */

    val avgSpeed = extractor.extractRoadSpeed(convertedRDD)
    for (i <- avgSpeed.take(5)) {
      println(s"Road ID: ${i._1} --- Average speed ${i._2.formatted("%.3f")} km/h")
    }

    sc.stop()
  }
}