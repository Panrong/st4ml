package examples

import convertion.Converter
import extraction.SMExtractor
import geometry.Rectangle
import geometry.road.RoadGrid
import org.apache.spark.sql.SparkSession
import preprocessing.ReadMMTrajFile
import selection.partitioner.HashPartitioner
import selection.selector.{RTreeSelector, TemporalSelector}

import java.lang.System.nanoTime
import scala.io.Source

object SimpleTest {
  def main(args: Array[String]): Unit = {
    /** set up Spark environment and prepare data */
    var t = nanoTime()
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

    val trajRDD = ReadMMTrajFile(trajectoryFile, mapFile)
      .repartition(numPartitions)
      .cache()
    println(s"... Read input file done. Total ${trajRDD.count} trajectories.")
    println(s".... Time used: ${(nanoTime() - t) * 1e-9} s.")
    val sQuery = Rectangle(Array(-8.682329739182336, 41.16930767535641, -8.553892156181982, 41.17336956864337))
    val tQuery = (1372700000L, 1372750000L)

    /** initialise operators */

    val partitioner = new HashPartitioner(numPartitions)
    val pRDD = partitioner.partition(trajRDD)
    val partitionRange = partitioner.partitionRange
    val spatialSelector = new RTreeSelector(partitionRange)
    val temporalSelector = new TemporalSelector
    val converter = new Converter
    val extractor = new SMExtractor

    /** step 1: selection */
    t = nanoTime()
    val sRDD = spatialSelector.query(pRDD)(sQuery)
    val stRDD = temporalSelector.query(sRDD)(tQuery).cache()
    println(s"... Step 1 SELECTION done. Total ${stRDD.count} trajectories selected.")
    println(s".... Time used: ${(nanoTime() - t) * 1e-9} s.")

    /** step 2: conversion */

    val roadGrid = RoadGrid(mapFile)
    val speedRDD = stRDD.map(x => (x._1, x._2.getRoadSpeed(roadGrid)))
    val convertedRDD = converter.trajSpeed2SpatialMap(speedRDD).cache()
    println(s"... Step 2 CONVERSION done. Total ${convertedRDD.count} geometry.road segments.")
    println(s".... Time used: ${(nanoTime() - t) * 1e-9} s.")

    /** step 3: extraction */

    val avgSpeed = extractor.extractRoadSpeed(convertedRDD)
    println(s"... Step 3 EXTRACTION done.")
    println(s".... Time used: ${(nanoTime() - t) * 1e-9} s.")
    println("... 5 examples: ")
    for (i <- avgSpeed.take(5)) {
      println(s"Road ID: ${i._1} --- Average speed ${i._2.formatted("%.3f")} km/h")
    }
    sc.stop()
  }
}