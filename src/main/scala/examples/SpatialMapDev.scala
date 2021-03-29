package examples

import geometry.Rectangle
import operators.convertion.Converter
import operators.extraction.SpatialMapExtractor
import operators.selection.partitioner.{FastPartitioner, QuadTreePartitioner}
import operators.selection.selectionHandler.RTreeHandler
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import preprocessing.ReadTrajJson
import utils.Config

import java.lang.System.nanoTime

object SpatialMapDev {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master(Config.get("master"))
      .appName("SpatialMapDev")
      .getOrCreate()

    val trajectoryFile = Config.get("hzData")
    val numPartitions = Config.get("numPartitions").toInt
    val trajRDD = ReadTrajJson(trajectoryFile, numPartitions)
      .persist(StorageLevel.MEMORY_AND_DISK)
    val sQuery = Rectangle(Array(118.116, 29.061, 120.167, 30.184))
    val tQuery = (1597000000L, 1598000000L)

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val partitioner = new FastPartitioner(numPartitions)
    val pRDD = partitioner.partition(trajRDD)

    val wholeRange = partitioner.partitionRange(0)
    val partitionRange = gridPartition(wholeRange.coordinates, 4).zipWithIndex.map(_.swap).toMap

    val selector = RTreeHandler(partitioner.partitionRange, Some(500))
    val queriedRDD = selector.query(pRDD)(Rectangle(Array(-180, -180, 180, 180)))
    println(s"--- ${queriedRDD.count} trajectories")

    val converter = new Converter()
    val pointRDD = converter.traj2Point(queriedRDD).map((0, _))


    val tStart = pointRDD.map(_._2.timeStamp).min._1
    val tEnd = pointRDD.map(_._2.timeStamp).max._2

    var t = nanoTime()
    val spatialMapRDD = converter.point2SpatialMap(pointRDD, tStart, tEnd, partitionRange)

    spatialMapRDD.take(1)
    println(s"... Conversion takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    t = nanoTime()

    val extractor = new SpatialMapExtractor()
    val extractedRDD = extractor.rangeQuery(spatialMapRDD, sQuery, tQuery)
    println(s"... Total ${extractedRDD.count} points")
    println(s"... Extraction takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    t = nanoTime()

    val gt = pointRDD.filter(p => p._2.inside(sQuery) && p._2.t <= tQuery._2 && p._2.t >= tQuery._1).count
    println(s"... (Benchmark) Total $gt points")
    println(s"... Benchmark takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
  }

  def gridPartition(sRange: Array[Double], gridSize: Int): Array[Rectangle] = {
    val longInterval = (sRange(2) - sRange(0)) / gridSize
    val latInterval = (sRange(3) - sRange(1)) / gridSize
    val longSeparations = (0 until gridSize)
      .map(t => (sRange(0) + t * longInterval, sRange(0) + (t + 1) * longInterval)).toArray
    val latSeparations = (0 until gridSize)
      .map(t => (sRange(1) + t * latInterval, sRange(1) + (t + 1) * latInterval)).toArray
    for ((longMin, longMax) <- longSeparations;
         (latMin, latMax) <- latSeparations)
      yield Rectangle(Array(longMin, latMin, longMax, latMax))
  }
}
