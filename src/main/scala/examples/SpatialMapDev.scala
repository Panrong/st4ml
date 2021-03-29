package examples

import geometry.Rectangle
import operators.convertion.Converter
import operators.extraction.SpatialMapExtractor
import operators.selection.partitioner.FastPartitioner
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
    val partitionRange = partitioner.partitionRange

    val selector = RTreeHandler(partitionRange, Some(500))
    val queriedRDD = selector.query(pRDD)(Rectangle(Array(-180, -180, 180, 180)))
    println(s"--- ${queriedRDD.count} trajectories")

    val converter = new Converter()
    val pointRDD = converter.traj2Point(queriedRDD).map((0,_))


    val tStart = pointRDD.map(_._2.timeStamp).min._1
    val tEnd = pointRDD.map(_._2.timeStamp).max._2

    println("Suppose we have points already spatially partitioned.")
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
}
