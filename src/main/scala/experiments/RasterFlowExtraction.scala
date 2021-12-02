package experiments

import instances.{Duration, Extent, Polygon, Trajectory}
import operatorsNew.converter.Traj2RasterConverter
import operatorsNew.selector.Selector
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime

object RasterFlowExtraction {
  // datasets/vhc_tstr datasets/vhc_metadata.json 4 118.35,29.183,120.5,30.55 1596211200 31 3600 10
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val metadata = args(1)
    val numPartitions = args(2).toInt
    val sQuery = Extent(args(3).split(",").map(_.toDouble)).toPolygon
    val tStart = args(4).toLong
    val NumDays = args(5).toInt
    val tSplit = args(6).toInt
    val sSize = args(7).toInt
    val spark = SparkSession.builder()
      .appName("IntervalSpeedExtraction")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    type TRAJ = Trajectory[Option[String], String]

    val ranges = (0 to NumDays).map(x =>
      (sQuery, Duration(x * 86400 + tStart, (x + 1) * 86400 + tStart))).toArray
    for ((spatial, temporal) <- ranges) {
      val t = nanoTime()
      val stRanges = genRaster(temporal.start, temporal.end, sQuery, tSplit, sSize)
      val selector = Selector[TRAJ](spatial, temporal, numPartitions)
      val trajRDD = selector.selectTraj(fileName, metadata)
      val converter = new Traj2RasterConverter[Option[String],
        String, Int, None.type](
        x => x.length, stRanges.map(_._1), stRanges.map(_._2))
      val rasterRDD = converter.convertWithRTree(trajRDD)
      val res = rasterRDD.collect
      val matrix = res.drop(1).foldRight(res.head)((x, y) => x.merge(y, _+_, (_, _) => None)).entries.map(_.value)
      rasterRDD.unpersist()
      println(matrix.take(10).deep)
      println(s"${temporal.start} raster extraction ${(nanoTime - t) * 1e-9} s")
    }
    sc.stop()
  }

  def genRaster(startTime: Long, endTime: Long, sRange: Polygon,
                tSplit: Int, sSize: Int): Array[(Polygon, Duration)] = {
    val durations = (startTime until endTime + 1 by tSplit).sliding(2).map(x => Duration(x(0), x(1))).toArray
    val polygons = splitSpatial(sRange, sSize)
    for (s <- polygons; t <- durations) yield (s, t)
  }

  def splitSpatial(spatialRange: Polygon, gridSize: Int): Array[Polygon] = {
    val xMin = spatialRange.getCoordinates.map(_.x).min
    val xMax = spatialRange.getCoordinates.map(_.x).max
    val yMin = spatialRange.getCoordinates.map(_.y).min
    val yMax = spatialRange.getCoordinates.map(_.y).max
    val xStep = (xMax - xMin) / gridSize
    val xs = (0 to gridSize).map(x => x * xStep + xMin).sliding(2).toArray
    val yStep = (yMax - yMin) / gridSize
    val ys = (0 to gridSize).map(y => y * yStep + yMin).sliding(2).toArray
    for (x <- xs; y <- ys) yield Extent(x(0), y(0), x(1), y(1)).toPolygon
  }
}
