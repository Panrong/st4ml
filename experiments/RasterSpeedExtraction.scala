// For case study

package experiments

import org.apache.spark.sql.SparkSession
import st4ml.instances.{Duration, Extent, Polygon, Trajectory}
import st4ml.operators.converter.Traj2RasterConverter
import st4ml.operators.selector.Selector
import st4ml.utils.Config

import java.io.FileWriter
import java.lang.System.nanoTime

object RasterSpeedExtraction {
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
      val converter = new Traj2RasterConverter(stRanges.map(_._1), stRanges.map(_._2))
      val preMap: TRAJ => Trajectory[Option[String], Double] = x => {
        val speed = x.consecutiveSpatialDistance("greatCircle").sum / x.duration.seconds * 3.6
        Trajectory(x.entries, speed)
      }
      val agg: Array[Trajectory[Option[String], Double]] => (Double, Int) = x => {
        val res = x.map(t => (t.data, 1)).filter(_._1 > 0)
        (res.map(_._1).sum, res.map(_._2).sum)
      }
      val rasterRDD = converter.convert(trajRDD, preMap, agg)
      val res = rasterRDD.collect
      val matrix = res.drop(1).foldRight(res.head)((x, y) => x.merge(y, (a, b) => ((a._1 + b._1) / (a._2 + b._2), a._2 + b._2), (_, _) => None)).entries.map(_.value)

      val resFile = s"${temporal.start}.csv"
      val ColumnSeparator = ","
      val writer = new FileWriter(resFile, true)
      try {
        matrix.foreach {
          line => writer.write(line.toString)
        }
      } finally {
        writer.flush()
        writer.close()
      }

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
