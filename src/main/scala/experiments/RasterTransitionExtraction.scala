package experiments

import instances.{Duration, Entry, Extent, Polygon, Raster, Trajectory}
import operatorsNew.converter.{Traj2RasterConverter, Traj2SpatialMapConverter}
import operatorsNew.selector.Selector
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.Geometry
import utils.Config

import java.lang.System.nanoTime
import scala.io.Source

object RasterTransitionExtraction {
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val metadata = args(1)
    val queryFile = args(2)
    val gridSize = args(3).toDouble
    val tStep = args(4).toInt
    val numPartitions = args(5).toInt
    val spark = SparkSession.builder()
      .appName("RasterTransitionExtraction")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    // read queries
    val f = Source.fromFile(queryFile)
    val ranges = f.getLines().toArray.map(line => {
      val r = line.split(" ")
      (Extent(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble).toPolygon, Duration(r(4).toLong, r(5).toLong))
    })
    val t = nanoTime()
    type TRAJ = Trajectory[Option[String], String]

    def findInOut[T <: Trajectory[_, _]](arr: Array[T])(implicit sRange: Polygon, tRange: Duration): (Int, Int) = {
      val inOuts = arr.map { traj =>
        val inside = traj.entries.map(p => p.intersects(sRange, tRange)).sliding(2).toArray
        val in = inside.count(_ sameElements Array(false, true))
        val out = inside.count(_ sameElements Array(true, false))
        (in, out)
      }
      (inOuts.map(_._1).sum, inOuts.map(_._2).sum)
    }

    for ((spatial, temporal) <- ranges) {
      val selector = Selector[TRAJ](spatial, temporal, numPartitions)
      val trajRDD = selector.selectTraj(fileName, metadata, false)
      val sRanges = splitSpatial(spatial, gridSize)
      val tRanges = splitTemporal(Array(temporal.start, temporal.end), tStep)
      val stRanges = for (s <- sRanges; t <- tRanges) yield (s, t)
      val converter = new Traj2RasterConverter[Option[String],
        String, Array[TRAJ], None.type](
        x => x, stRanges.map(_._1), stRanges.map(_._2))
      val rasterRDD = converter.convertWithRTree(trajRDD).map(raster => Raster(
        raster.entries.map(entry => {
          implicit val s: Polygon = entry.spatial
          implicit val t: Duration = entry.temporal
          Entry(
            entry.spatial,
            entry.temporal,
            findInOut(entry.value))
        }),
        raster.data))
      val res = rasterRDD.collect

      def valueMerge(x: (Int, Int), y: (Int, Int)): (Int, Int) = (x._1 + y._1, x._2 + y._2)

      val mergedRaster = res.drop(1).foldRight(res.head)(_.merge(_, valueMerge, (_, _) => None))
      rasterRDD.unpersist()
      mergedRaster.entries.take(5).foreach(println)

    }
    println(s"Raster transition extraction ${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }

  def splitSpatial(spatialRange: Polygon, gridSize: Double): Array[Polygon] = {
    val xMin = spatialRange.getCoordinates.map(_.x).min
    val xMax = spatialRange.getCoordinates.map(_.x).max
    val yMin = spatialRange.getCoordinates.map(_.y).min
    val yMax = spatialRange.getCoordinates.map(_.y).max
    val xSplit = ((xMax - xMin) / gridSize).toInt
    val xs = (0 to xSplit).map(x => x * gridSize + xMin).sliding(2).toArray
    val ySplit = ((yMax - yMin) / gridSize).toInt
    val ys = (0 to ySplit).map(y => y * gridSize + yMin).sliding(2).toArray
    for (x <- xs; y <- ys) yield Extent(x(0), y(0), x(1), y(1)).toPolygon
  }

  def splitTemporal(temporalRange: Array[Long], tStep: Int): Array[Duration] = {
    val tMin = temporalRange(0)
    val tMax = temporalRange(1)
    val tSplit = ((tMax - tMin) / tStep).toInt
    val ts = (0 to tSplit).map(x => x * tStep + tMin).sliding(2).toArray
    for (t <- ts) yield Duration(t(0), t(1))
  }
}
