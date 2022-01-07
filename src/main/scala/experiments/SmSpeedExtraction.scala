package experiments

import instances.{Duration, Extent, Polygon, Trajectory}
import operatorsNew.selector.Selector
import operatorsNew.converter.Traj2SpatialMapConverter
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime
import scala.io.Source

object SmSpeedExtraction {
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val metadata = args(1)
    val queryFile = args(2)
    val gridSize = args(3).toDouble
    val numPartitions = args(4).toInt
    val spark = SparkSession.builder()
      .appName("trajRangeQuery")
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

    for ((spatial, temporal) <- ranges) {
      val selector = Selector[TRAJ](spatial, temporal, numPartitions)
      val trajRDD = selector.selectTraj(fileName, metadata, false)
      val sRanges = splitSpatial(spatial, gridSize)
      val preMap: TRAJ => Trajectory[Option[String], Double] = x => {
        val speed = x.consecutiveSpatialDistance("greatCircle").sum / x.duration.seconds * 3.6
        Trajectory(x.entries, speed)
      }
      val agg: Array[Trajectory[Option[String], Double]] => (Double, Int) = x => {
        val res = x.map(t => (t.data, 1))
        (res.map(_._1).sum, res.map(_._2).sum)
      }
      val converter = new Traj2SpatialMapConverter(sRanges)
      val smRDD = converter.convert(trajRDD, preMap, agg)
      val res = smRDD.collect

      def valueMerge(x: (Double, Int), y: (Double, Int)): (Double, Int) = (x._1 + y._1, x._2 + y._2)

      val mergedSm = res.drop(1).foldRight(res.head)((x, y) => x.merge(y, valueMerge, (_, _) => None))
      smRDD.unpersist()
      println(mergedSm.entries.map(x => (x.value._1 / x.value._2)).deep)

    }
    println(s"Stay point extraction ${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }

  def splitSpatial(spatialRange: Polygon, gridSize: Double): Array[Polygon] = {
    //    val xMin = spatialRange(0)
    //    val yMin = spatialRange(1)
    //    val xMax = spatialRange(2)
    //    val yMax = spatialRange(3)
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
}