package experiments

import instances.{Duration, Event, Extent, Polygon, Point}
import operators.selection.indexer.RTree
import operatorsNew.converter.Event2SpatialMapConverter
import operatorsNew.selector.SelectionUtils.{E, T}
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime

object ConversionExp {
  def main(args: Array[String]): Unit = {
//    val entries = Array(
//      (geometry.Point(Array(0,0)), "a", 1),
//      (geometry.Point(Array(1,0)), "b", 2),
//      (geometry.Point(Array(0,1)), "c", 3),
//      (geometry.Point(Array(2,2)), "d", 4),
//    )
//    val rtree = RTree(entries, 2)
//    val selected = rtree.range(geometry.Rectangle(Array(1,1, 2,2)))
//    println(selected.deep)
    val spark = SparkSession.builder()
      .appName("LoadingTest")
      .master(Config.get("master"))
      .getOrCreate()
    import spark.implicits._

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val fileName = args(0)
    val m = args(1)

    /**
     * 1 for event to spatial map
     * 2 for traj to spatial map
     * 3 for event to raster
     * 4 for traj to raster
     * 5 for event to time series
     * 6 for traj to time series
     */

    val sRange = args(2).split(",").map(_.toDouble)
    val tRange = args(3).split(",").map(_.toLong)
    val xSplit = args(4).toInt
    val ySplit = args(5).toInt
    val useRTree = args(6).toBoolean

    if (m == "1") {
      val inputRDD = spark.read.parquet(fileName).drop("pId").as[E]
        .toRdd.map(_.asInstanceOf[Event[Point, None.type, String]])
      println(inputRDD.count)
      val t = nanoTime()
      val ranges = splitSpatial(sRange, xSplit, ySplit)
      val converter = new Event2SpatialMapConverter[Point,
        None.type, String, Array[Event[Point, None.type, String]], None.type](x => x, ranges)
      val convertedRDD = if(useRTree) converter.convertWithRTree(inputRDD)
      else converter.convert(inputRDD)
      println(convertedRDD.count)
      println(s" conversion time: ${(nanoTime - t) * 1e-9} s")

      /** print converted result */
      val sms = convertedRDD.collect
      val sm = sms.drop(1).foldRight(sms.head)(_.merge(_))
      println(sm.entries.map(_.value.length).deep)
    }

    sc.stop
  }

  def splitSpatial(spatialRange: Array[Double], xSplit: Int, ySplit: Int): Array[Polygon] = {
    val xMin = spatialRange(0)
    val yMin = spatialRange(1)
    val xMax = spatialRange(2)
    val yMax = spatialRange(3)
    val xStep = (xMax - xMin) / xSplit
    val xs = (0 to xSplit).map(x => x * xStep + xMin).sliding(2)
    val yStep = (yMax - yMin) / ySplit
    val ys = (0 to ySplit).map(y => y * yStep + yMin).sliding(2)

    xs.flatMap(x =>
      ys.map(y => (x, y))).map(r => {
      Extent(r._1(0), r._2(0), r._1(1), r._2(1)).toPolygon
    })
      .toArray
  }
}
