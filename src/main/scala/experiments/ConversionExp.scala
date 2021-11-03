package experiments

import instances.{Duration, Event, Extent, Point, Polygon, Trajectory}
import operators.selection.indexer.RTree
import operatorsNew.converter.{Event2SpatialMapConverter, Traj2SpatialMapConverter}
import operatorsNew.selector.SelectionUtils.{E, T}
import operatorsNew.selector.partitioner.HashPartitioner
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
    val numPartitions = args(1).toInt
    val m = args(2)

    /**
     * 1 for event to spatial map
     * 2 for traj to spatial map
     * 3 for event to raster
     * 4 for traj to raster
     * 5 for event to time series
     * 6 for traj to time series
     */

    val sRange = args(3).split(",").map(_.toDouble)
    val tRange = args(4).split(",").map(_.toLong)
    val xSplit = args(5).toInt
    val ySplit = args(6).toInt
    val useRTree = args(7).toBoolean

    if (m == "1") {
      val inputRDD = spark.read.parquet(fileName).drop("pId").as[E]
        .toRdd.map(_.asInstanceOf[Event[Point, None.type, String]])
      val partitioner = new HashPartitioner(numPartitions)
      val selectedRDD = partitioner.partition(inputRDD)
      println(selectedRDD.count)
      val t = nanoTime()
      val ranges = splitSpatial(sRange, xSplit, ySplit)
      val converter = new Event2SpatialMapConverter[Point,
        None.type, String, Array[Event[Point, None.type, String]], None.type](x => x, ranges)
      val convertedRDD = if (useRTree) converter.convertWithRTree(selectedRDD)
      else converter.convert(inputRDD)
      println(convertedRDD.count)
      println(s"Conversion time: ${(nanoTime - t) * 1e-9} s")

      //      /** print converted result */
      //      val sms = convertedRDD.collect
      //      val sm = sms.drop(1).foldRight(sms.head)(_.merge(_))
      //      println(sm.entries.map(_.value.length).deep)
      //      println(s"Sum: ${sm.entries.map(_.value.length).sum}")
    }

    else if (m == "2") {
      type TRAJ = Trajectory[None.type, String]
      val inputRDD = spark.read.parquet(fileName).drop("pId").as[T]
        .toRdd.map(_.asInstanceOf[TRAJ])
      val partitioner = new HashPartitioner(numPartitions)
      val selectedRDD = partitioner.partition(inputRDD)
      println(selectedRDD.count)
      val t = nanoTime()
      val ranges = splitSpatial(sRange, xSplit, ySplit)
      val converter = new Traj2SpatialMapConverter[None.type, String, Array[TRAJ], None.type](x => x, ranges)
      val convertedRDD = if (useRTree) converter.convertWithRTree(selectedRDD)
      else converter.convert(inputRDD)
      println(convertedRDD.count)
      println(s"Conversion time: ${(nanoTime - t) * 1e-9} s")
      //      /** print converted result */
      //      val sms = convertedRDD.collect
      //      val sm = sms.drop(1).foldRight(sms.head)(_.merge(_))
      //      println(sm.entries.map(_.value.length).deep)
      //      println(s"Sum: ${sm.entries.map(_.value.length).sum}")
    }

    sc.stop
  }

  def splitSpatial(spatialRange: Array[Double], xSplit: Int, ySplit: Int): Array[Polygon] = {
    val xMin = spatialRange(0)
    val yMin = spatialRange(1)
    val xMax = spatialRange(2)
    val yMax = spatialRange(3)
    val xStep = (xMax - xMin) / xSplit
    val xs = (0 to xSplit).map(x => x * xStep + xMin).sliding(2).toArray
    val yStep = (yMax - yMin) / ySplit
    val ys = (0 to ySplit).map(y => y * yStep + yMin).sliding(2).toArray
    for (x <- xs; y <- ys) yield Extent(x(0), y(0), x(1), y(1)).toPolygon
  }
}
