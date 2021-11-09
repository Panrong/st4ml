package experiments

import instances.{Duration, Event, Extent, Point, Polygon, Trajectory}
import instances.RTree
import operatorsNew.converter.{Event2RasterConverter, Event2SpatialMapConverter, Event2TimeSeriesConverter, Traj2RasterConverter, Traj2SpatialMapConverter}
import operatorsNew.selector.SelectionUtils.{E, T}
import operatorsNew.selector.partitioner.HashPartitioner
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime

object ConversionExp {
  def main(args: Array[String]): Unit = {

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
     * 7 for rtree test
     */

    val sRange = args(3).split(",").map(_.toDouble)
    val tRange = args(4).split(",").map(_.toLong)
    val xSplit = args(5).toInt
    val ySplit = args(6).toInt
    val tSplit = args(7).toInt
    val useRTree = args(8).toBoolean

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
      else converter.convert(selectedRDD)
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
      else converter.convertFast(selectedRDD)
      println(convertedRDD.count)
      println(s"Conversion time: ${(nanoTime - t) * 1e-9} s")
      //      /** print converted result */
      //      val sms = convertedRDD.collect
      //      val sm = sms.drop(1).foldRight(sms.head)(_.merge(_))
      //      println(sm.entries.map(_.value.length).deep)
      //      println(s"Sum: ${sm.entries.map(_.value.length).sum}")
    }
    else if (m == "3") {
      val inputRDD = spark.read.parquet(fileName).drop("pId").as[E]
        .toRdd.map(_.asInstanceOf[Event[Point, None.type, String]])
      val partitioner = new HashPartitioner(numPartitions)
      val selectedRDD = partitioner.partition(inputRDD)
      println(selectedRDD.count)
      val t = nanoTime()
      val sRanges = splitSpatial(sRange, xSplit, ySplit)
      val tRanges = splitTemporal(tRange, tSplit)
      val stRanges = for (s <- sRanges; t <- tRanges) yield (s, t)
      val converter = new Event2RasterConverter[Point, None.type,
        String, Array[Event[Point, None.type, String]], None.type](
        x => x, stRanges.map(_._1), stRanges.map(_._2))
      val convertedRDD = if (useRTree) converter.convertWithRTree(selectedRDD)
      else converter.convert(selectedRDD)
      println(convertedRDD.count)
      println(s"Conversion time: ${(nanoTime - t) * 1e-9} s")
      //      /** print converted result */
      //      val sms = convertedRDD.map(x => x.mapValue(x => Array(x.length))).collect
      //      println(sms.head.entries.length)
      //      val sm = sms.drop(1).foldRight(sms.head)(_.merge(_))
      //      println(sm.entries.map(_.value.sum).deep)
      //      println(s"Sum: ${sm.entries.map(_.value.length).sum}")
    }
    else if (m == "4") {
      val inputRDD = spark.read.parquet(fileName).drop("pId").as[T]
        .toRdd.map(_.asInstanceOf[Trajectory[None.type, String]])
      val partitioner = new HashPartitioner(numPartitions)
      val selectedRDD = partitioner.partition(inputRDD)
      println(selectedRDD.count)
      val t = nanoTime()
      val sRanges = splitSpatial(sRange, xSplit, ySplit)
      val tRanges = splitTemporal(tRange, tSplit)
      val stRanges = for (s <- sRanges; t <- tRanges) yield (s, t)
      val converter = new Traj2RasterConverter[None.type,
        String, Array[Trajectory[None.type, String]], None.type](
        x => x, stRanges.map(_._1), stRanges.map(_._2))
      val convertedRDD = if (useRTree) converter.convertWithRTree(selectedRDD)
      else converter.convert(selectedRDD)
      println(convertedRDD.count)
      println(s"Conversion time: ${(nanoTime - t) * 1e-9} s")
      //      /** print converted result */
      //      val sms = convertedRDD.collect
      //      println(sms.head.entries.length)
      //      val sm = sms.drop(1).foldRight(sms.head)(_.merge(_))
      //      println(sm.entries.map(_.value.length).deep)
      //      println(s"Sum: ${sm.entries.map(_.value.length).sum}")
    }
    // fake one
    else if (m == "5") {
      val inputRDD = spark.read.parquet(fileName).drop("pId").as[E]
        .toRdd.map(_.asInstanceOf[Event[Point, None.type, String]])
      val partitioner = new HashPartitioner(numPartitions)
      val selectedRDD = partitioner.partition(inputRDD)
      println(selectedRDD.count)
      val t = nanoTime()
      val tRanges = splitTemporal(tRange, tSplit)
      val converter = new Event2TimeSeriesConverter[Point, None.type,
        String, Array[Event[Point, None.type, String]], None.type](
        x => x, tRanges)
      val convertedRDD = if (useRTree) converter.convertWithRTree(selectedRDD)
      else converter.convert(selectedRDD)
      println(convertedRDD.count)
      println(s"Conversion time: ${(nanoTime - t) * 1e-9} s")
      /** print converted result */
      val sms = convertedRDD.collect
      println(sms.head.entries.length)
      val sm = sms.drop(1).foldRight(sms.head)(_.merge(_))
      println(sm.entries.map(_.value.length).deep)
      println(s"Sum: ${sm.entries.map(_.value.length).sum}")
    }
    else if (m == "7") {
      //          val entries = Array(
      //            (geometry.Cube(Array(0,0,1,1,0,0)), "a", 1),
      //            (geometry.Cube(Array(1,0,1,1,0,0)), "b", 2),
      //            (geometry.Cube(Array(0,1,1,1,0,0)), "c", 3),
      //            (geometry.Cube(Array(2,2,2,2,0,0)), "d", 4),
      //          )
      //          val rtree = RTree(entries, 2)
      //          val selected = rtree.range(geometry.Rectangle(Array(1,1, 2,2)))
      //          println(selected.deep)
      val a = Extent(0, 0, 1, 1).toPolygon
      a.setUserData(Array(1.0, 1.0))
      val b = Extent(1, 1, 2, 2).toPolygon
      b.setUserData(Array(2.0, 2.0))
      val c = Extent(2, 0, 3, 1).toPolygon
      c.setUserData(Array(1.0, 1.0))
      val d = Extent(2, 1, 3, 2).toPolygon
      d.setUserData(Array(3.0, 3.0))
      val entries = Array(
        (a, "a", 1),
        (b, "b", 2),
        (c, "c", 3),
        (d, "d", 4))
      val rtree = RTree(entries, 2, dimension = 3)
      val selected = rtree.range(Extent(1.5, 1.5, 2.5, 2.5).toPolygon)
      val cube = Event(Extent(1.5, 1.5, 2.5, 2.5).toPolygon, Duration(1, 2))
      val selected2 = rtree.range3d(cube)
      println(selected.deep)
      println(selected2.deep)
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

  def splitTemporal(temporalRange: Array[Long], tSplit: Int): Array[Duration] = {
    val tMin = temporalRange(0)
    val tMax = temporalRange(1)
    val tStep = (tMax - tMin) / tSplit
    val ts = (0 to tSplit).map(x => x * tStep + tMin).sliding(2).toArray
    for (t <- ts) yield Duration(t(0), t(1))
  }
}
