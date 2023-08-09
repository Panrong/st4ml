// for figure 6
package experiments

import st4ml.instances.{Duration, Event, Extent, Point, Polygon, RTree, Raster, SpatialMap, TimeSeries, Trajectory}
import st4ml.operators.converter.{Event2RasterConverter, Event2SpatialMapConverter, Event2TimeSeriesConverter, Traj2RasterConverter, Traj2SpatialMapConverter, Traj2TimeSeriesConverter}
import st4ml.operators.selector.SelectionUtils.{E, T}
import st4ml.operators.selector.partitioner.HashPartitioner
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.SizeEstimator
import st4ml.utils.Config

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
      val converter = if (useRTree) new Event2SpatialMapConverter(ranges) else new Event2SpatialMapConverter(ranges, "none")
      val convertedRDD = converter.convert(selectedRDD)
      println(convertedRDD.count)
      println(xSplit, ySplit, useRTree)
      println(s"Conversion time: ${(nanoTime - t) * 1e-9} s")

      //            /** print converted result */
      //            val sms = convertedRDD.collect
      //            val sm = sms.drop(1).foldRight(sms.head)(_.merge(_))
      //            println(sm.entries.map(_.value.length).deep)
      //            println(s"Sum: ${sm.entries.map(_.value.length).sum}")
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
      val converter = if (useRTree) new Traj2SpatialMapConverter(ranges) else new Traj2SpatialMapConverter(ranges, "none")
      val convertedRDD = converter.convert(selectedRDD)
      println(convertedRDD.count)
      println(xSplit, ySplit, useRTree)
      println(s"Conversion time: ${(nanoTime - t) * 1e-9} s")
      //            /** print converted result */
      //            val sms = convertedRDD.collect
      //            val sm = sms.drop(1).foldRight(sms.head)(_.merge(_))
      //            println(sm.entries.map(_.value.length).deep)
      //            println(s"Sum: ${sm.entries.map(_.value.length).sum}")
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
      val converter = if (useRTree) new Event2RasterConverter(stRanges.map(_._1), stRanges.map(_._2))
      else new Event2RasterConverter(stRanges.map(_._1), stRanges.map(_._2), "none")
      val convertedRDD = converter.convert(selectedRDD)
      println(convertedRDD.count)
      println(xSplit, ySplit, tSplit, useRTree)
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
      val converter = if (useRTree) new Traj2RasterConverter(stRanges.map(_._1), stRanges.map(_._2))
      else new Traj2RasterConverter(stRanges.map(_._1), stRanges.map(_._2), "none")
      val convertedRDD = converter.convert(selectedRDD)
      println(convertedRDD.count)
      println(xSplit, ySplit, tSplit, useRTree)
      println(s"Conversion time: ${(nanoTime - t) * 1e-9} s")
      //      /** print converted result */
      //      val sms = convertedRDD.collect
      //      println(sms.head.entries.length)
      //      val sm = sms.drop(1).foldRight(sms.head)(_.merge(_))
      //      println(sm.entries.map(_.value.length).deep)
      //      println(s"Sum: ${sm.entries.map(_.value.length).sum}")
    }

    else if (m == "5") {
      val inputRDD = spark.read.parquet(fileName).drop("pId").as[E]
        .toRdd.map(_.asInstanceOf[Event[Point, None.type, String]])
      val partitioner = new HashPartitioner(numPartitions)
      val selectedRDD = partitioner.partition(inputRDD)
      println(selectedRDD.count)
      val t = nanoTime()
      val tRanges = splitTemporal(tRange, tSplit)
      val converter = if (useRTree) new Event2TimeSeriesConverter(tRanges) else new Event2TimeSeriesConverter(tRanges, "none")
      val convertedRDD = converter.convert(selectedRDD)
      println(convertedRDD.count)
      println(tSplit, useRTree)
      println(s"Conversion time: ${(nanoTime - t) * 1e-9} s")
      //            /** print converted result */
      //            val sms = convertedRDD.collect
      //            println(sms.head.entries.length)
      //            val sm = sms.drop(1).foldRight(sms.head)(_.merge(_))
      //            println(sm.entries.map(_.value.length).deep)
      //            println(s"Sum: ${sm.entries.map(_.value.length).sum}")
    }
    else if (m == "6") {
      val inputRDD = spark.read.parquet(fileName).drop("pId").as[T]
        .toRdd.map(_.asInstanceOf[Trajectory[None.type, String]])
      val partitioner = new HashPartitioner(numPartitions)
      val selectedRDD = partitioner.partition(inputRDD)
      println(selectedRDD.count)
      val t = nanoTime()
      val tRanges = splitTemporal(tRange, tSplit)
      val converter = if (useRTree) new Traj2TimeSeriesConverter(tRanges) else new Traj2TimeSeriesConverter(tRanges, "none")
      val convertedRDD = converter.convert(selectedRDD)
      println(convertedRDD.count)
      println(tSplit, useRTree)
      println(s"Conversion time: ${(nanoTime - t) * 1e-9} s")
      //      /** print converted result */
      //      val sms = convertedRDD.collect
      //      println(sms.head.entries.length)
      //      val sm = sms.drop(1).foldRight(sms.head)(_.merge(_))
      //      println(sm.entries.map(_.value.length).deep)
      //      println(s"Sum: ${sm.entries.map(_.value.length).sum}")
    }
    else if (m == "7") {
      //      val timeSeries = TimeSeries.empty[Int](Array(Duration(0, 10), Duration(10, 20), Duration(20, 30)))
      //      val f: Array[Int] => Int = _=>5
      //      val f2: (Array[Int], Polygon, Duration) => Long = (_,_, t) => t.start
      //      val ts1 = timeSeries.mapValue(f)
      //      val ts2 = timeSeries.mapValuePlus(f2)
      //      println(ts1)
      //      println(ts2)
      //
      //      val raster = Raster.empty(Array(Extent(0,0,1,1).toPolygon),Array(Duration(0, 10), Duration(10, 20), Duration(20, 30)))
      //      val f3:(Array[Nothing], Polygon, Duration) => Long = (_,_, t) => t.start
      //      val raster2 = raster.mapValuePlus(f3)
      //      println(raster2)
      //
      //      val m = 33483200
      //      val n = 30*30
      //      println(m * math.log(m) + n * math.log(m),n * math.log(n) + m * math.log(n), m*n)
      val df = Range(0, 99).toDF
      val partitionedDf = df.withColumn("idx", $"value" % 10).repartition(10)
      println(partitionedDf.rdd.mapPartitions(x => Iterator(x.size)).collect.deep)
      val res = partitionedDf.rdd.mapPartitionsWithIndex { case (idx, iter) =>
        iter.map(x => (idx, x))
      }.collect
      println(res.deep)
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