package examples

import geometry.Rectangle
import operators.CustomOperatorSet
import operators.convertion.{LegacyConverter, Point2SpatialMapConverter, Traj2PointConverter}
import operators.extraction.SpatialMapExtractor
import operators.selection.DefaultSelector
import org.apache.spark.sql.SparkSession
import preprocessing.ReadTrajJson
import utils.Config
import utils.TimeParsing.parseTemporalRange
import utils.SpatialProcessing.gridPartition

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
    val sQuery = Rectangle(args(0).split(",").map(_.toDouble))
    val tQuery = parseTemporalRange(args(1))
    val tStart = tQuery._1
    val tEnd = tQuery._2
    val partitionRange = gridPartition(sQuery.coordinates, args(2).toInt)
    val timeInterval = args(3).toInt
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /** initialize operators */
    val operator = new CustomOperatorSet(
      DefaultSelector(numPartitions),
      new LegacyConverter,
      new SpatialMapExtractor)

    /** read input data */
    val trajRDD = ReadTrajJson(trajectoryFile, numPartitions)

    /** step 1: Selection */
    val rdd1 = operator.selector.query(trajRDD, sQuery, tQuery)
    println(s"--- ${rdd1.count} trajectories")

    /** step 2: Conversion */
    val converter = new Traj2PointConverter()
    val pointRDD = converter.convert(rdd1).map((0, _))
      .filter {
        case (_, p) => p.inside(sQuery) && p.timeStamp._1 >= tStart && p.timeStamp._2 <= tEnd
      }
    println(s"    <- debug: num of points before conversion: ${pointRDD.count}")

    var t = nanoTime()
    println(partitionRange)
    val spatialMapRDD = new Point2SpatialMapConverter(tStart, tEnd, partitionRange, Some(timeInterval)).convert(pointRDD).cache()
    println(s"    <- debug: num of points after conversion: ${spatialMapRDD.flatMap(_.contents).flatMap(x => x._2).count}")
    spatialMapRDD.take(1)
    println(s"... Conversion takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    /** step 3.1: Extraction.GenHeatMap */
    t = nanoTime()
    //    spatialMapRDD.map(_.printInfo()).foreach(println(_))
    val countPerRegion = operator.extractor.countPerRegion(spatialMapRDD, tQuery).collect
    //    println(countPerRegion.map(_._2).sum)
    countPerRegion.foreach(println(_))
    println(s"... Getting aggregation info takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    val extractedRDD = operator.extractor.rangeQuery(spatialMapRDD, sQuery, tQuery)

    /** step 3.2: Extraction.RangeQuery */
    t = nanoTime()
    println(s"... Total ${extractedRDD.count} points")
    println(s"... Extraction takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    t = nanoTime()
    val gt = pointRDD.filter(p => p._2.inside(sQuery) && p._2.t <= tQuery._2 && p._2.t >= tQuery._1).count
    println(s"... (Benchmark) Total $gt points")
    println(s"... Benchmark takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
  }


}
