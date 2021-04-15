package examples

import geometry.Rectangle
import operators.CustomOperatorSet
import operators.convertion.Converter
import operators.extraction.SpatialMapExtractor
import operators.selection.DefaultSelector
import org.apache.spark.sql.SparkSession
import preprocessing.ReadTrajJson
import utils.Config
import utils.TimeParsing.parseTemporalRange
import utils.SpatialProcessing.gridPartition
import java.lang.System.nanoTime

object TrajSpatialMapDev {
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
      new Converter,
      new SpatialMapExtractor)

    /** read input data */
    val trajRDD = ReadTrajJson(trajectoryFile, numPartitions)

    /** step 1: Selection */
    val rdd1 = operator.selector.query(trajRDD, sQuery, tQuery)
    println(s"--- ${rdd1.count} trajectories")

    /** step 2: Conversion */
    val converter = operator.converter
    var t = nanoTime()
    println("--- start conversion")
    t = nanoTime()
    val rdd2 = converter.traj2SpatialMap(rdd1, tStart, tEnd, partitionRange, Some(timeInterval))
    rdd2.cache()
    rdd2.take(1)
    println(s"... conversion takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    println(s"Number of partitions: ${rdd2.getNumPartitions}")
    t = nanoTime()

    /** step 3: Extraction */
    t = nanoTime()
    //    rdd2.map(_.printInfo()).foreach(println(_))
    val extractedRDD = operator.extractor.rangeQuery(rdd2, sQuery, tQuery)
    println(s"... Total ${extractedRDD.count} points")
    println(s"... Extraction takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
  }
}
