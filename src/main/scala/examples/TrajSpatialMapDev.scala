package examples

import geometry.Rectangle
import operators.CustomOperatorSet
import operators.convertion.Traj2SpatialMapConverter
import operators.extraction.SpatialMapExtractor
import operators.selection.DefaultSelectorOld
import org.apache.spark.sql.SparkSession
import preprocessing.ReadTrajJson
import utils.Config
import utils.SpatialProcessing.gridPartition
import utils.TimeParsing.parseTemporalRange

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
      DefaultSelectorOld(numPartitions, sQuery, tQuery),
      //      new Traj2SpatialMapConverter(tStart, tEnd, partitionRange),
      new Traj2SpatialMapConverter(tStart, tEnd, partitionRange, Some(timeInterval)),
      new SpatialMapExtractor)

    /** read input data */
    val trajRDD = ReadTrajJson(trajectoryFile, numPartitions).map(_.reorderTemporally())
    /** step 1: Selection */
    val rdd1 = operator.selector.query(trajRDD)
    println(s"--- ${rdd1.map(_.id).distinct.count} trajectories")
    /** step 2: Conversion */
    var t = nanoTime()
    println("--- start conversion")
    t = nanoTime()
    val rdd2 = operator.converter.convert(rdd1)
    rdd2.cache()
    rdd2.take(1)
    println(s"... conversion takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    println(s"Number of partitions: ${rdd2.getNumPartitions}")
    t = nanoTime()

    /** step 3: Extraction */
    t = nanoTime()
    val total = rdd2.flatMap(x => x.contents.flatMap(_._2)).collect
    println(total.map(_.id.split("_")(0)).distinct.length)
    //    rdd2.map(_.printInfo()).foreach(println(_))
    val extractedRDD = operator.extractor.rangeQuery(rdd2, sQuery, tQuery)
    println(s"... Total ${extractedRDD.count} sub trajectories")
    val uniqueTrajs = extractedRDD.map(x => (x.id.split("_")(0), 1)).reduceByKey(_ + _).map(_._1)
    println(s"... Total ${uniqueTrajs.count} unique trajectories")
    println(s"... Extraction takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    /** benchmark */
    t = nanoTime()
    val benchmark = rdd1.filter { traj =>
      traj.strictIntersect(sQuery, tQuery)
    }.map(_.id).distinct

    //    println(benchmark.collect.filterNot(uniqueTrajs.collect().contains(_)).mkString("Array(", ", ", ")"))
    println(s"... Total ${benchmark.count} unique trajectories")
    println(s"... Extraction takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
  }
}
