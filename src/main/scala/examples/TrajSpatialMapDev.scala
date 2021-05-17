package examples

import geometry.{Rectangle, SpatialMap, Trajectory}
import operators.OperatorSet
import operators.convertion.Traj2SpatialMapConverter
import operators.extraction.{SpatialMapExtractor, TrajSpatialMapExtractor}
import operators.selection.DefaultSelector
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
    val partitionRange = gridPartition(sQuery.coordinates, args(2).toInt).take(5)
    val timeInterval = args(3).toInt


    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /** initialize operators */
    val operator = new OperatorSet {
      override type I = Trajectory
      override type O = SpatialMap[Trajectory]
      override val selector = new DefaultSelector[I](sQuery, tQuery)
      override val converter = new Traj2SpatialMapConverter(tStart, tEnd, partitionRange, Some(timeInterval))
      override val extractor = new SpatialMapExtractor[O]
    }


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

    //    /** step 3: Extraction */
    //    t = nanoTime()
    //    val total = rdd2.flatMap(x => x.contents.flatMap(_._2)).collect
    //    println(total.map(_.id.split("_")(0)).distinct.length)
    //    //    rdd2.map(_.printInfo()).foreach(println(_))
    //    val extractedRDD = operator.extractor.rangeQuery(rdd2, sQuery, tQuery)
    //    println(s"... Total ${extractedRDD.count} sub trajectories")
    //    val uniqueTrajs = extractedRDD.map(x => (x.id.split("_")(0), 1)).reduceByKey(_ + _).map(_._1)
    //    println(s"... Total ${uniqueTrajs.count} unique trajectories")
    //    println(s"... Extraction takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    //
    //    /** benchmark */
    //    t = nanoTime()
    //    val benchmark = rdd1.filter { traj =>
    //      traj.strictIntersect(sQuery, tQuery)
    //    }.map(_.id).distinct
    //
    //    //    println(benchmark.collect.filterNot(uniqueTrajs.collect().contains(_)).mkString("Array(", ", ", ")"))
    //    println(s"... Total ${benchmark.count} unique trajectories")
    //    println(s"... Extraction takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.\n")
    //
    //
    //    /** speed average per raster extractor */
    //    t = nanoTime()
    //
    //    val avgSpeedInfo = speedExtractor.extractAvgSpeed(rdd2)
    //    avgSpeedInfo.take(5).foreach {
    //      case (t, map) =>
    //        println(s"time slot: $t")
    //        map.foreach(x => println(s"${x._1}: ${(x._2 / 3.6).formatted("%.2f")} km/h "))
    //    }
    //    println(s"... Extraction speed takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")


    /** speed extractor */
    val speedExtractor = new TrajSpatialMapExtractor[SpatialMap[Trajectory]]
    t = nanoTime()
    val speedInfo = speedExtractor.extractRangeAvgSpeed(rdd2)
    speedInfo.take(5).foreach(x => println(s"${x._1}: ${(x._2 / 3.6).formatted("%.2f")} km/h "))
    println(s"... Avg speed extraction takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.\n")


    /** speed extractor benchmark */
    val bc = rdd1.flatMap {
      traj => partitionRange.map(x => (x._1, traj.calAvgSpeed(x._2)))
    }
      .mapValues(value => Array(value))
      .reduceByKey(_ ++ _)
      .mapValues(x => x.sum / x.length)
      .map(x => (partitionRange(x._1), x._2))

    bc.take(5).foreach(x => println(s"${x._1}: ${(x._2 / 3.6).formatted("%.2f")} km/h "))
    println(s"... Speed benchmark takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.\n")

    sc.stop()
  }

}
