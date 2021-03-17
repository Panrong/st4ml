package examples

import operators.convertion.Converter
import operators.extraction.SMExtractor
import geometry.Rectangle
import geometry.road.RoadGrid
import org.apache.spark.sql.SparkSession
import preprocessing.ReadMMTrajFile
import operators.selection.partitioner.{HashPartitioner, STRPartitioner}
import operators.selection.selectionHandler.{FilterHandler, RTreeHandler, TemporalSelector}

import java.lang.System.nanoTime
import scala.io.Source
import utils.Config

object SpatialMapDev {
  def main(args: Array[String]) {
    var t = nanoTime()
    /** set up Spark environment */
    val spark = SparkSession
      .builder()
      .master(Config.get("master"))
      .appName("SpatialMapDev")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val numPartitions = Config.get("numPartitions").toInt
    val trajectoryFile = args(0)
    val mapFile = args(1)

    /**
     * example input arguments: datasets/mm10000.csv preprocessing/porto.csv
     */

    /** **********************************
     * test map-matched trajectory dataset
     * ********************************** */

    val trajRDD = ReadMMTrajFile(trajectoryFile, mapFile)
      .repartition(numPartitions)
      .cache()
    val sQuery = Rectangle(Array(-8.682329739182336, 41.16930767535641, -8.553892156181982, 41.17336956864337))
    val tQuery = (1372630000L, 1372660000L)

    println(s"\nOriginal trajectory dataset contains ${trajRDD.count} entries")
    println("\n*-*-*-*-*-*-*-*-*-*-*-*")

    /** benchmark */
    t = nanoTime()
    val fullSRDD = trajRDD.filter(x => x.intersect(sQuery))
    println(s"*- Full scan S: ${fullSRDD.count} -*")
    val fullSTRDD = fullSRDD.filter(x => {
      val (ts, te) = x.timeStamp
      x.intersect(sQuery) && (ts <= tQuery._2 && ts >= tQuery._1 || te <= tQuery._2 && te >= tQuery._1)
    })
    println(s"*- Full scan ST: ${fullSTRDD.count} -*")
    println("*-*-*-*-*-*-*-*-*-*-*-*")
    println(s"... Full scanning takes ${(nanoTime() - t) * 1e-9} s.\n")

    /** partition */
    println("==== STR ====")
    t = nanoTime()
    val partitioner = new STRPartitioner(numPartitions)
    val pRDD = partitioner.partition(trajRDD)
    val partitionRange = partitioner.partitionRange
    val filterSelector = new FilterHandler(partitionRange)
    val rtreeSelector = new RTreeHandler(partitionRange)

    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")

    /** spatial query by filtering */
    t = nanoTime()
    val queriedRDD1 = filterSelector.query(pRDD)(sQuery) //.map(x => (x._2.id, x)).groupByKey().flatMap(x => x._2.take(1))
    println(s"==== Queried dataset contains ${queriedRDD1.count} entries (filtering)")
    println(s"... Querying by filtering takes ${(nanoTime() - t) * 1e-9} s.")

    /** spatial query with index */
    t = nanoTime()
    val queriedRDD2 = rtreeSelector.query(pRDD)(sQuery) //.map(x => (x._2.id, x)).groupByKey().flatMap(x => x._2.take(1))


    println(s"==== Queried dataset contains ${queriedRDD2.count} entries (RTree)")
    println(s"... Querying with index takes ${(nanoTime() - t) * 1e-9} s.")

    /** temporal query by filtering */
    t = nanoTime()
    val temporalSelector = new TemporalSelector
    val queriedRDD3 = temporalSelector.query(queriedRDD2)(tQuery)
    //    queriedRDD3.take(6).foreach(println(_))
    println(s"==== Queried dataset contains ${queriedRDD3.count} entries (ST)")
    println(s"... Temporal querying takes ${(nanoTime() - t) * 1e-9} s.")

    /** test hash partitioner */
    println("\n==== HASH ====")

    t = nanoTime()
    val hashPartitioner = new HashPartitioner(numPartitions)
    val pRDDHash = hashPartitioner.partition(trajRDD)
    val partitionRangeHash = hashPartitioner.partitionRange
    val selectorHash = new FilterHandler(partitionRangeHash)
    val rtreeselectorHash = new FilterHandler(partitionRangeHash)

    println(s"... Partitioning takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val queriedRDD1Hash = selectorHash.query(pRDDHash)(sQuery)
    println(s"==== Queried dataset contains ${queriedRDD1Hash.count} entries (filtering)")
    println(s"... Querying by filtering takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val queriedRDD2Hash = rtreeselectorHash.query(pRDDHash)(sQuery)
    println(s"==== Queried dataset contains ${queriedRDD2Hash.count} entries (RTree)")
    println(s"... Querying with index takes ${(nanoTime() - t) * 1e-9} s.")

    t = nanoTime()
    val temporalSelectorH = new TemporalSelector
    val queriedRDD3Hash = temporalSelectorH.query(queriedRDD2Hash)(tQuery)
    println(s"==== Queried dataset contains ${queriedRDD3Hash.count} entries (ST)")
    println(s"... Temporal querying takes ${(nanoTime() - t) * 1e-9} s.")

    /** add speed info */
    val roadGrid = RoadGrid(mapFile)
    val speedRDD = queriedRDD3.map(x => (x._1, x._2.getRoadSpeed(roadGrid)))

    /** test conversion */
    val converter = new Converter
    val convertedRDD = converter.trajSpeed2SpatialMap(speedRDD)

    /** test extraction */
    val extractor = new SMExtractor
    val avgSpeed = extractor.extractRoadSpeed(convertedRDD)
    println("\n=== Average Speed === :")
    for (i <- avgSpeed.take(5)) {
      println(s"Road ID: ${i._1} --- Average speed ${i._2.formatted("%.3f")} km/h")
    }

    val totalFlow = extractor.extractRoadFlow(convertedRDD)
    println("\n===  Flow === :")
    for (i <- totalFlow.take(5)) {
      println(s"Road ID: ${i._1} --- Total Flow ${i._2}")
    }

    val startTime = tQuery._1
    val endTime = tQuery._2
    val windowSize = 15 * 60
    val slidingFlow = extractor.extractSlidingFlow(convertedRDD, startTime, endTime, windowSize)
    println(s"\n===  Sliding flow started at $startTime with window size $windowSize s === :")
    for (i <- slidingFlow.take(5)) {
      println(s"Road ID: ${i._1} --- Sliding Flow ${i._2}")
    }

    println(s"\n===  Sliding speed started at $startTime with window size $windowSize s === :")
    val slidingSpeed = extractor.extractSlidingSpeed(convertedRDD, startTime, endTime, windowSize)
    for (i <- slidingSpeed.take(5)) {
      println(s"Road ID: ${i._1} --- Sliding Flow ${i._2}")
    }

    sc.stop()
  }
}