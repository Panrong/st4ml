package examples

import geometry.Rectangle
import operators.CustomOperatorSet
import operators.convertion.Converter
import operators.extraction.TimeSeriesExtractor
import operators.selection.DefaultSelector
import operators.selection.partitioner.STRPartitioner
import org.apache.spark.sql.SparkSession
import preprocessing.ReadTrajJson
import utils.Config
import utils.TimeParsing.parseTemporalRange
import java.lang.System.nanoTime

object TrajTimeSeriesDev {
  def main(args: Array[String]): Unit = {
    /** set up Spark environment */
    val spark = SparkSession
      .builder()
      .appName("TrajTimeSeriesApp")
      .master(Config.get("master"))
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /** parse input arguments */
    val trajFile = Config.get("hzData")
    val numPartitions = Config.get("numPartitions").toInt
    val sQuery = Rectangle(args(0).split(",").map(_.toDouble))
    val tQuery = parseTemporalRange(args(1))
    val queryRange = args(2).split(",").map(_.toLong)
    val timeInterval = args(3).toInt

    /**
     * example input arguments: 118.116,29.061,120.167,30.184 1596816000,1596902400 1596841200,1596852000 900
     */

    /** initialize operators */
    val operator = new CustomOperatorSet(
      DefaultSelector(numPartitions),
      new Converter,
      new TimeSeriesExtractor)
    /** read input data */
    val trajRDD = ReadTrajJson(trajFile, numPartitions, clean = true)

    /** step 1: Selection */
    val rdd1 = operator.selector.query(trajRDD, sQuery, tQuery)
    rdd1.cache()
    println(s"Number of trajectories: ${rdd1.count}")

    /** step 2: Conversion */
    var t = nanoTime()
    println("--- start conversion")
    t = nanoTime()
    val partitioner = new STRPartitioner(numPartitions, Some(Config.get("samplingRate").toDouble))
    val rdd2 = operator.converter.traj2TimeSeries(rdd1, startTime = tQuery._1, timeInterval, partitioner)
    rdd2.cache()
    rdd2.take(1)
    println(s"... conversion takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    println(s"Number of partitions: ${rdd2.getNumPartitions}")

    /** step 3: Extraction */
    t = nanoTime()
    val res = operator.extractor.countTimeSlotSamplesSpatial((queryRange.head, queryRange.last))(rdd2)
    val resCombined = res.flatMap(x => x).reduceByKey(_ + _).collect
    println(resCombined.sortBy(_._1._1).deep)
    println(s"Total number: ${resCombined.map(_._2).sum}")
    println(s"... extraction takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    val slots = resCombined.map(_._1)
    println(s"Number of slots: ${slots.length}")

    /** benchmark */
    println(s"coarse total count by filtering trajectory: ${trajRDD.filter(x => x.inside(sQuery) && temporalOverlap(x.timeStamp, tQuery)).count}")
    sc.stop()
  }

  def temporalOverlap(t1: (Long, Long), t2: (Long, Long)): Boolean = {
    if (t1._1 >= t2._1 && t1._1 <= t2._2) true
    else if (t2._1 >= t1._1 && t2._1 <= t1._2) true
    else false
  }
}