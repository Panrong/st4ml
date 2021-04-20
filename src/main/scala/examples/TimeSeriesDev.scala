package examples

import geometry.Rectangle
import operators.convertion.Converter
import operators.extraction.TimeSeriesExtractor
import operators.selection.DefaultSelector
import operators.selection.partitioner._
import org.apache.spark.sql.SparkSession
import preprocessing.ReadTrajJson
import utils.Config
import utils.TimeParsing.parseTemporalRange

import java.lang.System.nanoTime

object TimeSeriesDev {
  def main(args: Array[String]): Unit = {

    /** set up Spark environment */
    val spark = SparkSession
      .builder()
      .appName("TimeSeriesApp")
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
     * example input arguments: -180,-180,180,180 0,20000000000 1597015819,1597016719 temporal
     */

    /** read input data */
    val trajRDD = ReadTrajJson(trajFile, numPartitions)

    /** step 1: Selection */
    val selector = DefaultSelector(numPartitions)
    val rdd1 = selector.query(trajRDD, sQuery, tQuery)
    rdd1.cache()

    /** step 2: Conversion */
    val converter = new Converter
    val pointRDD = converter.traj2Point(rdd1)
      .filter(x => {
        val (ts, te) = x.timeStamp
        ts <= tQuery._2 && te >= tQuery._1
      })
      .filter(x => x.inside(sQuery)).map((0, _))
    pointRDD.cache()
    pointRDD.take(1)
    println(s"Number of points: ${pointRDD.count}")
    var t = nanoTime()
    println("--- start conversion")
    t = nanoTime()
    val partitioner = new STRPartitioner(numPartitions, Some(0.1))
    val rdd2 = converter.point2TimeSeries(pointRDD, startTime = tQuery._1, timeInterval, partitioner)
      .flatMap(_.split(numPartitions))
    rdd2.cache()
    rdd2.take(1)
    println(s"... conversion takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    println(s"Number of partitions: ${rdd2.getNumPartitions}")
    t = nanoTime()

    /** step 3: Extraction */
    val extractor = new TimeSeriesExtractor()
    val res = extractor.countTimeSlotSamplesSpatial((queryRange.head, queryRange.last))(rdd2)
    val resCombined = res.flatMap(x => x).reduceByKey(_ + _).collect
    println(resCombined.sortBy(_._1._1).deep)
    println(s"Total number: ${resCombined.map(_._2).sum}")
    println(s"... extraction takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    val slots = resCombined.map(_._1)
    println(s"Number of slots: ${slots.length}")

    /** benchmark */
    t = nanoTime()
    val benchmark = pointRDD.mapPartitions(iter => {
      val points = iter.map(_._2).toArray
      slots.map(slot => {
        (slot, points.count(x => x.timeStamp._1 >= slot._1 && x.timeStamp._1 < slot._2))
      }).toIterator
    }).reduceByKey(_ + _)
    println("=== Benchmark:")
    println(benchmark.collect.sortBy(_._1._1).deep)
    println(s"... benchmark takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    println(s"Total number: ${benchmark.map(_._2).sum.toInt}")
    sc.stop()
  }
}

