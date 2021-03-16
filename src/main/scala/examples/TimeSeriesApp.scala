package examples

import geometry.Rectangle
import operators.convertion.Converter
import operators.extraction.TimeSeriesExtractor
import operators.selection.DefaultSelector
import org.apache.spark.sql.SparkSession
import preprocessing.ReadTrajJson
import utils.TimeParsing.parseTemporalRange

import java.lang.System.nanoTime

object TimeSeriesApp {
  def main(args: Array[String]): Unit = {

    /** set up Spark environment */
    val spark = SparkSession
      .builder()
      .appName("TimeSeriesApp")
      //      .master("local[*]")
      .getOrCreate()
    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /** parse input arguments */
    val trajFile = args(0)
    val numPartitions = args(1).toInt
    val sQuery = Rectangle(args(2).split(",").map(_.toDouble))
    val tQuery = parseTemporalRange(args(3))
    val queryRange = args(4).split(",").map(_.toLong)

    /** read input data */
    val trajRDD = ReadTrajJson(trajFile, numPartitions)

    /** step 1: Selection */
    val selector = DefaultSelector(numPartitions)
    val rdd1 = selector.query(trajRDD, sQuery, tQuery)
    rdd1.cache()

    /** step 2: Conversion */
    val converter = new Converter
    val pointRDD = converter.traj2Point(rdd1).map((0, _))
    pointRDD.cache()
    pointRDD.take(1)
    var t = nanoTime()
    println("--- start conversion")
    t = nanoTime()
    val rdd2 = converter.point2TimeSeries(pointRDD, startTime = 1596038419, 15 * 60)
    rdd2.cache()
    rdd2.take(1)
    println(s"... conversion takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    t = nanoTime()

    /** step 3: Extraction */
    val extractor = new TimeSeriesExtractor()
    val res = extractor.CountTimeSlotSamples((queryRange.head, queryRange.last))(rdd2)
    println(res.collect.sortBy(_._1._1).deep)
    println(s"... extraction takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

    val slots = res.collect.map(_._1)

    /** benchmark */
    t = nanoTime()

    val benchmark = pointRDD.mapPartitions(iter => {
      val points = iter.map(_._2).toArray
      slots.map(slot => {
        (slot, points.count(x => x.timeStamp._1 >= slot._1 && x.timeStamp._1 < slot._2))
      }).toIterator
    }).reduceByKey(_ + _)
    println(benchmark.collect.sortBy(_._1._1).deep)
    println(s"... benchmark takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    println(res.map(_._2).sum)
    println(benchmark.map(_._2).sum)
    println(slots.length)
    //    println(pointRDD.count)
    //    println(rdd2.map(x => x.series.flatten.length).sum)
    //    println(rdd2.flatMap(x => x.toMap.values).map(_.length).reduce(_ + _))
    //    var sum = 0
    //    var slot = new Array[(Long, Long)](0)
    //    val test = rdd2.flatMap(_.toMap).collect
    //    val testMap = test.toMap.mapValues(_.length)
    //    test.sortBy(_._1._1).foreach(x => {
    //      println(s"${x._1} ${x._2.length}")
    //      sum += x._2.length
    //      slot = slot :+ x._1
    //    })
    //    var gt = slot.map((_, 0)).toMap
    //    val points = pointRDD.map(_._2).collect()
    //    for (point <- points) {
    //      for (ts <- gt.keys) {
    //        if (point.timeStamp._1 >= ts._1 && point.timeStamp._2 < ts._2) {
    //          gt = gt + (ts -> (gt(ts) +1))
    //        }
    //      }
    //    }
    //    gt.toArray.sortBy(_._1._1).foreach(x => {
    //      println(s"${x._1} ${x._2}")
    //    })
    //
    //    for(i <- slot){
    //      if(testMap(i)!=gt(i)) println(i, testMap(i), gt(i))
    //    }
    //
    //    println(sum)
    //    println(gt.values.toArray.sum)
    sc.stop()
  }
}

