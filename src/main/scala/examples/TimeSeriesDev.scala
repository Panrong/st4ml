package examples

import geometry.Rectangle
import operators.convertion.Converter
import operators.extraction.TimeSeriesExtractor
import operators.selection.DefaultSelector
import operators.selection.partitioner.QuadTreePartitioner
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

    /**
     * example input arguments: -180,-180,180,180 0,20000000000 1597015819,1597016719 temporal
     */

    /** read input data */
    val trajRDD = ReadTrajJson(trajFile, numPartitions)

    /** step 1: Selection */
    val selector = DefaultSelector(numPartitions)
    val rdd1 = selector.query(trajRDD, sQuery, tQuery)
    rdd1.cache()

    //    if (mode == "temporal") {
    //      /** step 2: Conversion */
    //      val converter = new Converter
    //      val pointRDD = converter.traj2Point(rdd1).map((0, _))
    //      pointRDD.cache()
    //      pointRDD.take(1)
    //      var t = nanoTime()
    //      println("--- start conversion")
    //      t = nanoTime()
    //      val rdd2 = converter.point2TimeSeries(pointRDD, startTime = 1596038419, 15 * 60)
    //      rdd2.cache()
    //      rdd2.take(1)
    //      println(s"... conversion takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    //      t = nanoTime()
    //
    //      /** step 3: Extraction */
    //      val extractor = new TimeSeriesExtractor()
    //      val res = extractor.countTimeSlotSamples((queryRange.head, queryRange.last))(rdd2)
    //      println(res.collect.sortBy(_._1._1).deep)
    //      println(s"... extraction takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    //
    //      val slots = res.collect.map(_._1)
    //
    //      /** benchmark */
    //      t = nanoTime()
    //
    //      val benchmark = pointRDD.mapPartitions(iter => {
    //        val points = iter.map(_._2).toArray
    //        slots.map(slot => {
    //          (slot, points.count(x => x.timeStamp._1 >= slot._1 && x.timeStamp._1 < slot._2))
    //        }).toIterator
    //      }).reduceByKey(_ + _)
    //      println(benchmark.collect.sortBy(_._1._1).deep)
    //      println(s"... benchmark takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")
    //      println(res.map(_._2).sum)
    //      println(benchmark.map(_._2).sum)
    //      println(slots.length)
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
    //  }

    /** step 2: Conversion */
    val converter = new Converter
    val pointRDD = converter.traj2Point(rdd1).map((0, _))
    pointRDD.cache()
    pointRDD.take(1)
    println(s"Number of points: ${pointRDD.count}")
    var t = nanoTime()
    println("--- start conversion")
    t = nanoTime()
    val partitioner = new QuadTreePartitioner(numPartitions)
    val rdd2 = converter.point2TimeSeries(pointRDD, startTime = 1596038419, 15 * 60, partitioner)
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
    val slots = resCombined.map(_._1)
    println(s"Number of slots: ${slots.length}")
    println(s"... extraction takes ${((nanoTime() - t) * 1e-9).formatted("%.3f")} s.")

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

    println(s"Total number: ${benchmark.map(_._2).sum}")

    sc.stop()
  }

}

