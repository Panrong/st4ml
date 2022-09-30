package experiments

import org.apache.spark.sql.SparkSession
import st4ml.instances.{Duration, Event, Extent, Point}
import st4ml.operators.selector.Selector
import st4ml.operators.selector.partitioner.TSTRPartitioner
import st4ml.utils.Config
import st4ml.utils.TimeParsing.getHour

import scala.io.Source

object LoadBalance {
  def main(args: Array[String]): Unit = {
    val mode = args(0)
    val fileName = args(1)
    val metadata = args(2)
    val queryFile = args(3)
    val numPartitions = args(4).toInt
    val opt = args(5).toBoolean
    val spark = SparkSession.builder()
      .appName("AnomalyExtraction")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // read queries
    val f = Source.fromFile(queryFile)
    val ranges = f.getLines().toArray.map(line => {
      val r = line.split(" ")
      (Extent(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble).toPolygon, Duration(r(4).toLong, r(5).toLong))
    })
    if (mode == "event") {
      type EVENT = Event[Point, Option[String], String]
      for ((spatial, temporal) <- ranges) {
        val selector = new Selector[EVENT]().setSQuery(spatial).setTQuery(temporal).setParallelism(numPartitions)
          .setPartitioner(TSTRPartitioner(numPartitions, samplingRate = Some(1)))
        val eventRDD = selector.selectEvent(fileName, metadata, partition = opt)
        val countRDD = eventRDD.mapPartitions(s => Iterator(s.length))
        val areaRDD = eventRDD.mapPartitions(s => {
          val arr = s.toArray
          if (arr.length > 0)
            Iterator((Extent(arr.map(_.extent)), Duration(arr.map(_.duration))))
          else Iterator()
        })
        //        areaRDD.collect().foreach(x => println(x, x._1.area * x._2.seconds))
        //        println(Extent(areaRDD.collect().map(_._1)), Duration(areaRDD.collect().map(_._2)))
        val area1 = areaRDD.collect().map(x => x._1.area * x._2.seconds).sum
        val area2 = Extent(areaRDD.collect().map(_._1)).area * Duration(areaRDD.collect().map(_._2)).seconds
        println(area1, area2)
        //        println(area1 / area2)
        println(countRDD.collect().deep)
      }
    }
    sc.stop()
  }
}
