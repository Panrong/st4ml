package experiments

import instances.{Duration, Event, Extent, Point}
import operatorsNew.selector.Selector
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime
import scala.io.Source

object AnomalyExtraction {
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val metadata = args(1)
    val queryFile = args(2)
    val threshold = args(3).split(",").map(_.toLong)
    val numPartitions = args(4).toInt
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
    val t = nanoTime()
    type EVENT = Event[Point, Option[String], String]
    val condition = if (threshold(0) > threshold(1)) (x: Double) => x >= threshold(0) || x < threshold(1)
    else (x: Double) => x >= threshold(0) && x < threshold(1)

    for ((spatial, temporal) <- ranges) {
      val selector = Selector[EVENT](spatial, temporal, numPartitions)
      val eventRDD = selector.selectEvent(fileName, metadata, false)

      val res = eventRDD.filter(x => condition(x.duration.hours)).map(_.data).collect
      eventRDD.unpersist()
      println(res.take(5).deep)
    }
    println(s"Anomaly extraction ${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }
}
