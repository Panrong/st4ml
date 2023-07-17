// For figure 7(a)
package experiments

import st4ml.instances.{Duration, Event, Extent, Point}
import st4ml.operators.selector.Selector
import org.apache.spark.sql.SparkSession
import st4ml.utils.Config
import st4ml.utils.TimeParsing.getHour

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
      val selector = new Selector[EVENT]().setSQuery(spatial).setTQuery(temporal).setParallelism(numPartitions)
      val eventRDD = selector.selectEvent(fileName, metadata, partition = false)

      val res = eventRDD.filter(x => condition(getHour(x.duration.start))).map(_.data)
      eventRDD.unpersist()
      println(res.take(5).deep)
    }
    println(s"Anomaly extraction ${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }
}
