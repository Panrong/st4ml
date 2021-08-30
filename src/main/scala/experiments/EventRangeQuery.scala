package experiments

import instances.Extent.toPolygon
import instances.{Duration, Extent, Point, Event}
import operatorsNew.selector.MultiSTRangeSelector
import operatorsNew.selector.partitioner.HashPartitioner
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime
import scala.io.Source

case class P(id: String, lon: Double, lat: Double, t: Long)

object EventRangeQuery {
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val queryFile = args(1)
    val numPartitions = args(2).toInt
    val partition = args(3).toBoolean

    // read queries
    val f = Source.fromFile(queryFile)
    val queries = f.getLines().toArray.map(line => {
      val r = line.split(" ")
      (Extent(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble),
        Duration(r(4).toLong, r(5).toLong))
    })
    val spark = SparkSession.builder()
      .appName("eventRangeQuery")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    var t = nanoTime
    // read events
    val readDs = spark.read.parquet(fileName)
    import spark.implicits._
    val eventRDD = readDs.as[P].rdd.map(x => Event(Point(x.lon, x.lat), Duration(x.t), d = x.id))

    println(eventRDD.count)
    println(s"Data loading ${(nanoTime - t) * 1e-9} s")
    t = nanoTime
    eventRDD.cache()

    val sQuery = queries.map(x => toPolygon(x._1))
    val tQuery = queries.map(x => x._2)
    val selector = new MultiSTRangeSelector[Event[Point, None.type, String]](sQuery, tQuery, numPartitions, partition)
    val res = selector.queryWithInfo(eventRDD, true).flatMap {
      case (_, qArray) => qArray.map((_, 1))
    }.reduceByKey(_ + _)
    println(res.collect.deep)
    println(s"Multi range query ${(nanoTime - t) * 1e-9} s")


    sc.stop()
  }
}
