package experiments

import instances.{Duration, Event, Extent, Point}
import operatorsNew.converter.{Event2TimeSeriesConverter, Event2TrajConverter}
import operatorsNew.selector.DefaultSelector
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime

object EventConversionTest {
  case class E(lon: Double, lat: Double, t: Long, id: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("EventConversionTest")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val fileName = args(0)
    val spatialRange = args(1).split(",").map(_.toDouble)
    val temporalRange = args(2).split(",").map(_.toLong)
    val numPartitions = args(3).toInt
    val c = args(4)

    val sQuery = new Extent(spatialRange(0), spatialRange(1), spatialRange(2), spatialRange(3))
    val tQuery = Duration(temporalRange(0), temporalRange(1))

    // read parquet
    val readDs = spark.read.parquet(fileName)

    import spark.implicits._
    val eventRDD = readDs.as[E].rdd.map(x => {
      Event(Point(x.lon, x.lat), new Duration(x.t, x.t), None, x.id)
    })
    println(eventRDD.take(5).deep)
    type I = Event[Point, None.type, String]
    val selector = new DefaultSelector[I](sQuery, tQuery, numPartitions)
    eventRDD.cache()
    val res = selector.query(eventRDD)
    println(res.count)
    // selection done

    if (c == "ts") {
      val f: Array[Event[Point, None.type, String]] => Array[Event[Point, None.type, String]] = x => x
      val tArray = (tQuery.start until tQuery.end by (tQuery.end - tQuery.start) / 10).sliding(2).map(x => Duration(x(0), x(1))).toArray
      val t = nanoTime
      val converter = new Event2TimeSeriesConverter(f, tArray)
      val convertedRDD = converter.convert(res)
      println(convertedRDD.count)
      println("event to time series")
      println((nanoTime - t) * 1e-9)
    }
    else if (c == "traj") {
      val t = nanoTime
      val converter2 = new Event2TrajConverter[None.type, String]
      val trajRDD = converter2.convert(res)
      println(trajRDD.count)
      println("event to trajectory")
      println((nanoTime - t) * 1e-9)
    }
    sc.stop()
  }
}
