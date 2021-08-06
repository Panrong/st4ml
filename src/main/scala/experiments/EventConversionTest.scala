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

    val sQuery = new Extent(spatialRange(0), spatialRange(1), spatialRange(2), spatialRange(3))
    val tQuery = Duration(temporalRange(0), temporalRange(1))

    // read parquet
    val readDs = spark.read.parquet(fileName)

    import spark.implicits._
    val eventRDD = readDs.as[E].rdd.map(x => {
      Event(Point(x.lon, x.lat), new Duration(x.t, x.t), None, x.id)
    })

    val selector = new DefaultSelector[Event[Point, None.type, String]](sQuery, tQuery, numPartitions)
    eventRDD.cache()
    val res = selector.query(eventRDD).count
    // selection done

    var t = nanoTime
    val f: Array[Event[Point, None.type, String]] => Array[Event[Point, None.type, String]] = x => x
    val tArray = (tQuery.start until tQuery.end by (tQuery.end - tQuery.start) / 10).sliding(2).map(x => Duration(x(0), x(1))).toArray
    val converter = new Event2TimeSeriesConverter(f, tArray)
    val convertedRDD = converter.convert(eventRDD)
    println(convertedRDD.count)
    println("event to time series")
    println((nanoTime - t) * 1e-9)

    t = nanoTime
    val converter2 = new Event2TrajConverter[None.type, String]
    val trajRDD = converter2.convert(eventRDD)
    println(trajRDD.count)
    println("event to trajectory")
    println((nanoTime - t) * 1e-9)

    sc.stop()
  }
}
