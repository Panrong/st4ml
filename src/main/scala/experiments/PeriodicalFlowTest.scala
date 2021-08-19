package experiments

import instances.{Duration, Event, Extent, Point}
import operatorsNew.converter.Event2TimeSeriesConverter
import operatorsNew.selector.DefaultSelector
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime

object PeriodicalFlowTest {
  case class E(lon: Double, lat: Double, t: Long, id: String)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("PeriodicalFlowTest")
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
      Event(Point(x.lon, x.lat), Duration(x.t), d = x.id)
    })

    val selector = new DefaultSelector[Event[Point, None.type, String]](sQuery, tQuery, numPartitions)
    val res = selector.query(eventRDD)
    // selection done

    val f: Array[Event[Point, None.type, String]] => Int = x => x.length
    val tArray = (tQuery.start until tQuery.end by 86400)
      .sliding(2)
      .map(x => Duration(x(0), x(1))).toArray

    val converter = new Event2TimeSeriesConverter(f, tArray)
    val tsRDD = converter.convert(res)
    tsRDD.count
    val t = nanoTime
    val binRDD = tsRDD.flatMap(ts => ts.entries.zipWithIndex.map {
      case (bin, idx) => (idx, bin.value)
    })
    val binCount = binRDD.reduceByKey(_ + _).collect
    val t2 = nanoTime

    println(binCount.deep)
    println((t2 - t) * 1e-9)

    sc.stop()
  }
}
