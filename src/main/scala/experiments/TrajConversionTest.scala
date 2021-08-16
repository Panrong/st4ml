package experiments

import instances.{Duration, Extent, Point, SpatialMap, Trajectory}
import operatorsNew.converter.{Traj2EventConverter, Traj2SpatialMapConverter, Traj2TimeSeriesConverter}
import operatorsNew.selector.DefaultSelector
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime

object TrajConversionTest {
  case class E(lon: Double, lat: Double, t: Long)

  case class T(id: String, entries: Array[E])

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("TrajConversionTest")
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
    val trajRDD = readDs.as[T].rdd.map(x => {
      val entries = x.entries.map(p => (Point(p.lon, p.lat), Duration(p.t), None))
      Trajectory(entries, x.id)
    })

    val selector = new DefaultSelector[Trajectory[None.type, String]](sQuery, tQuery, numPartitions)
    val res = selector.query(trajRDD)
    res.cache()
    res.count
    // selection done

    if (c == "sm") {
      val f: Array[Trajectory[None.type, String]] => Array[Trajectory[None.type, String]] = x => x
      val xArray = (sQuery.xMin until sQuery.xMax by (sQuery.xMax - sQuery.xMin) / 11).sliding(2).toArray
      val yArray = (sQuery.yMin until sQuery.yMax by (sQuery.yMax - sQuery.yMin) / 11).sliding(2).toArray
      val sArray = xArray.flatMap(x => yArray.map(y => (x, y))).map(x => Extent(x._1(0), x._2(0), x._1(1), x._2(1)).toPolygon)
      val t = nanoTime

      val converter = new Traj2SpatialMapConverter(f, sArray)
      val convertedRDD = converter.convert(res)
      println(convertedRDD.count)
      println("traj to spatial map")
      println((nanoTime - t) * 1e-9)
      //      println(res.count())
      //      var sum = 0
      //
      //      convertedRDD.collect().foreach(sm => sm.entries.foreach(x => {
      //        println(x.temporal, x.spatial)
      //        println(x.value.length)
      //        sum += x.value.length
      //      })
      //      )
      //      println(sum)
    }

    //    else if (c == "raster") {
    //      val converter2 = new Event2TrajConverter[None.type, String]
    //      val trajRDD = converter2.convert(trajRDD)
    //      println(trajRDD.count)
    //      println("event to trajectory")
    //      println((nanoTime - t) * 1e-9)
    //    }
    else if (c == "event") {
      val t = nanoTime()
      val converter = new Traj2EventConverter[None.type, String]
      val convertedRDD = converter.convert(trajRDD)
      println(convertedRDD.count)
      println("traj to event")
      println((nanoTime - t) * 1e-9)
    }

    else if (c == "ts") {
      val f: Array[Trajectory[None.type, String]] => Array[Trajectory[None.type, String]] = x => x
      val tArray = (tQuery.start until tQuery.end by (tQuery.end - tQuery.start) / 10).sliding(2).map(x => Duration(x(0), x(1))).toArray
      val t = nanoTime
      val converter = new Traj2TimeSeriesConverter(f, tArray)
      val convertedRDD = converter.convert(trajRDD)
      //      println(convertedRDD.count)
      println("traj to time series")
      println((nanoTime - t) * 1e-9)
      //      println(res.count())
      //      convertedRDD.collect().head.entries.foreach(x => {
      //        println(x.temporal, x.spatial)
      //        println(x.value.length)
      //      })
    }
    sc.stop()
  }
}
