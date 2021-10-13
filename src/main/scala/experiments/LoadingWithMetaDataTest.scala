package experiments

import instances.{Duration, Event, Extent, Point, Polygon, Trajectory}
import operatorsNew.converter.Event2TrajConverter
import operatorsNew.selector.SelectionUtils.EwP
import operatorsNew.selector.partitioner.{HashPartitioner, STPartitioner}
import org.apache.spark.sql.SparkSession
import operatorsNew.selector.{MultiRangeSelector, Selector}
import utils.Config
import operatorsNew.selector.SelectionUtils._
import scala.util.Random.nextDouble
import java.lang.System.nanoTime
import scala.math.sqrt

object LoadingWithMetaDataTest {

  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val numPartitions = args(1).toInt
    val metadata = args(2)
    val sRange = args(3).split(",").map(_.toDouble) // -8.446832 41.010165 -7.932837 41.381359
    val tRange = args(4).split(",").map(_.toLong)

    val spark = SparkSession.builder()
      .appName("LoadingWithMetaDataTest")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    //    val wholeSpatial = Extent(-8.7, 41, -7.87, 41.42)
    val wholeSpatial = Extent(-8.632746, 41.141376, -8.62668, 41.154516)

    //    val wholeTemporal = Duration(1372636800, 1404172800)
    val wholeTemporal = Duration(1372636858, 1372637188)
    type T = Event[Point, Option[String], String]
    var t = nanoTime()

    for (ratio <- List(0.2, 0.4, 0.6, 0.8, 1.0)) {
      val start1 = nextDouble * (1 - sqrt(ratio))
      val start2 = nextDouble * (1 - sqrt(ratio))
      val start3 = nextDouble * (1 - ratio)
      val spatial = Extent(wholeSpatial.xMin + start1 * (wholeSpatial.xMax - wholeSpatial.xMin),
        wholeSpatial.yMin + start2 * (wholeSpatial.yMax - wholeSpatial.yMin),
        wholeSpatial.xMin + (start1 + sqrt(ratio)) * (wholeSpatial.xMax - wholeSpatial.xMin),
        wholeSpatial.yMin + (start2 + sqrt(ratio)) * (wholeSpatial.yMax - wholeSpatial.yMin)).toPolygon
      val temporal = Duration(wholeTemporal.start + (start3 * (wholeTemporal.end - wholeTemporal.start)).toLong,
        wholeTemporal.start + ((start3 + ratio) * (wholeTemporal.end - wholeTemporal.start)).toLong
      )
      //      println(ratio, spatial.getCoordinates.deep, temporal)
      //      println(spatial.getArea / wholeSpatial.getArea, temporal.seconds / wholeTemporal.seconds.toDouble)

      /** event */
      // metadata
      t = nanoTime()
      val selector = Selector[T](spatial, temporal, numPartitions)
      val rdd1 = selector.select(fileName, metadata)
      println(rdd1.count)
      println(s"metadata: ${(nanoTime() - t) * 1e-9} s.")

      //no metadata
      t = nanoTime()
      import spark.implicits._
      val eventRDD = spark.read.parquet(fileName).drop("pId").as[E].toRdd
      val rdd2 = eventRDD.filter(_.intersects(spatial, temporal))
      println(rdd2.count)
      println(s"no metadata: ${(nanoTime() - t) * 1e-9} s.\n")

      //    val multiSelector = new MultiRangeSelector[T](Array(spatial), Array(temporal), new HashPartitioner(numPartitions))
      //    val rdd1M = multiSelector.selectWithInfo(fileName, metadata)
      //    rdd1M.take(5).foreach(println)
      //
      //    rdd1.map(_.extent)
      //      .collect()
      //    println(rdd1.count)
      //
      //    val converter = new Event2TrajConverter[Option[String], String]
      //    val rdd2 = converter.convert(rdd1)
      //    rdd2.collect
      //    println(rdd2.collect.deep)

      //    println(Polygon.empty)
      //    println(Extent(0,0,1,1).toPolygon)
      //    println((Extent(0,0,1,1).toPolygon union Extent(1,1,2,2).toPolygon).getEnvelope)

      //    /** compare with original method */
      //    val rdd2 = spark.read.parquet(fileName).as[TwP].toRdd
      //      .map(_._1)
      //      .filter(_.intersects(spatial, temporal))
      //    println(rdd2.count)
    }
    sc.stop()
  }
}
