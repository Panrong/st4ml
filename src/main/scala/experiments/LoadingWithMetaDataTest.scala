package experiments

import instances.{Duration, Event, Extent, Point, Polygon, Trajectory}
import operatorsNew.converter.Event2TrajConverter
import operatorsNew.selector.partitioner.{HashPartitioner, SpatialPartitioner}
import org.apache.spark.sql.SparkSession
import operatorsNew.selector.{MultiRangeSelector, Selector}
import utils.Config

import java.lang.System.nanoTime

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

    var t = nanoTime()

    val spatial = Extent(sRange(0), sRange(1), sRange(2), sRange(3)).toPolygon
    val temporal = Duration(tRange(0), tRange(1))
    type T = Event[Point, Option[String], String]
    val selector =  Selector[T](spatial, temporal, numPartitions)
    val rdd1 = selector.select(fileName, metadata)

    val multiSelector = new MultiRangeSelector[T](Array(spatial), Array(temporal), new HashPartitioner(numPartitions))
    val rdd1M = multiSelector.selectWithInfo(fileName, metadata)
    rdd1M.take(5).foreach(println)

    rdd1.map(_.extent)
      .collect()
    println(rdd1.count)

    val converter = new Event2TrajConverter[Option[String], String]
    val rdd2 = converter.convert(rdd1)
    rdd2.collect
    println(rdd2.collect.deep)

    println(Polygon.empty)
    println(Extent(0,0,1,1).toPolygon)
    println((Extent(0,0,1,1).toPolygon union Extent(1,1,2,2).toPolygon).getEnvelope)

    //    /** compare with original method */
    //    val rdd2 = spark.read.parquet(fileName).as[TwP].toRdd
    //      .map(_._1)
    //      .filter(_.intersects(spatial, temporal))
    //    println(rdd2.count)

    sc.stop()
  }
}
