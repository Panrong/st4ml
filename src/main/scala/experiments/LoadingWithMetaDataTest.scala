package experiments

import instances.{Duration, Event, Extent, Point, Polygon, Trajectory}
import operatorsNew.converter.Event2TrajConverter
import operatorsNew.selector.SelectionUtils.EwP
import operatorsNew.selector.partitioner.{HashPartitioner, STPartitioner}
import org.apache.spark.sql.SparkSession
import operatorsNew.selector.{MultiRangeSelector, Selector}
import utils.Config
import operatorsNew.selector.SelectionUtils._
import org.apache.spark.rdd.RDD

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
    val instance = args(5)
    val ratio = args(6).toDouble
    val useMetadata = args(7).toBoolean
    val spark = SparkSession.builder()
      .appName("LoadingWithMetaDataTest")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    val random = new scala.util.Random(1)

    //    val wholeSpatial = Extent(-8.7, 41, -7.87, 41.42)
    //    val wholeSpatial = Extent(-8.632746, 41.141376, -8.62668, 41.154516)
    val wholeSpatial = Extent(sRange(0), sRange(1), sRange(2), sRange(3))
    //    val wholeTemporal = Duration(1372636800, 1404172800)
    //    val wholeTemporal = Duration(1372636858, 1372637188)
    val wholeTemporal = Duration(tRange(0), tRange(1))

    var t = nanoTime()


    println(ratio)
    val start1 = random.nextDouble * (1 - sqrt(ratio))
    val start2 = random.nextDouble * (1 - sqrt(ratio))
    val start3 = random.nextDouble * (1 - ratio)
    val spatial = Extent(wholeSpatial.xMin + start1 * (wholeSpatial.xMax - wholeSpatial.xMin),
      wholeSpatial.yMin + start2 * (wholeSpatial.yMax - wholeSpatial.yMin),
      wholeSpatial.xMin + (start1 + sqrt(ratio)) * (wholeSpatial.xMax - wholeSpatial.xMin),
      wholeSpatial.yMin + (start2 + sqrt(ratio)) * (wholeSpatial.yMax - wholeSpatial.yMin)).toPolygon
    val temporal = Duration(wholeTemporal.start + (start3 * (wholeTemporal.end - wholeTemporal.start)).toLong,
      wholeTemporal.start + ((start3 + ratio) * (wholeTemporal.end - wholeTemporal.start)).toLong
    )
    //      println(ratio, spatial.getCoordinates.deep, temporal)
    //          println(spatial.getArea / wholeSpatial.getArea, temporal.seconds / wholeTemporal.seconds.toDouble)
    if (instance == "event") {
      type EVT = Event[Point, Option[String], String]
      if (useMetadata) {

        // metadata
        t = nanoTime()
        val selector = Selector[EVT](spatial, temporal, numPartitions)
        val rdd1 = selector.select(fileName, metadata)
        println(rdd1.count)
        println(s"metadata: ${(nanoTime() - t) * 1e-9} s.")
      }
      else {
        //no metadata
        t = nanoTime()
        import spark.implicits._
        val eventRDD = spark.read.parquet(fileName).drop("pId").as[E].toRdd //.repartition(numPartitions)
        val partitioner = new HashPartitioner(numPartitions)
        val rdd2 = partitioner.partition(eventRDD).filter(_.intersects(spatial, temporal))
        println(rdd2.count)
        println(s"no metadata: ${(nanoTime() - t) * 1e-9} s.\n")
        eventRDD.unpersist()
      }
    }

    else if (instance == "traj") {
      type TRAJ = Trajectory[Option[String], String]

      if (useMetadata) {
        // metadata
        val selector = Selector[TRAJ](spatial, temporal, numPartitions)
        val rdd1 = selector.select(fileName, metadata)
      }
      // no metadata
      else {
        t = nanoTime()
        import spark.implicits._
        val trajDf = spark.read.parquet(fileName).drop("pId").as[T]
        val trajRDD = trajDf.toRdd //.repartition(numPartitions)
        //        println(s"no metadata total: ${trajRDD.count}")

        val partitioner = new HashPartitioner(numPartitions)
        val partitionedRDD = partitioner.partition(trajRDD)
        partitionedRDD.count
        println(s"data loading：${(nanoTime() - t) * 1e-9} s.")
        t = nanoTime()
        val rdd2 = partitionedRDD.filter(_.intersects(spatial, temporal))
        println(rdd2.count)
        println(s"no metadata selection time: ${(nanoTime() - t) * 1e-9} s.\n")
      }
    }
    spark.stop()
  }
}
