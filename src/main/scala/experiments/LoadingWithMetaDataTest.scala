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
        t = nanoTime()
        // metadata
        val selector = Selector[TRAJ](spatial, temporal, numPartitions)
        val rdd1 = selector.select(fileName, metadata)
        println(rdd1.count)
        println(s"metadata: ${(nanoTime() - t) * 1e-9} s.")
      }
      // no metadata
      else {
        t = nanoTime()
        //        import spark.implicits._
        //        val trajDf = spark.read.parquet(fileName).drop("pId").as[T]
        //        val trajRDD = trajDf.toRdd //.repartition(numPartitions)
        //        //        println(s"no metadata total: ${trajRDD.count}")

        val trajRDD = readTrajJson(fileName)
        val partitioner = new HashPartitioner(numPartitions)
        val rdd2 = partitioner.partition(trajRDD).filter(_.intersects(spatial, temporal))
        println(rdd2.count)
        println(s"no metadata: ${(nanoTime() - t) * 1e-9} s.\n")
      }
    }
    spark.stop()
  }

  case class P(
                latitude: String,
                longitude: String,
                timestamp: String
              )

  case class TmpTraj(
                      id: String,
                      points: Array[P]
                    )

  def readTrajJson(fileName: String): RDD[Trajectory[None.type, String]] = {

    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = spark.read.option("multiline", "true").json(fileName)
    df.as[TmpTraj].rdd
      .filter(tmpTraj => tmpTraj.points.length != 0)
      .map(tmpTraj => {
        val traj = tmpTraj.points.length match {
          case 0 => Trajectory(Array(Point(0, 0), Point(0, 0)), Array(Duration(-1), Duration(-1)), Array(None, None), tmpTraj.id)
          case 1 => Trajectory(Array(Point(0, 0), Point(0, 0)), Array(Duration(-1), Duration(-1)), Array(None, None), tmpTraj.id)
          case _ =>
            try {
              val points = tmpTraj.points.map(p =>
                Point(p.longitude.toDouble, p.latitude.toDouble))
              val durs = tmpTraj.points.map(p => Duration(p.timestamp.toLong))
              Trajectory(points, durs, Array(None), tmpTraj.id)
            }
            catch {
              case _: Throwable => Trajectory(Array(Point(0, 0), Point(0, 0)), Array(Duration(-1), Duration(-1)), Array(None, None), tmpTraj.id)
            }
        }
        traj
      })
  }
}
