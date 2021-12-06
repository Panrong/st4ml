package experiments

import instances.GeometryImplicits.withExtraPointOps
import instances.{Duration, Entry, Extent, Point, Polygon, SpatialMap, Trajectory}
import operatorsNew.converter.Traj2SpatialMapConverter
import operatorsNew.selector.DefaultLegacySelector
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime

object RegionalSpeedTest {
  case class E(lon: Double, lat: Double, t: Long)

  case class T(id: String, entries: Array[E])

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("RegionalSpeedTest")
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
    val trajRDD = readDs.as[T].rdd.map(x => {
      val entries = x.entries.map(p => (Point(p.lon, p.lat), Duration(p.t), None))
      Trajectory(entries, x.id)
    })

    val selector = new DefaultLegacySelector[Trajectory[None.type, String]](sQuery, tQuery, numPartitions)
    val res = selector.query(trajRDD)

    //    val res = sc.parallelize(Array(
    //      Trajectory(Array(Point(5,5), Point(5,10), Point(5, 15)), Array(Duration(0), Duration(1), Duration(3)), Array(None, None, None), "1"),
    //      Trajectory(Array(Point(5,15), Point(10,15), Point(15, 15), Point(16,16)), Array(Duration(0), Duration(1), Duration(3), Duration(4)), Array(None, None, None, None), "1")
    //    ))
    // selection done

    val f: Array[Trajectory[None.type, String]] => Array[Trajectory[None.type, String]] = x => x

    val xArray = (sQuery.xMin until sQuery.xMax by (sQuery.xMax - sQuery.xMin) / 11).sliding(2).toArray
    val yArray = (sQuery.yMin until sQuery.yMax by (sQuery.yMax - sQuery.yMin) / 11).sliding(2).toArray
    val sArray = xArray.flatMap(x => yArray.map(y => (x, y))).map(x => Extent(x._1(0), x._2(0), x._1(1), x._2(1)).toPolygon)

    //    val sArray = Array(Extent(0,0,10,10), Extent(10,0,20,10),Extent(10,10,20,20), Extent(0,10,10,20)).map(_.toPolygon)
    val converter = new Traj2SpatialMapConverter(sArray)
    val smRDD = converter.convert(res)
    smRDD.count()
    val t = nanoTime

    val calSpeed: Array[Trajectory[None.type, String]] => Option[Double] = trajArray => {
      if (trajArray.isEmpty) None
      else {
        val speedArr = trajArray.map(traj => {
          //            traj.consecutiveSpatialDistance("euclidean").sum / traj.duration.seconds
          traj.entries.last.spatial.greatCircle(traj.entries.head.spatial) / (traj.entries.last.temporal.end - traj.entries.head.temporal.start)
        })
        Some(speedArr.sum / speedArr.length)
      }
    }

    val speedRDD = smRDD.map(sm => {
      val sBins = sm.spatials
      val newEntries = sm.entries.zipWithIndex.map { case (entry, entryIdx) =>
        val newValue = entry.value.map(traj => {
          val entryIds = traj.entries.zipWithIndex.filter(entryWIdx =>
            entryWIdx._1.spatial.intersects(sBins(entryIdx))).map(_._2)
          if (entryIds.length < 2) None
          else {
            val start = entryIds.head
            val end = entryIds.last
            Some(new Trajectory(traj.entries.slice(start, end + 1), traj.data))
          }
        }).filter(_.isDefined).map(_.get)
        new Entry(entry.spatial, entry.duration, newValue)
      }
      val newSm = new SpatialMap(newEntries, sm.data)
      newSm.mapValue(calSpeed)
    })

    val aggregatedSpeed = speedRDD.map(sm =>
      sm.entries.map(_.value).zipWithIndex.map(_.swap)).flatMap(x => x)
      .groupByKey()
      .mapValues(x => {
        val arr = x.toArray.filter(_.isDefined)
        if (arr.isEmpty) 0
        else arr.map(_.get).sum / arr.length
      })
    val speeds = aggregatedSpeed.collect.map(x => (sArray(x._1), x._2))
    val t2 = nanoTime

    println(speeds.deep)
    println((t2 - t) * 1e-9)

    sc.stop()
  }
}
