package experiments

import instances.{Duration, Extent, Point, Polygon, Trajectory}
import operatorsNew.converter.Traj2SpatialMapConverter
import operatorsNew.selector.DefaultSelector
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

    val selector = new DefaultSelector[Trajectory[None.type, String]](sQuery, tQuery, numPartitions)
    val res = selector.query(trajRDD)
    // selection done

    val f: Array[Trajectory[None.type, String]] => Array[Trajectory[None.type, String]] = x => x

    val xArray = (sQuery.xMin until sQuery.xMax by (sQuery.xMax - sQuery.xMin) / 11).sliding(2).toArray
    val yArray = (sQuery.yMin until sQuery.yMax by (sQuery.yMax - sQuery.yMin) / 11).sliding(2).toArray
    val sArray = xArray.flatMap(x => yArray.map(y => (x, y))).map(x => Extent(x._1(0), x._2(0), x._1(1), x._2(1)).toPolygon)

    val converter = new Traj2SpatialMapConverter(f, sArray)
    val smRDD = converter.convert(res)
    smRDD.count()
    val t = nanoTime

    val calSpeed: Array[Trajectory[None.type, String]] => Double = trajArray => {
      if (trajArray.isEmpty) -1
      else {
        val speedArr = trajArray.map(traj =>
          {
            traj.consecutiveSpatialDistance("euclidean").sum / traj.duration.seconds
          })
        speedArr.sum / speedArr.length
      }
    }

    val speedRDD = smRDD.map(sm => sm.mapValue(calSpeed))

    val aggregatedSpeed = speedRDD.map(sm =>
      sm.entries.map(_.value).zipWithIndex.map(_.swap)).flatMap(x => x)
      .groupByKey()
      .mapValues(x => {
        val arr = x.toArray.filter(_ > 0)
        if (arr.isEmpty) 0
        else arr.sum / arr.length
      })
    val speeds = aggregatedSpeed.collect.map(x => (sArray(x._1), x._2))
    val t2 = nanoTime

    println(speeds.deep)
    println((t2 - t) * 1e-9)

    sc.stop()
  }
}
