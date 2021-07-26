package experiments

import experiments.TrajSpeedSpark.{genGrids, genSTGrids, greatCircleDistance}
import instances.Extent.toPolygon
import instances.GeometryImplicits.withExtraPointOps
import instances.{Duration, Extent, Trajectory}
import operatorsNew.selector.MultiSTRangeSelector
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.Polygon
import preprocessing.ParquetReader
import utils.Config

object TrajSpeedExp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("flowExp")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    /**
     * C:\Users\kaiqi001\Documents\GitHub\geomesa-fs_2.12-3.2.0\trajectory\2013\07\01\13_W2757b56033204294839f0dd45b1fd366.parquet "-8.65, 41.13, -8.57, 41.17" "1372636800,1372736800" 2 36000 16
     */
    val sQuery = args(1).split(",").map(_.toDouble)
    val tQuery = args(2).split(",").map(_.toLong)
    val sSize = args(3).toInt
    val tSplit = args(4).toInt
    val grids = genGrids(sQuery, sSize)
    val stGrids = genSTGrids(grids, (tQuery(0), tQuery(1)), tSplit)
      .map(x => (new Extent(x._1(0), x._1(1), x._1(2), x._1(3)),
        Duration(x._2(0), x._2(1))))
    val sQ = stGrids.map(x => toPolygon(x._1))
    val tQ = stGrids.map(_._2)
    val numPartitions = args(5).toInt

    val trajFile = args(0)

    val trajRDD = ParquetReader.readTrajGeomesa(trajFile)

    val selector = new MultiSTRangeSelector[Trajectory[None.type, String]](sQ, tQ, numPartitions)
    val selectedRDD = selector.queryWithInfo(trajRDD)
    val speedRDD = selectedRDD.flatMap {
      case (traj, qArr) => qArr.map(q => (traj, q))
    }.map {
      case (traj, q) => (q, calSpeed(traj, sQ(q), tQ(q)))
    }.filter(_._2.isDefined)
      .map(x => (x._1, new AvgCollector(x._2.get)))
      .reduceByKey(_ combine _)
      .map { case (k, v) => (k, v.avg) }

    val paddingRDD = sQ.indices.map((_, 0)).toMap
    (paddingRDD ++ speedRDD.collect.toMap).foreach {
      case (idx, speed) => println(sQ(idx), tQ(idx), speed)
    }

    sc.stop()
  }

  class AvgCollector(val tot: Double, val cnt: Int = 1) extends Serializable {
    def combine(that: AvgCollector) = new AvgCollector(tot + that.tot, cnt + that.cnt)

    def avg: Double = tot / cnt
  }

  def calSpeed(traj: Trajectory[_, _], sRange: Polygon, tRange: Duration): Option[Double] = {
    val validPoints = traj.entries.filter(p =>
      p.spatial.intersects(sRange) && p.temporal.intersects(tRange))
    if (validPoints.length < 2) None
    else {
      val duration = validPoints.last.temporal.end - validPoints.head.temporal.start
      val distance = validPoints.last.spatial.greatCircle(validPoints.head.spatial)
      Some(distance / duration)
    }
  }
}
