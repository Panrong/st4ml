package experiments

import instances.{Duration, Extent, Trajectory}
import operatorsNew.converter.{Traj2SpatialMapConverter, Traj2TrajConverter}
import operatorsNew.selector.Selector
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime

object MapMatching {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("MapMatching")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val mapFile = args(0)
    val trajDir = args(1)
    val spatial = Extent(args(2).split(",").map(_.toDouble)).toPolygon
    val temporal = Duration(args(3).split(",").map(_.toLong))
    val numPartitions = args(4).toInt

    val t = nanoTime

    type TRAJ = Trajectory[Option[String], String]
    val selector = Selector[TRAJ](spatial, temporal, numPartitions)
    val trajRDD = selector.selectTraj(trajDir)//.sample(false, 0.02)
    val converter = new Traj2TrajConverter[Option[String]](mapFile)
    val mmRDD = converter.convert(trajRDD)
    //    println(mmRDD.count)
    //    mmRDD.take(2).foreach(println)
    val converter2 = new Traj2SpatialMapConverter(Array(Extent(Array(0.0, 0, 1, 1)).toPolygon))
    val smRDD = converter2.convert(mmRDD, converter.mapMatcher.roadNetwork)
      .map(_.mapValue(x => (x._1, x._2.length)))
    println(smRDD.count)
    println(s"road network flow extraction ${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }
}
