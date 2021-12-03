package experiments

import instances.{Duration, Extent, Trajectory}
import operatorsNew.selector.Selector
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime

object IntervalSpeedExtraction {
  // datasets/vhc_tstr datasets/vhc_metadata.json 4 118.35,29.183,120.5,30.55 1596211200 31
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val metadata = args(1)
    val numPartitions = args(2).toInt
    val sQuery = Extent(args(3).split(",").map(_.toDouble)).toPolygon
    val tStart = args(4).toLong
    val NumDays = args(5).toInt
    val spark = SparkSession.builder()
      .appName("IntervalSpeedExtraction")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    type TRAJ = Trajectory[Option[String], String]

    val ranges = (0 to NumDays).map(x =>
      (sQuery, Duration(x * 86400 + tStart, (x + 1) * 86400 + tStart))).toArray
    for ((spatial, temporal) <- ranges) {
      val t = nanoTime()
      val selector = Selector[TRAJ](spatial, temporal, numPartitions)
      val trajRDD = selector.selectTraj(fileName, metadata)
      val speedRDD = trajRDD.map(x => (x.data, (x.entries.dropRight(1) zip
        (x.consecutiveSpatialDistance("greatCircle") zip
          x.entries.map(_.temporal).sliding(2).map(x => x(1).end - x(0).start).toSeq)
          .map(x => x._1 / x._2 * 3.6))
        .filter(_._2 > 120)
      ))
      val res = speedRDD.collect
      trajRDD.unpersist()
      res.take(5).foreach(x => println(x._1, x._2.deep))
      println(s"${temporal.start} Interval extraction ${(nanoTime - t) * 1e-9} s")
    }

    sc.stop()
  }
}
