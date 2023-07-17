package experiments

import st4ml.instances.{Duration, Extent, Point, Trajectory}
import st4ml.operators.selector.SelectionUtils.T
import st4ml.operators.selector.Selector
import org.apache.spark.sql.SparkSession
import st4ml.utils.Config

import java.lang.System.nanoTime
import scala.io.Source

object TrajSpeedExtraction {
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val metadata = args(1)
    val queryFile = args(2)
    val numPartitions = args(3).toInt
    val spark = SparkSession.builder()
      .appName("trajRangeQuery")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    // read queries
    val f = Source.fromFile(queryFile)
    val ranges = f.getLines().toArray.map(line => {
      val r = line.split(" ")
      (Extent(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble).toPolygon, Duration(r(4).toLong, r(5).toLong))
    })
    val t = nanoTime()
    type TRAJ = Trajectory[Option[String], String]

    for ((spatial, temporal) <- ranges) {
      val selector = Selector[TRAJ](spatial, temporal, numPartitions)
      val trajRDD = selector.selectTraj(fileName, metadata, true)
      val speedRDD = trajRDD.map(x => (x.data, x.consecutiveSpatialDistance("greatCircle").sum / x.duration.seconds * 3.6))
      val res = speedRDD.collect
      trajRDD.unpersist()
      println(res.sortBy(_._2).take(5).deep)
    }
    println(s"Stay point extraction ${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }
}
