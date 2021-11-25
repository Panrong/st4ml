package experiments

import instances.{Duration, Point, Trajectory}
import operatorsNew.selector.SelectionUtils.T
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime

object TrajSpeedExtraction {
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val numPartitions = args(1).toInt
    val spark = SparkSession.builder()
      .appName("trajRangeQuery")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    // read trajectories
    import spark.implicits._
    val trajRDD = spark.read.parquet(fileName).drop("pId").as[T]
      .toRdd.map(_.asInstanceOf[Trajectory[None.type, String]])
    println(trajRDD.count)
    trajRDD.cache()

    val t = nanoTime

    val speedRDD = trajRDD.map(x => (x.data, x.consecutiveSpatialDistance("greatCircle").sum / x.duration.seconds * 3.6))
    val res = speedRDD.collect
    println(s"Avg speed extraction ${(nanoTime - t) * 1e-9} s")
    println(res.take(5).deep)

    sc.stop()

  }
}
