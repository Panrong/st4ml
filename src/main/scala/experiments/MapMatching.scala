package experiments

import instances.{Duration, Extent, Trajectory}
import operatorsNew.converter.{Traj2SpatialMapConverter, Traj2TrajConverter}
import operatorsNew.selector.Selector
import org.apache.spark.sql.SparkSession
import utils.Config
import java.text.SimpleDateFormat

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
    val trajRDD = selector.selectTraj(trajDir) //.sample(false, 0.02)
    val converter = new Traj2TrajConverter[Option[String]](mapFile)
    val mmRDD = converter.convert(trajRDD)

    val formatter = new SimpleDateFormat("yyyy-MM-dd hh")

    val roadSegs = mmRDD.flatMap { traj =>
      traj.entries.map(entry => (entry.value, formatter.format(entry.temporal.start * 1000))
      ).groupBy(_._1).mapValues(x => x.map(_._2))
    }.reduceByKey((x, y) => x ++ y).mapValues(x => x.map(i => (i, 1)).groupBy(_._1).mapValues(_.length).toArray)
    println(roadSegs.count)
    println(s"road network flow extraction ${(nanoTime - t) * 1e-9} s")
    import spark.implicits._
    val df = roadSegs.flatMap(x => x._2.map(y => (x._1, y._1, y._2))).toDF
    df.repartition(1).write.csv("datasets/roadseg")
    sc.stop()
  }

}
