package experiments

import instances.LineString
import instances.{Duration, Entry, Extent, Point, RTree, Trajectory}
import operatorsNew.converter.{MapMatcher, Traj2SpatialMapConverter, Traj2TrajConverter}
import operatorsNew.selector.Selector
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import utils.Config

import java.text.SimpleDateFormat
import java.lang.System.nanoTime
import scala.reflect.ClassTag

object MapMatching {
  def main(args: Array[String]): Unit = {
   val sparkConf = new SparkConf()
      .setAppName("MapMatching")
     .setMaster(Config.get("master"))
     .registerKryoClasses(Array(classOf[MapMatcher], classOf[Traj2TrajConverter[Option[String]]], classOf[RTree[LineString]]))
     val spark = SparkSession.builder()
      .config(sparkConf).getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")


    val mapFile = args(0)
    val trajDir = args(1)
    val spatial = Extent(args(2).split(",").map(_.toDouble)).toPolygon
    val temporal = Duration(args(3).split(",").map(_.toLong))
    val numPartitions = args(4).toInt
    val res = args(5)

    val t = nanoTime

    type TRAJ = Trajectory[Option[String], String]
    val selector = Selector[TRAJ](spatial, temporal, numPartitions)
    val trajRDD = selector.selectTraj(trajDir).flatMap(split(_))
    println(s"num trajs: ${trajRDD.count()}")
    val l = trajRDD.map(_.entries.length)
    println(s"avg length: ${l.sum()/l.count()}")
    val converter = new Traj2TrajConverter[Option[String]](mapFile)
    val mmRDD = converter.convert(trajRDD)

    val formatter = new SimpleDateFormat("yyyy-MM-dd HH")

    val roadSegs = mmRDD.flatMap { traj =>
      traj.entries.map(entry => (entry.value, formatter.format(entry.temporal.start * 1000))
      ).groupBy(_._1).mapValues(x => x.map(_._2))
    }.reduceByKey((x, y) => x ++ y).mapValues(x => x.map(i => (i, 1)).groupBy(_._1).mapValues(_.length).toArray)
    println(roadSegs.count)
    println(s"road network flow extraction ${(nanoTime - t) * 1e-9} s")
    import spark.implicits._
    val df = roadSegs.flatMap(x => x._2.map(y => (x._1, y._1, y._2))).toDF
    df.repartition(1).write.csv(res)
    sc.stop()
  }

  def split[V: ClassTag](traj:Trajectory[V,String], tSplit: Int = 3600, maxLength: Int = 50): Array[Trajectory[V,String]] = {
    val data = traj.data
    var arr = new Array[Array[Entry[Point, V]]](0)
      traj.entries.foreach{e =>
        if(arr.length ==0 ||
          (e.temporal.start - arr.last.last.temporal.end) > tSplit ||
          arr.last.length == maxLength) arr = arr :+ Array(e)
        else arr = arr.dropRight(1) :+ (arr.last :+ e)
    }
    arr.filter(_.length > 1).zipWithIndex.map(x =>
    Trajectory(x._1, data + "_" + x._2.toString))
  }

}
