package experiments

import instances.{Duration, Extent, Point, Trajectory}
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime
import scala.collection.mutable
import scala.io._

object LoadingWithMetaDataTest {
  case class E(lon: Double, lat: Double, t: Long)

  case class T(id: String, entries: Array[E])

  case class TwP(id: String, entries: Array[E], pId: Int)

  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val numPartitions = args(1).toInt
    val metadata = args(2)
    val range = args(3).split(",").map(_.toDouble) // -8.446832 41.010165 -7.932837 41.381359

    val spark = SparkSession.builder()
      .appName("LoadingWithMetaDataTest")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    var t = nanoTime()

    /** test loading with metadata */
    // parse metadata
    val partitionInfo = mutable.Map[Int, Extent]()
    val f = Source.fromFile(metadata)
    for (line <- f.getLines) {
      val x = line.split(" ").map(_.toDouble)
      partitionInfo(x(0).toInt) = Extent(x(1), x(2), x(3), x(4))
    }

    val sRange = Extent(range(0), range(1), range(2), range(3))
    val relatedPartitions = partitionInfo.filter(_._2.intersects(sRange)).keys.toArray
    val dirs = relatedPartitions.map(x => fileName + s"/pId=$x")
    val readDs = spark.read.parquet(dirs: _*)

    import spark.implicits._
    val trajRDD = readDs.as[T].rdd.map(x => {
      val entries = x.entries.map(p => (Point(p.lon, p.lat), Duration(p.t), None))
      Trajectory(entries, x.id)
    })
      .repartition(numPartitions)
      .filter(_.intersects(sRange))

    println(trajRDD.count)
    println(s"Loading with metadata ${(nanoTime - t) * 1e-9} s")

    /** test loading without metadata */
    val readDs2 = spark.read.parquet(fileName)

    val trajRDD2 = readDs2.as[T].rdd.map(x => {
      val entries = x.entries.map(p => (Point(p.lon, p.lat), Duration(p.t), None))
      Trajectory(entries, x.id)
    })
      .repartition(numPartitions)
      .filter(_.intersects(sRange))

    println(trajRDD2.count)
    println(s"Loading without metadata ${(nanoTime - t) * 1e-9} s")
    sc.stop()
  }
}
