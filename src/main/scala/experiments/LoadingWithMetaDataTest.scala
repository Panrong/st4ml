package experiments

import instances.{Duration, Extent, Point, Trajectory}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
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
    val partitionNum = mutable.Map[Int, Int]()
    val f = Source.fromFile(metadata)
    for (line <- f.getLines) {
      val x = line.split(" ").map(_.toDouble)
      partitionInfo(x(0).toInt) = Extent(x(1), x(2), x(3), x(4))
      partitionNum(x(0).toInt) = x(5).toInt
    }

    val sRange = Extent(range(0), range(1), range(2), range(3))
    val relatedPartitions = partitionInfo.filter(x => x._2.intersects(sRange)
      && partitionNum(x._1) > 0).keys.toArray
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
    t = nanoTime()
    val readDs2 = spark.read.parquet(fileName)

    val trajRDD2 = readDs2.as[T].rdd.map(x => {
      val entries = x.entries.map(p => (Point(p.lon, p.lat), Duration(p.t), None))
      Trajectory(entries, x.id)
    })
      .repartition(numPartitions)
      .filter(_.intersects(sRange))

    println(trajRDD2.count)
    println(s"Loading without metadata ${(nanoTime - t) * 1e-9} s")

    /** test selecting with metadata */
    t = nanoTime()
    // recompute to record time
    val partitionInfo2 = mutable.Map[Int, Extent]()
    val partitionNum2 = mutable.Map[Int, Int]()
    val f2 = Source.fromFile(metadata)
    for (line <- f2.getLines) {
      val x = line.split(" ").map(_.toDouble)
      partitionInfo2(x(0).toInt) = Extent(x(1), x(2), x(3), x(4))
      partitionNum2(x(0).toInt) = x(5).toInt
    }

    val sRange2 = Extent(range(0), range(1), range(2), range(3))
    val relatedPartitions2 = partitionInfo2.filter(x => x._2.intersects(sRange2)
      && partitionNum2(x._1) > 0).keys.toArray

    val readDs3 = spark.read.parquet(fileName).filter(col("pId").isin(relatedPartitions2: _*))

    val trajRDD3 = readDs3.as[T].rdd.map(x => {
      val entries = x.entries.map(p => (Point(p.lon, p.lat), Duration(p.t), None))
      Trajectory(entries, x.id)
    })
      .repartition(numPartitions)
      .filter(_.intersects(sRange))

    println(trajRDD3.count)
    println(s"Loading without metadata ${(nanoTime - t) * 1e-9} s")

    sc.stop()
  }
}
