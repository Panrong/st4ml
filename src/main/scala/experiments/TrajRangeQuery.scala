package experiments

import experiments.TrajConversionTest.T
import instances.Extent.toPolygon
import instances.{Duration, Extent, Point, Trajectory}
import operatorsNew.selector.partitioner.HashPartitioner
import operatorsNew.selector.{DefaultSelector, MultiSTRangeSelector}
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime
import scala.io.Source

object TrajRangeQuery {
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val queryFile = args(1)
    val numPartitions = args(2).toInt
    val m = args(3)
    val partition = args(4).toBoolean

    // read queries
    val f = Source.fromFile(queryFile)
    val queries = f.getLines().toArray.map(line => {
      val r = line.split(" ")
      (Extent(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble),
        Duration(r(4).toLong, r(5).toLong))
    })
    val spark = SparkSession.builder()
      .appName("trajRangeQuery")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    var t = nanoTime
    // read trajectories
    val readDs = spark.read.parquet(fileName)
    import spark.implicits._
    val trajRDD = readDs.as[T].rdd.map(x => {
      val entries = x.entries.map(p => (Point(p.lon, p.lat), Duration(p.t), None))
      Trajectory(entries, x.id)
    })
    println(trajRDD.count)
    println(s"Data loading ${(nanoTime - t) * 1e-9} s")
    t = nanoTime
    trajRDD.cache()
    if (m == "single") {

      val partitionedRDD = if (partition) new HashPartitioner(numPartitions).partition(trajRDD) else trajRDD
      partitionedRDD.cache
      partitionedRDD.count
      for ((s, t) <- queries) {
        val sRDD = partitionedRDD.filter(_.intersects(s, t)).filter(x => x.toGeometry.intersects(s))
        println(sRDD.count)
      }
      println(s"Single range query ${(nanoTime - t) * 1e-9} s")
    }
    else if (m == "multi") {
      val sQuery = queries.map(x => toPolygon(x._1))
      val tQuery = queries.map(x => x._2)
      val selector = new MultiSTRangeSelector[Trajectory[None.type, String]](sQuery, tQuery, numPartitions, partition)
      val res = selector.queryWithInfo(trajRDD, true).flatMap {
        case (_, qArray) => qArray.map((_, 1))
      }.reduceByKey(_ + _)
      println(res.collect.deep)
      println(s"Multi range query ${(nanoTime - t) * 1e-9} s")

    }
    sc.stop()
  }
}
