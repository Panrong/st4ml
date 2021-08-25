package experiments

import experiments.TrajConversionTest.T
import instances.{Duration, Extent, Point, Trajectory}
import operatorsNew.selector.partitioner.HashPartitioner
import operatorsNew.selector.DefaultSelector
import org.apache.spark.sql.SparkSession
import utils.Config

import java.lang.System.nanoTime
import scala.io.Source

object TrajRangeQuery {
  def main(args: Array[String]): Unit = {
    val fileName = args(0)
    val queryFile = args(1)
    val numPartitions = args(2).toInt
    // read queries
    val f = Source.fromFile(queryFile)
    val queries = f.getLines().toArray.map(line => {
      val r = line.split(" ")
      (Extent(r(0).toDouble, r(1).toDouble, r(2).toDouble, r(3).toDouble),
        Duration(r(4).toLong, r(5).toLong))
    })
    val spark = SparkSession.builder()
      .appName("flowExp")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val t = nanoTime
    // read trajectories
    val readDs = spark.read.parquet(fileName)
    import spark.implicits._
    val trajRDD = readDs.as[T].rdd.map(x => {
      val entries = x.entries.map(p => (Point(p.lon, p.lat), Duration(p.t), None))
      Trajectory(entries, x.id)
    })
    trajRDD.cache()
    val partitionedRDD = new HashPartitioner(numPartitions).partition(trajRDD)
    partitionedRDD.cache
    for ((s, t) <- queries) {
      val sRDD = partitionedRDD.filter(_.intersects(s, t))
      println(sRDD.count)
    }
    println((nanoTime - t) * 1e-9)

    sc.stop()
  }
}
