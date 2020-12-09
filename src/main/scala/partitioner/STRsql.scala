//package partitioner

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{Partitioner, SparkConf, SparkContext, SparkEnv, sql}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.MutablePair

object STRsql extends App {
  override def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf()
    //    conf.setAppName("SQL-STR").setMaster("local")
    //    val sc = new SparkContext(conf)
    //    sc.setLogLevel("ERROR")
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SQL-STR")
      .getOrCreate()

    import spark.implicits._

    val points = genRandomPoints(10000)
    val pointsDF = points.toDF()
    pointsDF.show(5)
    val partitionedDF = HashPartition(pointsDF.rdd.map(row => (row(0), row)), 10)
    println(partitionedDF.collect.deep)
    spark.stop()
  }

  def genRandomPoints(num: Int): Seq[Point] = {
    var points = new Array[Point](0)
    val r = scala.util.Random
    for (i <- 0 until num) points = points :+ Point(i, r.nextDouble() * 100, r.nextDouble() * 100)
    points.toSeq
  }

  object HashPartition {
    def apply(origin: RDD[(Any, Row)], num_partitions: Int): RDD[(Any, Row)] = {
      val rdd = {
        origin.mapPartitions { iter =>
          val mutablePair = new MutablePair[Any, Row]()
          iter.map(row => mutablePair.update(row._1, row._2.copy()))
        }
      }

      val part = new HashPartitioner(num_partitions)
      new ShuffledRDD[Any, Row, Row](rdd, part)
    }
  }

  class HashPartitioner(num_partitions: Int) extends Partitioner {
    override def numPartitions: Int = num_partitions

    override def getPartition(key: Any): Int = {
      key.hashCode() % num_partitions
    }
  }

}

case class Point(id: Long = 0, lon: Double, lat: Double, attribute: String = "")