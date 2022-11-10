package experiments

import org.apache.spark.sql.SparkSession
import st4ml.instances.{Duration, Extent}
import st4ml.operators.selector.SelectionUtils.{E, T}
import st4ml.operators.selector.partitioner.{HashPartitioner, TSTRPartitioner}
import st4ml.utils.Config

import scala.Numeric.Implicits._

object LoadBalance {

  def mean[T: Numeric](xs: Iterable[T]): Double = xs.sum.toDouble / xs.size

  def variance[T: Numeric](xs: Iterable[T]): Double = {
    val avg = mean(xs)

    xs.map(_.toDouble).map(a => math.pow(a - avg, 2)).sum / xs.size
  }

  def stdDev[T: Numeric](xs: Iterable[T]): Double = math.sqrt(variance(xs))

  def main(args: Array[String]): Unit = {
    val mode = args(0)
    val fileName = args(1)
    val numPartitions = args(2).toInt
    val samplingRate = args(3).toDouble
    val opt = args(4).toBoolean
    val spark = SparkSession.builder()
      .appName("LoadBalance")
      .master(Config.get("master"))
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    if (mode == "event") {
      import spark.implicits._
      val eventRDD = spark.read.parquet(fileName).as[E].toRdd
      val partitioner = if (opt) new TSTRPartitioner(math.sqrt(numPartitions).toInt, math.sqrt(numPartitions).toInt, samplingRate = Some(samplingRate))
      else new HashPartitioner(numPartitions)
      val partitionedRDD = partitioner.partition(eventRDD)

      val sizes = partitionedRDD.mapPartitions(x => Iterator(x.size)).collect().filter(_ > 0)
      println(sizes.deep)
      println(stdDev(sizes) / mean(sizes))

      val areaRDD = partitionedRDD.mapPartitions { x =>
        val arr = x.toArray
        if (arr.length > 0)
          Iterator((Extent(arr.map(_.extent)), Duration(arr.map(_.duration))))
        else Iterator()
      }
      val area1 = areaRDD.collect().map(x => x._1.area * x._2.seconds).sum
      val area2 = Extent(areaRDD.collect().map(_._1)).area * Duration(areaRDD.collect().map(_._2)).seconds
      println(area1 / area2)
    }

    if (mode == "traj") {
      import spark.implicits._
      spark.read.parquet(fileName).show(2)
      val trajRDD = spark.read.parquet(fileName).as[T].toRdd
      val partitioner = if (opt) new TSTRPartitioner(math.sqrt(numPartitions).toInt, math.sqrt(numPartitions).toInt, samplingRate = Some(samplingRate))
      else new HashPartitioner(numPartitions)
      val partitionedRDD = partitioner.partition(trajRDD)

      val sizes = partitionedRDD.mapPartitions(x => Iterator(x.size)).collect().filter(_ > 0)
      println(sizes.deep)
      println(stdDev(sizes) / mean(sizes))

      val areaRDD = partitionedRDD.mapPartitions { x =>
        val arr = x.toArray
        if (arr.length > 0)
          Iterator((Extent(arr.map(_.extent)), Duration(arr.map(_.duration))))
        else Iterator()
      }
      val area1 = areaRDD.collect().map(x => x._1.area * x._2.seconds).sum
      val area2 = Extent(areaRDD.collect().map(_._1)).area * Duration(areaRDD.collect().map(_._2)).seconds
      println(area1 / area2)
    }

    sc.stop()
  }
}
