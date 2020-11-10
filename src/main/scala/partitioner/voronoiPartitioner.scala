package main.scala.partitioner

import main.scala.geometry.{Point, Shape}
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.{RDD, ShuffledRDD}

import scala.reflect.ClassTag

object voronoiPartitioner {
  /**
   * QuadTree partitioner
   *
   * @param r           : input RDD
   * @param pivotPoints : Array of pivot points, the voronoi partitioner assign each object to its nearest pivot point
   * @tparam T : type extends Shape
   * @return partitioned RDD
   */
  def apply[T <: Shape : ClassTag](r: RDD[T], pivotPoints: Array[Point]): RDD[T] = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()
    val pivotMap = pivotPoints.zipWithIndex.toMap
    val pivotRDD = sc.parallelize(pivotPoints)
    val numPartitions = pivotPoints.length
    val partitionedRDD = r.cartesian(pivotRDD)
      .map { case (s, p) => (s, (pivotMap(p), s.dist(p))) }
      .groupByKey
      .map { case (k, v) => (k, v.minBy(_._2)) }
      .map { case (k, v) => (v._1, k) }
    implicit val partitioner: voronoiPartitioner[T] = new voronoiPartitioner(numPartitions)
    new ShuffledRDD[Int, T, T](partitionedRDD, partitioner).map(x => x._2)
  }
}

class voronoiPartitioner[T <: Shape : ClassTag](num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}