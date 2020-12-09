package partitioner

import geometry.{Shape, Point}
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.SparkSession
import org.apache.spark.{Partitioner, SparkContext}

import scala.reflect.ClassTag

object voronoiPartitioner {
  /**
   * QuadTree partitioner
   *
   * @param r          : input RDD
   * @param num        : number of pivot points
   * @param sampleRate : sampling rate to generate pivot points
   * @tparam T : type extends Shape
   * @return partitioned RDD
   */
  def apply[T <: Shape : ClassTag](r: RDD[T], num: Int, sampleRate: Double): (RDD[T], Map[Point, Int], Map[Point, Double]) = {
    implicit val sc: SparkContext = SparkContext.getOrCreate()
    val pivotPoints = findPivot(r, sampleRate, num)
    val pivotMap = pivotPoints.zipWithIndex.toMap
    val inversePivotMap = pivotMap.map { case (k, v) => (v, k) }
    val pivotRDD = sc.parallelize(pivotPoints)
    val numPartitions = pivotPoints.length
    val partitionedRDD = r.cartesian(pivotRDD)
      .map { case (s, p) => (s, (pivotMap(p), s.geoDistance(p))) }
      .groupByKey
      .map { case (k, v) => (k, v.minBy(_._2)) }
      .map { case (k, v) => (v._1, k) }
    val pivotMaxDistMap = partitionedRDD.map {
      case (idx, v) => (idx, v.geoDistance(inversePivotMap(idx)))
    }.groupByKey.map(x => (inversePivotMap(x._1), x._2.max)).collect.toMap
    implicit val partitioner: voronoiPartitioner[T] = new voronoiPartitioner(numPartitions)
    val resRDD = new ShuffledRDD[Int, T, T](partitionedRDD, partitioner).map(x => x._2)
    (resRDD, pivotMap, pivotMaxDistMap)
  }

  def findPivot[T <: Shape : ClassTag](rdd: RDD[T], sampleRate: Double, num: Int): Array[Point] = {
    val sampledRDD = rdd.sample(withReplacement = false, sampleRate).map(x => Seq(x.center().lat, x.center().lon))
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val kmeans = new KMeans().setK(num).setSeed(1L)
    val model = kmeans.fit(sampledRDD.toDF("features"))
    model.clusterCenters.map(x => Point(Array(x(0), x(1))))
  }
}

class voronoiPartitioner[T <: Shape : ClassTag](num: Int) extends Partitioner {
  override def numPartitions: Int = num

  override def getPartition(key: Any): Int = key.asInstanceOf[Int]

}