package main.scala.partitioner

import main.scala.geometry.{Point, Rectangle, Shape}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}

import scala.math.{abs, max, min}
import scala.reflect.ClassTag

object gridPartitioner {
  /**
   * QuadTree partitioner
   * @param r : input RDD
   * @param numPartition : number of partitions, has to be to 3k+1 (otherwise is rounded)
   * @param samplingRate : sample some data to determine the boundary, less than 1
   * @tparam T : type extends Shape
   * @return partitioned RDD and a map of  PartitionNum -> boundary
   */
  def apply[T <: Shape : ClassTag](r: RDD[T], numPartition: Int,
                                   samplingRate: Double): (RDD[T], Map[Int, Rectangle]) = {

    // determine the number of divisions per latitude and longitude
    val sampledRDD = r.sample(withReplacement = false, samplingRate)
    val latMin = sampledRDD.map(x => x.center().lat).min
    val latMax = sampledRDD.map(x => x.center().lat).max
    val lonMin = sampledRDD.map(x => x.center().lon).min
    val lonMax = sampledRDD.map(x => x.center().lon).max
    val latLonRatio = (latMax - latMin) / (lonMax - lonMin)
    val wholeRange = Rectangle(Point(latMin, lonMin), Point(latMax, lonMax))
    // select the decomposition way that has the most similar ratio to latLonRatio
    val possibleDecompositions = decompose(numPartition)
    val bestDecomposition = possibleDecompositions.map(x => (x, abs(x._1 / x._2.toDouble - latLonRatio)))
      .sortBy(x => x._2)
      .head._1 //(latNum, lonNum)
    val partitionBounds = getPartitionBounds(wholeRange, bestDecomposition)
    val partitioner = new gridPartitioner[T](numPartition, partitionBounds)
    val pRDD = new ShuffledRDD[T, T, T](r.map(x => (x, x)), partitioner)
    (pRDD.map(x => x._1), partitionBounds.map { case (k, v) => (v, k) })
  }

  def decompose(n: Int): Array[(Int, Int)] = {
    var r = Array((1, n))
    for (i <- 2 to n + 1) {
      if (n % i == 0) r = r :+ (i, n / i)
    }
    r
  }

  def getPartitionBounds(wholeRange: Rectangle, decomposition: (Int, Int)): Map[Rectangle, Int] = {
    val latLength = (wholeRange.x_max - wholeRange.x_min) / decomposition._1
    val lonLength = (wholeRange.y_max - wholeRange.y_min) / decomposition._2
    val lats = Range.BigDecimal(wholeRange.x_min, wholeRange.x_max, latLength)
      .map(_.toDouble).toArray.dropRight(1)
    val lons = Range.BigDecimal(wholeRange.y_min, wholeRange.y_max, lonLength)
      .map(_.toDouble).toArray.dropRight(1)
    val a = lats.flatMap(lat => lons.map(lon => (lat, lon))).map(x =>
      Rectangle(Point(x._1, x._2), Point(x._1 + latLength, x._2 + lonLength)))
      .zipWithIndex.toMap
    a
  }
}

class gridPartitioner[T <: Shape : ClassTag](num: Int, partitionBounds: Map[Rectangle, Int]) extends Partitioner {
  override def numPartitions: Int = num

  val minLon: Double = partitionBounds.keys.map(_.x_min).toArray.min
  val minLat: Double = partitionBounds.keys.map(_.y_min).toArray.min
  val maxLon: Double = partitionBounds.keys.map(_.x_max).toArray.max
  val maxLat: Double = partitionBounds.keys.map(_.y_max).toArray.max

  override def getPartition(key: Any): Int = {
    val c = key.asInstanceOf[Shape].center()
    val k = Point(max(min(c.lon, maxLon), minLon), max(min(c.lat, maxLat), minLat))
    val res = partitionBounds.filterKeys(k.inside).values.toArray
    res(0)
  }
}
