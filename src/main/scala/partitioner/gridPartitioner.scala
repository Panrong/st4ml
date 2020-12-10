package partitioner

import geometry.{Point, Rectangle, Shape}
import org.apache.spark.Partitioner
import org.apache.spark.rdd.{RDD, ShuffledRDD}

import scala.math.{abs, max, min}
import scala.reflect.ClassTag

object gridPartitioner {
  /**
   * QuadTree partitioner
   *
   * @param r            : input RDD
   * @param numPartition : number of partitions, has to be to 3k+1 (otherwise is rounded)
   * @param samplingRate : sample some data to determine the boundary, less than 1
   * @tparam T : type extends Shape
   * @return partitioned RDD and a map of  PartitionNum -> boundary
   */
  def apply[T <: Shape : ClassTag](r: RDD[T], numPartition: Int,
                                   samplingRate: Double): (RDD[T], Map[Int, Rectangle]) = {

    if (isPrime(numPartition)) println("=== Warning: the number of partitions is a prime number.")

    /** determine the number of divisions per latitude and longitude */
    val sampledRDD = r.sample(withReplacement = false, samplingRate)
    val lonMin = sampledRDD.map(x => x.mbr.coordinates(0)).min.formatted("%.4f").toDouble
    val lonMax = sampledRDD.map(x => x.mbr.coordinates(2)).max.formatted("%.4f").toDouble
    val latMin = sampledRDD.map(x => x.mbr.coordinates(1)).min.formatted("%.4f").toDouble
    val latMax = sampledRDD.map(x => x.mbr.coordinates(3)).max.formatted("%.4f").toDouble
    val lonLatRatio = (lonMax - lonMin) / (latMax - latMin)
    val wholeRange = Rectangle(Array(lonMin, latMin, lonMax, latMax))

    /** select the decomposition way that has the most similar ratio to latLonRatio */
    val possibleDecompositions = decompose(numPartition)
    val bestDecomposition = possibleDecompositions
      .map(x => (x, abs(x._1 / x._2.toDouble - lonLatRatio)))
      .minBy(x => x._2)._1 //(lonNum, latNum)
    val partitionBounds = getPartitionBounds(wholeRange, bestDecomposition)
    val partitioner = new gridPartitioner[T](numPartition, partitionBounds)
    val pRDD = new ShuffledRDD[T, T, T](r.map(x => (x, x)), partitioner)
    (pRDD.map(x => x._1), partitionBounds.map { case (k, v) => (v, k) })
  }

  /** decompose an integer x to two y and z such that x = yz */
  def decompose(n: Int): Array[(Int, Int)] = {
    var r = Array((1, n))
    for (i <- 2 to n + 1) {
      if (n % i == 0) r = r :+ (i, n / i)
    }
    r
  }

  def getPartitionBounds(wholeRange: Rectangle, decomposition: (Int, Int)): Map[Rectangle, Int] = {
    val lonLength = (wholeRange.xMax - wholeRange.xMin) / decomposition._1
    val latLength = (wholeRange.yMax - wholeRange.yMin) / decomposition._2
    val lons = Range.BigDecimal(wholeRange.xMin, wholeRange.xMax, lonLength)
      .map(_.toDouble).toArray.take(decomposition._1)
    val lats = Range.BigDecimal(wholeRange.yMin, wholeRange.yMax, latLength)
      .map(_.toDouble).toArray.take(decomposition._2)
    val a = lons.flatMap(lon => lats.map(lat => (lon, lat)))
    a.map(x =>
      Rectangle(Array(x._1, x._2, x._1 + lonLength, x._2 + latLength)))
      .zipWithIndex.toMap
  }

  def isPrime(num: Int): Boolean =
    (num > 1) && !(2 to scala.math.sqrt(num).toInt).exists(x => num % x == 0)
}

class gridPartitioner[T <: Shape : ClassTag](num: Int, partitionBounds: Map[Rectangle, Int]) extends Partitioner {
  override def numPartitions: Int = num

  val minLon: Double = partitionBounds.keys.map(_.xMin).toArray.min
  val minLat: Double = partitionBounds.keys.map(_.yMin).toArray.min
  val maxLon: Double = partitionBounds.keys.map(_.xMax).toArray.max
  val maxLat: Double = partitionBounds.keys.map(_.yMax).toArray.max

  override def getPartition(key: Any): Int = {
    val c = key.asInstanceOf[Shape].center()
    val k = Point(Array(max(min(c.lon, maxLon), minLon), max(min(c.lat, maxLat), minLat)))
    val res = partitionBounds.filterKeys(k.inside).values.toArray
    res(0)
  }
}
