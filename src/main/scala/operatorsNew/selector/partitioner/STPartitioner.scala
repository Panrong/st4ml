package operatorsNew.selector.partitioner

import instances.{Duration, Extent, Geometry, Instance}
import org.apache.spark.rdd.RDD

import scala.math.{max, min}
import scala.reflect.ClassTag

abstract class STPartitioner extends Serializable {
  val numPartitions: Int
  var samplingRate: Option[Double]

  def partition[T <: Instance[_<:Geometry, _, _] : ClassTag](dataRDD: RDD[T]): RDD[T]

  def partitionWDup[T <: Instance[_, _, _] : ClassTag](dataRDD: RDD[T]): RDD[T]

  def calSpatialRanges[T <: Instance[_, _, _]](rdd: RDD[(Int, T)]): Map[Int, Extent] = {
    rdd.mapPartitionsWithIndex {
      case (id, iter) =>
        var xMin = 180.0
        var yMin = 90.0
        var xMax = -180.0
        var yMax = -90.0
        while (iter.hasNext) {
          val mbr = iter.next()._2.extent
          if (mbr.xMin < xMin) xMin = mbr.xMin
          if (mbr.xMax > xMax) xMax = mbr.xMax
          if (mbr.yMin < yMin) yMin = mbr.yMin
          if (mbr.yMax > yMax) yMax = mbr.yMax
        }
        Iterator((id, new Extent(xMin, yMin, xMax, yMax)))
    }.collect().toMap
  }

  def calSTRanges[T <: Instance[_, _, _]](rdd: RDD[(Int, T)]): Array[(Int, Extent, Duration, Int)] = {
    rdd.mapPartitionsWithIndex {
      case (id, iter) =>
        var xMin = 180.0
        var yMin = 90.0
        var xMax = -180.0
        var yMax = -90.0
        var tMin = 10000000000L
        var tMax = 0L
        var count = 0
        while (iter.hasNext) {
          val next = iter.next()._2
          val mbr = next.extent
          if (mbr.xMin < xMin) xMin = mbr.xMin
          if (mbr.xMax > xMax) xMax = mbr.xMax
          if (mbr.yMin < yMin) yMin = mbr.yMin
          if (mbr.yMax > yMax) yMax = mbr.yMax
          if (next.duration.start < tMin) tMin = next.duration.start
          if (next.duration.end > tMax) tMax = next.duration.end
          count += 1
        }
        Iterator((id, new Extent(xMin, yMin, xMax, yMax), Duration(tMin, tMax), count))
    }.collect
  }

  def getSamplingRate[T <: Instance[_, _, _]](dataRDD: RDD[T]): Double = {
    val dataSize = dataRDD.count
    max(min(1000 / dataSize.toDouble, 0.5), 100 * numPartitions / dataSize.toDouble)
  }
}
