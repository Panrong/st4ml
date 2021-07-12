package operatorsNew.Partitioner

import instances.{Extent, Instance}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

abstract class SpatialPartitioner extends Serializable {
  var partitionRange: Map[Int, Extent]
  var samplingRate: Option[Double]

  def partition[T <: Instance[_,_,_] : ClassTag](dataRDD: RDD[T]): RDD[T]
  def partitionWDup[T <: Instance[_,_,_] : ClassTag](dataRDD: RDD[T]): RDD[T]

  def calPartitionRanges[T <: Instance[_,_,_]](rdd: RDD[(Int, T)]):
  Map[Int, Extent] = {
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

}
