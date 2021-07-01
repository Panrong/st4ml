package operators.selection.partitioner

import geometry.Rectangle
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

abstract class SpatialPartitioner extends Serializable {
  var partitionRange: Map[Int, Rectangle]
  var samplingRate: Option[Double]

  def partition[T <: geometry.Shape : ClassTag](dataRDD: RDD[T]): RDD[T]

  def calPartitionRanges[T <: geometry.Shape : ClassTag](rdd: RDD[(Int, T)]): Map[Int, Rectangle] = {
    rdd.mapPartitionsWithIndex {
      case (id, iter) =>
        var xMin = 180.0
        var yMin = 90.0
        var xMax = -180.0
        var yMax = -90.0
        while (iter.hasNext) {
          val mbr = iter.next()._2.mbr
          if (mbr.xMin < xMin) xMin = mbr.xMin
          if (mbr.xMax > xMax) xMax = mbr.xMax
          if (mbr.yMin < yMin) yMin = mbr.yMin
          if (mbr.yMax > yMax) yMax = mbr.yMax
        }
        Iterator((id, Rectangle(Array(xMin, yMin, xMax, yMax))))
    }.collect().toMap
  }

}
