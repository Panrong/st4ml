package selection.partitioner

import geometry.Rectangle
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

abstract class SpatialPartitioner {
  var partitionRange: Map[Int, Rectangle]

  def partition[T <: geometry.Shape : ClassTag](dataRDD: RDD[T]): RDD[(Int, T)]
}
