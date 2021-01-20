package selection.selector

import geometry.{Rectangle, Shape}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag



abstract class SpatialSelector extends Serializable {
  val queryRange: Rectangle
  val partitionRange: Map[Int, Rectangle]
  def query[T <: Shape : ClassTag](dataRDD: RDD[(Int, T)]): RDD[(Int, T)]
}


