package operators.selection.selectionHandler

import geometry.{Rectangle, Shape}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

abstract class SpatialHandler extends Serializable {
  val partitionRange: Map[Int, Rectangle]
  def query[T <: Shape : ClassTag](dataRDD: RDD[(Int, T)])(queryRange: Rectangle): RDD[(Int, T)]
}


