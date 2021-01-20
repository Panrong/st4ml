package selection.selector

import org.apache.spark.rdd.RDD

import geometry.Rectangle

abstract class SpatialSelector extends Serializable {
  val queryRange: Rectangle
  val partitionRange: Map[Int, Rectangle]

  def query(): RDD[_]
}


