package operators.selection.selectionHandler

import geometry.{Rectangle, Shape}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


class FilterHandler(override val partitionRange: Map[Int, Rectangle]) extends SpatialHandler {

  override def query[T <: Shape : ClassTag](dataRDD: RDD[T])
                                           (queryRange: Rectangle): RDD[T] = {

    dataRDD.zipWithIndex.filter(x => x._1.intersect(queryRange)
      && queryRange.referencePoint(x._1).get.inside(partitionRange(x._2.toInt))
    ).map(_._1)
  }
}