package operators.selection.selectionHandler

import geometry.{Rectangle, Shape}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


class FilterHandler(override val partitionRange: Map[Int, Rectangle]) extends SpatialHandler {

  override def query[T <: Shape : ClassTag](dataRDD: RDD[(Int, T)])
                                           (queryRange: Rectangle): RDD[(Int, T)] = {
    val spark = SparkContext.getOrCreate()
    spark.broadcast(queryRange)
    dataRDD
      .filter(x =>
        x._2.intersect(queryRange)
          && queryRange.referencePoint(x._2).get.inside(partitionRange(x._1))
      ) // filter by reference point
  }
}