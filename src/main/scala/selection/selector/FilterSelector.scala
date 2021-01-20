package selection.selector

import geometry.{Rectangle, Shape}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


class FilterSelector[T <: Shape : ClassTag](dataRDD: RDD[(Int, T)],
                                            override val queryRange: Rectangle,
                                            override val partitionRange: Map[Int, Rectangle])
  extends SpatialSelector {

  override def query(): RDD[(Int, T)] = {
    val spark = SparkContext.getOrCreate()
    spark.broadcast(queryRange)
    dataRDD
      .filter(x =>
        x._2.intersect(queryRange)
          && queryRange.referencePoint(x._2).get.inside(partitionRange(x._1))
      ) // filter by reference point
  }
}