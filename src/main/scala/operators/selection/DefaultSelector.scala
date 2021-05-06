package operators.selection

import geometry.{Rectangle, Shape}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class DefaultSelector(sQuery: Rectangle, tQuery: (Long, Long)) extends Selector {
  override def query[R <: Shape : ClassTag](dataRDD: RDD[R]): RDD[R] = {
    dataRDD.filter {
      o => o.intersect(sQuery) && o.temporalOverlap(tQuery)
    }
  }
}
