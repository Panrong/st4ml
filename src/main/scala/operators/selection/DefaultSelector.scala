package operators.selection

import geometry.{Rectangle, Shape}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class DefaultSelector[R <: Shape](sQuery: Rectangle = Rectangle(Array(-180, -90, 180, 90)),
                                  tQuery: (Long, Long) = (0L, 9999999999L)) extends Selector[R] {
  override def query(dataRDD: RDD[R]): RDD[R] = {
    dataRDD.filter {
      o => o.intersect(sQuery) && o.temporalOverlap(tQuery)
    }
  }
}
