package geometry

import scala.reflect.ClassTag

case class TimeSeries[T: ClassTag]
(id: String, startTime: Long, timeInterval: Int, series: Array[T]) {
  def toMap: Map[Long, T] = {
    series.zipWithIndex.map{
      case(content, idx) => (idx * timeInterval + startTime, content)
    }.toMap
  }

}
