package geometry

import scala.reflect.ClassTag

case class TimeSeries[T: ClassTag]
(id: String, startTime: Long, timeInterval: Int, series: Array[Array[T]]) {
  def toMap: Map[(Long, Long), Array[T]] = {
    series.zipWithIndex.map {
      case (content, idx) =>
        ((idx * timeInterval + startTime, (idx + 1) * timeInterval + startTime), content)
    }.toMap
  }

  def endTime: Long = startTime + timeInterval * series.length
}
