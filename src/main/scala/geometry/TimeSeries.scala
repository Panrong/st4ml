package geometry

import scala.reflect.ClassTag

case class TimeSeries[T: ClassTag](id: String,
                                   startTime: Long,
                                   timeInterval: Int,
                                   series: Array[Array[T]]) {
  def toMap: Map[(Long, Long), Array[T]] = {
    series.zipWithIndex.map {
      case (content, idx) =>
        ((idx * timeInterval + startTime,
          (idx + 1) * timeInterval + startTime),
          content)
    }.toMap
  }

  def endTime: Long = startTime + timeInterval * series.length

  def extend(ts: TimeSeries[T]): TimeSeries[T] = {
    //extend the TimeSeries and keep the original id
    assert(this.timeInterval == ts.timeInterval && this.endTime == ts.startTime,
      "the extension is invalid")
    TimeSeries[T](id, startTime, timeInterval, this.series ++ ts.series)
  }

  def split(num: Int): Array[TimeSeries[T]] = {
    val subSeriesLength = series.length / num + 1
    series.sliding(subSeriesLength, subSeriesLength).zipWithIndex.map {
      case (series, id) =>
        TimeSeries(id = this.id + "-" + id.toString,
          startTime = this.startTime + id * this.timeInterval,
          timeInterval = this.timeInterval,
          series = series)
    }.toArray
  }
}
