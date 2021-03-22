package geometry

import scala.reflect.ClassTag

case class TimeSeries[T: ClassTag](id: String,
                                   startTime: Long,
                                   timeInterval: Int,
                                   spatialRange: Rectangle,
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

  def temporalRange: (Long, Long) = (startTime, endTime)

  // the extend and split operations are both implemented on temporal dimension
  def extend(ts: TimeSeries[T]): TimeSeries[T] = {
    //extend the TimeSeries and keep the original id
    // requires no overlapping nor discontinuity
    assert(this.timeInterval == ts.timeInterval && this.endTime == ts.startTime,
      "the extension is invalid")
    TimeSeries[T](id, startTime, timeInterval, spatialRange, this.series ++ ts.series)
  }

  def split(num: Int): Array[TimeSeries[T]] = {
    val subSeriesLength = series.length / num + 1
    splitByInterval(subSeriesLength)
  }

  def splitByInterval(interval: Int): Array[TimeSeries[T]] = {
    series.sliding(interval, interval).zipWithIndex.map {
      case (series, id) =>
        TimeSeries(id = this.id + "-" + id.toString,
          startTime = this.startTime + id * this.timeInterval,
          timeInterval = this.timeInterval,
          spatialRange = this.spatialRange,
          series = series)
    }.toArray
  }
}
