package operators.convertion

import geometry.{SpatialMap, TimeSeries}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class TimeSeries2SpatialMapConverter[T: ClassTag] extends Converter {

  override type I = TimeSeries[T]
  override type O = SpatialMap[T]

  //one time series contains several temporal slots, each slot converts to a spatial map (of one spatial region)
  override def convert(rdd: RDD[TimeSeries[T]]): RDD[SpatialMap[T]] = {
    rdd.map(ts => {
      val slots = ts.series
      val timeStamp = (ts.startTime, ts.endTime)
      val spatialRange = ts.spatialRange
      slots.zipWithIndex.map {
        case (contents, id) => SpatialMap(id.toString, timeStamp, Array((spatialRange, contents)))
      }
    }).flatMap(x => x)
  }
}
