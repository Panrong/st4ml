package operators.convertion

import geometry.{SpatialMap, TimeSeries}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class TimeSeries2SpatialMapConverter {
  //one time series contains several temporal slots, each slot converts to a spatial map (of one spatial region)
  def convert[T: ClassTag](rdd: RDD[(Int, TimeSeries[T])]): RDD[SpatialMap[T]] = {
    rdd.map(_._2).map(ts => {
      val slots = ts.series
      val timeStamp = (ts.startTime, ts.endTime)
      val spatialRange = ts.spatialRange
      slots.zipWithIndex.map {
        case (contents, id) => SpatialMap(id.toString, timeStamp, Array((spatialRange, contents)))
      }
    }).flatMap(x => x)
  }
}
