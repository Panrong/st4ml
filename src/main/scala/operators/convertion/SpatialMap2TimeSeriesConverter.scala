package operators.convertion

import geometry.{SpatialMap, TimeSeries}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SpatialMap2TimeSeriesConverter[T: ClassTag] extends Converter {

  override type I = SpatialMap[T]
  override type O = TimeSeries[T]

  //one spatial contains several spatial regions, each region converts to a time series (of one temporal slot)
  def convert(rdd: RDD[SpatialMap[T]]): RDD[TimeSeries[T]] = {
    rdd.map(sm => {
      val regions = sm.contents
      val startTime = sm.startTime
      val timeInterval = (sm.endTime - sm.startTime).toInt
      regions.zipWithIndex.map {
        case ((spatialRange, contents), id) => TimeSeries(id.toString, startTime, timeInterval, spatialRange, Array(contents))
      }
    }).flatMap(x => x)
  }
}
