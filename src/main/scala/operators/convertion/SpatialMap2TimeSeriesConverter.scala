package operators.convertion

import geometry.{SpatialMap, TimeSeries}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SpatialMap2TimeSeriesConverter extends Converter{
  //one spatial contains several spatial regions, each region converts to a time series (of one temporal slot)
  def convert[T: ClassTag](rdd: RDD[(Int, SpatialMap[T])]): RDD[TimeSeries[T]] = {
    rdd.map(_._2).map(sm => {
      val regions = sm.contents
      val startTime = sm.startTime
      val timeInterval = (sm.endTime - sm.startTime).toInt
      regions.zipWithIndex.map {
        case ((spatialRange, contents), id) => TimeSeries(id.toString, startTime, timeInterval, spatialRange, Array(contents))
      }
    }).flatMap(x => x)
  }
}
