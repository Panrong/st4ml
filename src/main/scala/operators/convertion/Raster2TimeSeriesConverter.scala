package operators.convertion

import geometry.{Raster, TimeSeries}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class Raster2TimeSeriesConverter[T: ClassTag] extends Converter {
  override type I = Raster[T]
  override type O = TimeSeries[T]

  // convert the raster elements inside one partition to one time series
  override def convert(rdd: RDD[(Int, Raster[T])]): RDD[TimeSeries[T]] = rdd.map(_._2).mapPartitions(
    iter => {
      val rasters = iter.toArray
      val combinedRaster = rasters.head.aggregateSpatial(rasters.drop(1))
      val contents = combinedRaster.contents.flatMap(x => x.series).flatten
      val temporalRange = combinedRaster.temporalRange()
      Iterator(TimeSeries(id = combinedRaster.id,
        startTime = temporalRange._1,
        timeInterval = (temporalRange._2 - temporalRange._1).toInt,
        spatialRange = combinedRaster.spatialRange(), series = Array(contents)))
    })
}
