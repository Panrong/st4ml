package operators.convertion

import geometry.{Raster, SpatialMap}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class Raster2SpatialMapConverter[T: ClassTag] extends Converter {
  // convert the raster elements inside one partition to one spatial map
  override type I = Raster[T]
  override type O = SpatialMap[T]

  override def convert(rdd: RDD[Raster[T]]): RDD[SpatialMap[T]] = {
    rdd.mapPartitions(iter => {
      val rasters = iter.toArray
      val combinedRaster = rasters.head.aggregateTemporal(rasters.drop(1))
      val contents = combinedRaster.contents.flatMap(x => x.series).flatten
      Iterator(SpatialMap(id = combinedRaster.id,
        timeStamp = combinedRaster.temporalRange(),
        contents = Array((combinedRaster.spatialRange(), contents))))
    })
  }
}
