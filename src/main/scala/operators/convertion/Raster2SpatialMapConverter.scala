package operators.convertion

import geometry.{Raster, SpatialMap}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class Raster2SpatialMapConverter extends Converter{
  // convert the raster elements inside one partition to one spatial map
  def convert[I: ClassTag, T: ClassTag](rdd: RDD[(Int, Raster[T])]): RDD[SpatialMap[T]] = {
    rdd.map(_._2).mapPartitions(iter => {
      val rasters = iter.toArray
      val combinedRaster = rasters.head.aggregateTemporal(rasters.drop(1))
      val contents = combinedRaster.contents.flatMap(x => x.series).flatten
      Iterator(SpatialMap(id = combinedRaster.id,
        timeStamp = combinedRaster.temporalRange(),
        contents = Array((combinedRaster.spatialRange(), contents))))
    })
  }
}
