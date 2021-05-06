package operators.convertion

import geometry.{Raster, Shape, SpatialMap, TimeSeries}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SpatialMap2RasterConverter[T <: Shape : ClassTag] extends Converter {
  override type I = SpatialMap[T]
  override type O = Raster[T]

  override def convert(rdd: RDD[SpatialMap[T]]): RDD[Raster[T]] = {
    rdd.mapPartitions(iter => {
      val sm = iter.toArray.head
      val subSpatialMaps = sm.splitByCapacity(1)
      subSpatialMaps.zipWithIndex.map {
        case (x, id) =>
          val ts = TimeSeries(id.toString,
            startTime = x.startTime,
            timeInterval = (x.endTime - x.startTime).toInt,
            spatialRange = x.contents.head._1,
            series = Array(x.contents.flatMap(_._2)))
          Raster(id = sm.id + "-" + id.toString, Array(ts))
      }.toIterator
    })
  }
}
