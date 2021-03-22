package operators.extraction

import geometry.{Rectangle, Shape, SpatialMap}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SpatialMapExtractor {

  // only filter at raster level
  def rangeQueryCoarse[T: ClassTag](rdd: RDD[SpatialMap[T]],
                                    spatialRange: Rectangle,
                                    temporalRange: (Long, Long)): RDD[T] = {
    rdd.filter(x => {
      x.timeStamp._1 <= temporalRange._2 && x.timeStamp._2 >= temporalRange._1
    })
      .map(sm =>
        sm.contents.filter {
          case (range, _) => range.intersect(spatialRange)
        }.flatMap(_._2))
      .flatMap(x => x)
  }

  //filter at element level
  def rangeQuery[T <: Shape : ClassTag](rdd: RDD[SpatialMap[T]],
                                        spatialRange: Rectangle,
                                        temporalRange: (Long, Long)): RDD[T] = {
    rangeQueryCoarse(rdd, spatialRange, temporalRange).filter(
      x => x.mbr.intersect(spatialRange) &&
        x.timeStamp._1 <= temporalRange._2 && x.timeStamp._2 >= temporalRange._1
    )
  }

}
