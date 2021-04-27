package operators.extraction

import geometry.{Rectangle, Shape, SpatialMap}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

class SpatialMapExtractor extends Extractor {

  // only filter at raster level
  def rangeQueryCoarse[T: ClassTag](rdd: RDD[SpatialMap[T]],
                                    spatialRange: Rectangle,
                                    temporalRange: (Long, Long)): RDD[T] = {
    rdd.filter(x => temporalOverlap(x.timeStamp, temporalRange))
      .map(sm =>
        sm.contents.filter {
          case (range, _) => range.intersect(spatialRange)
        }.flatMap(x => x._2))
      .flatMap(x => x)
  }

  //filter at element level
  def rangeQuery[T <: Shape : ClassTag](rdd: RDD[SpatialMap[T]],
                                        spatialRange: Rectangle,
                                        temporalRange: (Long, Long)): RDD[T] = {
    rangeQueryCoarse(rdd, spatialRange, temporalRange).filter(
      x => x.intersect(spatialRange) &&
        temporalOverlap(x.timeStamp, temporalRange)
    )
  }

  def temporalOverlap(t1: (Long, Long), t2: (Long, Long)): Boolean = {
    if (t1._1 >= t2._1 && t1._1 <= t2._2) true
    else if (t2._1 >= t1._1 && t2._1 <= t1._2) true
    else false
  }

}
