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

  // to generate a heatmap over a time interval
  def countPerRegion[T <: Shape : ClassTag](rdd: RDD[SpatialMap[T]], t: (Long, Long)): RDD[(Rectangle, Int)] = {
    rdd.filter(x => temporalOverlap(x.timeStamp, t))
      .flatMap(_.contents)
      .mapValues(_.map(x => x.id))
      .reduceByKey(_ ++ _)
      .map(x => (x._1.coordinates.toList, x._2.length))
      .reduceByKey(_ + _)
      .map(x => (Rectangle(x._1.toArray), x._2))
  }

  // to generate a heatmap per spatial map
  def genHeatMap[T <: Shape : ClassTag](rdd: RDD[SpatialMap[T]]): RDD[((Long, Long), Array[(Rectangle, Int)])] = {
    rdd.map(sm => (sm.timeStamp, sm.contents.map {
      case (r, c) => (r, c.length)
    }))
  }
}
