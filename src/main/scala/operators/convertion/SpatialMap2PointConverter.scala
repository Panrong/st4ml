package operators.convertion

import geometry.{Point, SpatialMap}
import org.apache.spark.rdd.RDD

class SpatialMap2PointConverter extends Converter{

  override type I = SpatialMap[Point]
  override type O = Point

  override def convert(rdd: RDD[(Int, SpatialMap[Point])]): RDD[Point] = {
    rdd.map(_._2).flatMap(_.contents).flatMap(x => x._2)
  }
}
