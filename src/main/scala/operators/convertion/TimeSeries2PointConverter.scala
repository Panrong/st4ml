package operators.convertion

import geometry.{Point, TimeSeries}
import org.apache.spark.rdd.RDD

class TimeSeries2PointConverter extends Converter {

  override type I = TimeSeries[Point]
  override type O = Point

  override def convert(rdd: RDD[TimeSeries[Point]]): RDD[Point] = {
    rdd.flatMap(_.series).flatMap(x => x)
  }
}
