package operators.convertion

import geometry.{Point, TimeSeries}
import org.apache.spark.rdd.RDD

class TimeSeries2PointConverter extends Converter {
  def convert(rdd: RDD[(Int, TimeSeries[Point])]): RDD[Point] = {
    rdd.map(_._2).flatMap(_.series).flatMap(x => x)
  }
}
