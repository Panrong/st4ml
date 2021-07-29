package operatorsNew.converter

import instances.{Duration, Instance, Polygon}
import org.apache.spark.rdd.RDD

abstract class Converter extends Serializable {
  type I <: Instance[_, _, _]
  type O <: Instance[_, _, _]

  def convert(input: RDD[I]): RDD[O]

  def mbrAll(geometries: Array[I]): Polygon = {
    val extents = geometries.map(_.extent)
    val initial = extents.head
    extents.foldLeft(initial)(_.combine(_)).toPolygon
  }

  def durationAll(instances: Array[I]): Duration = {
    val start = instances.map(_.duration.start).min
    val end = instances.map(_.duration.end).max
    Duration(start, end)
  }
}
