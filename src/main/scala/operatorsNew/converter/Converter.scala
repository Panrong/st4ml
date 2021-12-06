package operatorsNew.converter

import instances.{Duration, Instance, Polygon}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

abstract class Converter extends Serializable {

  val optimization: String

  def mbrAll[I <: Instance[_, _, _] : ClassTag](geometries: Array[I]): Polygon = {
    val extents = geometries.map(_.extent)
    val initial = extents.head
    extents.foldLeft(initial)(_.combine(_)).toPolygon
  }

  def durationAll[I <: Instance[_, _, _] : ClassTag](instances: Array[I]): Duration = {
    val start = instances.map(_.duration.start).min
    val end = instances.map(_.duration.end).max
    Duration(start, end)
  }
}
