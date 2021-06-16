package instances

import org.locationtech.jts.{geom => jts}

object Point extends PointConstructors

trait PointConstructors {
  def apply(coord: jts.Coordinate): Point =
    GeometryFactory.factory.createPoint(coord)

  def apply(x: Double, y: Double): Point =
    GeometryFactory.factory.createPoint(new jts.Coordinate(x, y))

  def apply(t: (Double, Double)): Point =
    apply(t._1, t._2)
}

