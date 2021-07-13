package instances

import org.locationtech.jts.{geom => jts}

object LineString extends LineStringConstructors

trait LineStringConstructors {
  def apply(coords: jts.CoordinateSequence): LineString =
    GeometryFactory.factory.createLineString(coords)

  def apply(coords: Traversable[jts.Coordinate]): LineString =
    apply(GeometryFactory.factory.getCoordinateSequenceFactory.create(coords.toArray))

  def apply(points: Traversable[Point])(implicit e1: DummyImplicit, e2: DummyImplicit): LineString = {
    if (points.size < 2) {
      sys.error(s"Invalid LineString: Requires 2 or more points: ${points}")
    }

    val coords = points.toArray.map(_.getCoordinate)
    GeometryFactory.factory.createLineString(coords)
  }

  def apply(points: Traversable[(Double, Double)])(implicit d: DummyImplicit): LineString =
    apply(points.map{
      case (x, y) => Point(x, y)
    })

  def apply(points: Point*): LineString =
    apply(points.toList)

}
