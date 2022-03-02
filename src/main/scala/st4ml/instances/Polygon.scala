package st4ml.instances

import org.locationtech.jts.geom.Coordinate
import org.locationtech.jts.{geom => jts}

import scala.collection.GenTraversable

object Polygon extends PolygonConstructors

trait PolygonConstructors {
  def empty: Polygon =
    GeometryFactory.factory.createPolygon()

  def apply(exterior: jts.LinearRing, holes: GenTraversable[jts.LinearRing]): Polygon =
    GeometryFactory.factory.createPolygon(exterior, holes.toArray)

  def apply(exterior: jts.CoordinateSequence, holes: GenTraversable[jts.CoordinateSequence]): Polygon =
    apply(
      GeometryFactory.factory.createLinearRing(exterior),
      holes.map(GeometryFactory.factory.createLinearRing)
    )

  def apply(exterior: LineString, holes: GenTraversable[LineString]): Polygon = {
    if(!exterior.isClosed) {
      sys.error(s"Cannot create a polygon with unclosed exterior: $exterior")
    }

    if(exterior.getNumPoints < 4) {
      sys.error(s"Cannot create a polygon with exterior with fewer than 4 points: $exterior")
    }

    val extGeom = GeometryFactory.factory.createLinearRing(exterior.getCoordinateSequence)

    val holeGeoms = (
      for (hole <- holes) yield {
        if (!hole.isClosed) {
          sys.error(s"Cannot create a polygon with an unclosed hole: $hole")
        } else {
          if (hole.getNumPoints < 4) {
            sys.error(s"Cannot create a polygon with a hole with fewer than 4 points: $hole")
          } else {
            GeometryFactory.factory.createLinearRing(hole.getCoordinateSequence)
          }
        }
      }).toArray

    apply(extGeom, holeGeoms)
  }

  def apply(exterior: jts.CoordinateSequence): Polygon =
    GeometryFactory.factory.createPolygon(exterior)

  def apply(exterior: LineString): Polygon =
    apply(exterior, Set())

  def apply(exterior: Traversable[Point]): Polygon =
    apply(LineString(exterior), Set())

  def apply(exterior: Traversable[jts.Coordinate])(implicit d: DummyImplicit): Polygon =
    apply(LineString(exterior), Set())

  def apply(exterior: Traversable[(Double, Double)])(implicit d: DummyImplicit, e:DummyImplicit): Polygon =
    apply(LineString(exterior)(d), Set())

  def apply(exterior: Point*): Polygon =
    apply(LineString(exterior), Set())
}
