package st4ml.instances

object GeometryImplicits extends GeometryImplicits

trait GeometryImplicits {
  implicit class withExtraPointOps(val self: Point) extends PointOps
  implicit class withExtraLineStringOps(val self: LineString) extends LineStringOps
  implicit class withExtraPolygonOps(val self: Polygon) extends PolygonOps
}
