package st4ml.utils.mapmatching.road
import st4ml.instances.Point
import st4ml.instances.GeometryImplicits._
final case class RoadVertex(id: String, point: Point) {
  def geoDistance(other: Point): Double = point.greatCircle(other)
}