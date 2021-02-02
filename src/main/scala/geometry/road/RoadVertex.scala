package geometry.road

import geometry.Point

final case class RoadVertex(id: String, point: Point) {
  def geoDistance(other: Point): Double = this.point.geoDistance(other)
}