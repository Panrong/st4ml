package st4ml.utils.mapmatching.road

import org.locationtech.jts.geom.LineSegment
import st4ml.instances.{GeometryImplicits, LineString, Point}

final case class RoadEdge(id: String, from: String, to: String, length: Double, ls: LineString, osmId: String = "") {

  import GeometryImplicits._

  def midPoint: Point = ls.getCentroid

  def midPointDistance(p: Point): (String, Double, Point) = {
    val midPoint = ls.getCentroid
    val distance = p.greatCircle(midPoint)
    (id, distance, midPoint)
  }

  //TODO projection distance
  def projectionDistance(p: Point): (String, Double, Point) = {
    val midPoint = ls.getCentroid
    val distance = p.greatCircle(midPoint)
    (id, distance, midPoint)
  }

  def coordinates: Array[Double] = {
    val coordinates = ls.getCoordinates
    val xMin = coordinates.map(_.x).min
    val yMin = coordinates.map(_.y).min
    val xMax = coordinates.map(_.x).max
    val yMax = coordinates.map(_.y).max
    Array(xMin, yMin, xMax, yMax)
  }
  //
  //   def mbr: Polygon = ls.ge
  //
  //   def intersect(other: Geometry): Boolean = this.mbr.intersect(other)
  //
  //   def center(): Point = midPoint
  //
  //   def geoDistance(other: Shape): Double = this.center().geoDistance(other)
  //
  //   def minDist(other: Shape): Double = this.projectionDistance(other.center())._2
  //
  //   def inside(rectangle: Polygon): Boolean = this.intersect(rectangle)

}