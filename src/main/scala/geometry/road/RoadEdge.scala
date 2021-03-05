package geometry.road

import geometry.{Line, LineString, Point, Rectangle, Shape}

final case class RoadEdge(override var id: String, from: String, to: String, length: Double, ls: LineString) extends Shape {
  def midPoint: Point = Line(ls.points(0), ls.points.last).midPoint

  def midPointDistance(p: Point): (String, Double, Point) = {
    val (distance, midPoint) = ls.midPointDistance(p)
    (id, distance, midPoint.setTimeStamp(p.t))
  }

  def projectionDistance(p: Point): (String, Double, Point) = {
    val (distance, projectedPoint) = ls.projectionDistance(p)
    (id, distance, projectedPoint.setTimeStamp(p.t))
  }

  override def coordinates: Array[Double] = this.mbr.coordinates

  override def mbr: Rectangle = ls.mbr

  override def intersect(other: Shape): Boolean = this.mbr.intersect(other)

  override def center(): Point = midPoint

  override def geoDistance(other: Shape): Double = this.center().geoDistance(other)

  override def minDist(other: Shape): Double = this.projectionDistance(other.center())._2

  override def inside(rectangle: Rectangle): Boolean = this.intersect(rectangle)

  override var timeStamp: (Long, Long) = (-1 ,-1)
}