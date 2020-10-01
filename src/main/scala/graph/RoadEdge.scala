package main.scala.graph

import main.scala.geometry.{Point, Line, LineString}

final case class RoadEdge(id: String, from: String, to: String, length: Double, ls: LineString) {
  def midPoint: Point = Line(ls.points(0), ls.points.last).midPoint

  def midPointDistance(p: Point): (String, Double, Point) = {
    val (distance, midPoint) = ls.midPointDistance(p)
    (id, distance, midPoint.assignTimeStamp(p.t))
  }

  def projectionDistance(p: Point): (String, Double, Point) = {
    val (distance, projectedPoint) = ls.projectionDistance(p)
    (id, distance, projectedPoint.assignTimeStamp(p.t))
  }
}