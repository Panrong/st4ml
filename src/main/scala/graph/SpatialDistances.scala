package main.scala.graph

import scala.math.{min, max, abs, acos, cos, sin}

case class Point(lon: Double, lat: Double)

case class Line(p1: Point, p2: Point)

case class LineString(points: Array[Point])


object SpatialDistances {
  implicit class PointOps(p: Point) {
    def +(other: Point): Point = Point(p.lon + other.lon, p.lat + other.lat)
    def -(other: Point): Point = Point(p.lon - other.lon, p.lat - other.lat)
    def dot(other: Point): Double = p.lon * other.lon + p.lat * other.lat
    def *(scalar: Double): Point = Point(p.lon * scalar, p.lat * scalar)
    def normSquare: Double = p.lon * p.lon + p.lat * p.lat
  }

  def greatCircleDistance(p1: Point, p2: Point): Double = {
    val r = 6371009 // earth radius in meter
    val phi1 = p1.lat.toRadians
    val lambda1 = p1.lon.toRadians
    val phi2 = p2.lat.toRadians
    val lambda2 = p2.lon.toRadians
    val deltaSigma = acos(sin(phi1) * sin(phi2) + cos(phi1) * cos(phi2) * cos(abs(lambda2 - lambda1)))
    r * deltaSigma
  }

  def point2LineMiddlePointDistance(p: Point, l: Line): (Double, Point) = {
    val midPoint = Point((l.p1.lon + l.p2.lon)/2, (l.p1.lat + l.p2.lat)/2)
    (greatCircleDistance(p, midPoint), midPoint)
  }

  def point2LineStringMiddlePointDistance(p: Point, ls: LineString): (Double, Point) = {
    val distances = ls.points.sliding(2).map(x => point2LineMiddlePointDistance(p, Line(x(0), x(1))))
    distances.reduceLeft((x, y) => if (x._1 < y._1) x else y)
  }

  def point2LineProjectionDistance(p: Point, l: Line): (Double, Point) = {
    val v = p - l.p1
    val d = l.p2 - l.p1
    val projectionPoint = l.p1 + d * max(0, min(1, (v dot d) / d.normSquare))
    val projectionDistance = greatCircleDistance(p, projectionPoint)
    (projectionDistance, projectionPoint)
  }

  def point2LineStringProjectionDistance(p: Point, ls: LineString): (Double, Point) = {
    val distances = ls.points.sliding(2).map(x => point2LineProjectionDistance(p, Line(x(0), x(1))))
    distances.reduceLeft((x, y) => if (x._1 < y._1) x else y)
  }

}
