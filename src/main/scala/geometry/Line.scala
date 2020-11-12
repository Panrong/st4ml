package geometry

import math.{max, min}
import Distances.greatCircleDistance


case class Line(o: Point, d: Point, ID: Long = 0) extends Serializable {
  def midPoint: Point = Point((this.o.lon + this.d.lon)/2, (this.o.lat + this.d.lat)/2)

  def midPointDistance(p: Point): (Double, Point) = (p.geoDistance(this.midPoint), this.midPoint)

  def projectionDistance(p: Point): (Double, Point) = {
    val v = p - this.o
    val d = this.d - this.o
    val projectionPoint = this.o + d * max(0, min(1, (v dot d) / d.normSquare))
    val projectionDistance = p.geoDistance(projectionPoint)
    (projectionDistance, projectionPoint)
  }

  val length = greatCircleDistance(o, d)

  def mbr(): Rectangle = {
    val lat_min = min(o.lat, d.lat)
    val long_min = min(o.lon, d.lon)
    val lat_max = max(o.lat, d.lat)
    val long_max = max(o.lon, d.lon)
    Rectangle(Point(lat_min, long_min), Point(lat_max, long_max))
  }
  def crosses(l: Line): Boolean = {
    def crossProduct(A: Point, B: Point, C: Point): Double = (B.x - A.x) * (C.y - A.y) - (B.y - A.y) * (C.x - A.x)
    val A = o
    val B = d
    val C = l.o
    val D = l.d
    if (min(A.x, B.x) <= max(C.x, D.x) &&
      min(C.x, D.x) <= max(A.x, B.x) &&
      min(A.y, B.y) <= max(C.y, D.y) &&
      min(C.y, D.y) <= max(A.y, B.y) &&
      crossProduct(A, B, C) * crossProduct(A, B, D) < 0 &&
      crossProduct(C, D, A) * crossProduct(C, D, B) < 0)  true
    else  false
  }

  def intersects(r: Rectangle): Boolean = {
    if (this.o.intersect(r) || this.d.intersect(r)) return true
    else {
      if (!this.mbr().intersect(r)) return false
      else {
        for (e <- r.diagonals) if (e.crosses(this)) return true
        return false
      }
    }
  }
}
