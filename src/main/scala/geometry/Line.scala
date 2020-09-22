package main.scala.geometry

import math.{max, min}


case class Line(o: Point, d: Point) extends Serializable {
  def midPoint: Point = Point((this.o.lon + this.d.lon)/2, (this.o.lat + this.d.lat)/2)

  def midPointDistance(p: Point): (Double, Point) = (p.geoDistance(this.midPoint), this.midPoint)

  def projectionDistance(p: Point): (Double, Point) = {
    val v = p - this.o
    val d = this.d - this.o
    val projectionPoint = this.o + d * max(0, min(1, (v dot d) / d.normSquare))
    val projectionDistance = p.geoDistance(projectionPoint)
    (projectionDistance, projectionPoint)
  }
}
