package geometry

import scala.math.{max, min}

case class Line(o: Point, d: Point, ID: Long = 0) extends Shape with Serializable {

  def midPoint: Point = Point(Array((o.coordinates(0) + d.coordinates(0)) / 2, (o.coordinates(1) + d.coordinates(1)) / 2))

  def midPointDistance(p: Point): (Double, Point) = (p.geoDistance(midPoint), midPoint)

  override def coordinates: Array[Double] = Array.concat(o.coordinates, d.coordinates)

  override def center(): Point = midPoint

  override def inside(rectangle: Rectangle): Boolean = mbr.inside(rectangle)

  var timeStamp = (0L, 0L)

  override var id = ID

  def projectionDistance(p: Point): (Double, Point) = {
    val v = p - o
    val d = this.d - o
    val projectionPoint = o + d * max(0, min(1, (v dot d) / d.normSquare))
    val projectionDistance = p.geoDistance(projectionPoint)
    (projectionDistance, projectionPoint)
  }

  val length = Distances.greatCircleDistance(o, d)

  override def mbr(): Rectangle = {
    val minLat = min(o.coordinates(1), d.coordinates(1))
    val minLon = min(o.coordinates(0), d.coordinates(0))
    val maxLat = max(o.coordinates(1), d.coordinates(1))
    val maxLon = max(o.coordinates(0), d.coordinates(0))
    Rectangle(Array(minLon, minLat, maxLon, maxLat))
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
      crossProduct(C, D, A) * crossProduct(C, D, B) < 0) true
    else false
  }

  override def intersect(other: Shape): Boolean = other match {
    case r: Rectangle => {
      if (o.intersect(r) || d.intersect(r)) true
      else {
        if (!mbr().intersect(r)) false
        else {
          for (e <- r.diagonals) if (e.crosses(this)) return true
          false
        }
      }
    }
    case p: Point => {
      if (projectionDistance(p)._1 == 0) true
      else false
    }
  }

  override def geoDistance(other: Shape): Double = projectionDistance(other.center())._1

  override def minDist(other: Shape): Double = other match {
    case p: Point => minDist(p)
    case r: Rectangle => minDist(r)
    case l: Line => minDist(l)
    case c: Cube => minDist(c.toRectangle)
  }

  def minDist(p: Point): Double = {
    require(p.coordinates.length == 2)
    val len = o.minDist(d)
    if (len == 0) return p.minDist(o)
    var t = ((p.coordinates(0) - o.coordinates(0)) * (d.coordinates(0) - o.coordinates(0))
      + (p.coordinates(1) - o.coordinates(1)) * (d.coordinates(1) - o.coordinates(1))) / (len * len)
    t = max(0, min(1, t))
    val proj_x = o.coordinates(0) + t * (d.coordinates(0) - o.coordinates(0))
    val proj_y = o.coordinates(1) + t * (d.coordinates(1) - o.coordinates(1))
    p.minDist(Point(Array(proj_x, proj_y)))
  }

  def minDist(r: Rectangle): Double = {
    val l1 = Line(r.low, Point(Array(r.low.coordinates(0), r.high.coordinates(1))))
    val l2 = Line(r.low, Point(Array(r.high.coordinates(0), r.low.coordinates(1))))
    val l3 = Line(r.high, Point(Array(r.low.coordinates(0), r.high.coordinates(1))))
    val l4 = Line(r.high, Point(Array(r.high.coordinates(0), r.low.coordinates(1))))
    min(min(minDist(l1), minDist(l2)), min(minDist(l3), minDist(l4)))
  }

  def minDist(l: Line): Double = {
    if (intersect(l)) 0.0
    else {
      min(min(minDist(l.o), minDist(l.d)), min(l.minDist(o), l.minDist(d)))
    }
  }

  def setID(id: Long): Line = Line(this.o, this.d, id)

  def setTimeStamp(t: (Long, Long)): Line = {
    timeStamp = t
    this
  }

  def setTimeStamp(t: Long): Line = {
    timeStamp = (t, t)
    this
  }
}
