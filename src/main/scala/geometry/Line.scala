package geometry

import scala.math.{max, min}

case class Line(o: Point, d: Point, ID: String = "0") extends Shape with Serializable {

  def midPoint: Point = Point(Array((o.coordinates(0) + d.coordinates(0)) / 2, (o.coordinates(1) + d.coordinates(1)) / 2))

  def midPointDistance(p: Point): (Double, Point) = (p.geoDistance(midPoint), midPoint)

  override def coordinates: Array[Double] = Array.concat(o.coordinates, d.coordinates)

  override def center(): Point = midPoint

  override def inside(rectangle: Rectangle): Boolean = mbr.inside(rectangle)

  var timeStamp = (o.timeStamp._1, d.timeStamp._2)

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

  def setID(id: String): Line = Line(this.o, this.d, id)

  def setTimeStamp(t: (Long, Long)): Line = {
    timeStamp = t
    this
  }

  def setTimeStamp(t: Long): Line = {
    timeStamp = (t, t)
    this
  }

  //crop by a spatial window
  def windowBy(rectangle: Rectangle): Line = {

    // check if the line is totally inside the rectangle
    if (o.inside(rectangle) && d.inside(rectangle)) this

    // check if the line is parallel to x or y axis
    else if (d.y == o.y) {
      val kt = (d.t - o.t) / (d.x - o.x)
      val xs = Array(o.x, d.x, rectangle.xMin, rectangle.xMax).sorted.slice(1, 3)
      val (newO, newD) = if (d.x < o.x) (Point(Array(xs(1), d.y)), Point(Array(xs(0), d.y)))
      else (Point(Array(xs(0), d.y)), Point(Array(xs(1), d.y)))
      Line(newO.setTimeStamp(((newO.x - o.x) * kt + o.t).toLong), newD.setTimeStamp(((newD.x - o.x) * kt + o.t).toLong))
    }
    else if (d.x == o.x) {
      val kt = (d.t - o.t) / (d.y - o.y)
      val ys = Array(o.y, d.y, rectangle.yMin, rectangle.yMax).sorted.slice(1, 3)
      val (newO, newD) = if (d.y < o.y) (Point(Array(d.x, ys(1))), Point(Array(d.x, ys(0))))
      else (Point(Array(d.x, ys(0))), Point(Array(d.x, ys(1))))
      Line(newO.setTimeStamp(((newO.y - o.y) * kt + o.t).toLong), newD.setTimeStamp(((newD.y - o.y) * kt + o.t).toLong))
    }
    else {
      val k = (d.y - o.y) / (d.x - o.x) // y = k * (x - o.x) + o.y and x = o.x + (y - o.y) / k
      val kt = (d.t - o.t) / (d.x - o.x)
      // get intersections with four edges
      val point1 = Point(Array(rectangle.xMin, k * (rectangle.xMin - o.x) + o.y))
      val point2 = Point(Array(rectangle.xMax, k * (rectangle.xMax - o.x) + o.y))
      val point3 = Point(Array(o.x + (rectangle.yMin - o.y) / k, rectangle.yMin))
      val point4 = Point(Array(o.x + (rectangle.yMax - o.y) / k, rectangle.yMax))
      val edgePoints = Array(point1, point2, point3, point4).sortBy(_.x).slice(1, 3) // get the middle two points of the four
      val actualPoints = (edgePoints :+ o :+ d).sortBy(_.x).slice(1, 3)
      if (o.x > d.x) Line(actualPoints(1).setTimeStamp(((actualPoints(1).x - o.x) * kt + o.t).toLong),
        actualPoints(0).setTimeStamp(((actualPoints(0).x - o.x) * kt + o.t).toLong))
      else Line(actualPoints(0).setTimeStamp(((actualPoints(0).x - o.x) * kt + o.t).toLong),
        actualPoints(1).setTimeStamp(((actualPoints(1).x - o.x) * kt + o.t).toLong))
    }
  }

  // crop by a temporal window
  def windowBy(range: (Long, Long)): Line = {
    val oInside = if (timeStamp._1 >= range._1 && timeStamp._1 <= range._2) true else false
    val dInside = if (timeStamp._2 >= range._1 && timeStamp._2 <= range._2) true else false
    if (oInside && dInside) this
    else {
      val kx = (d.x - o.x) / (timeStamp._2 - timeStamp._1)
      val ky = (d.y - o.y) / (timeStamp._2 - timeStamp._1)
      if (oInside) {
        val newX = kx * (range._2 - timeStamp._1) + o.x
        val newY = ky * (range._2 - timeStamp._1) + o.y
        val newD = Point(Array(newX, newY), t = range._2)
        Line(o, newD, this.id)
      }
      else if (dInside) {
        val newX = kx * (range._1 - timeStamp._1) + o.x
        val newY = ky * (range._1 - timeStamp._1) + o.y
        val newO = Point(Array(newX, newY), t = range._1)
        Line(newO, d, this.id)
      }
      else if (timeStamp._1 <= range._1 && timeStamp._2 >= range._2) {
        val newOX = kx * (range._1 - timeStamp._1) + o.x
        val newOY = ky * (range._1 - timeStamp._1) + o.y
        val newO = Point(Array(newOX, newOY), t = range._1)
        val newDX = kx * (range._2 - timeStamp._1) + o.x
        val newDY = ky * (range._2 - timeStamp._1) + o.y
        val newD = Point(Array(newDX, newDY), t = range._2)
        Line(newO, newD, this.id)
      }
      else {
        println(o.t, d.t, range)
        throw new IllegalArgumentException
      }
    }
  }
}
