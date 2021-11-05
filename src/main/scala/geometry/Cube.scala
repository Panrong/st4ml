package geometry

import scala.math.{max, min}

case class Cube(coordinates: Array[Double], ID: String = "0") extends Shape with Serializable {
  require(coordinates.length == 6,
    s"Cube should have 6 coordinates(xMin, yMin, xMax, yMax, tMin, tMax) " +
      s"while ${coordinates.mkString("Array(", ", ", ")")} has ${coordinates.length} dimensions.")
  val xMin: Double = coordinates(0)
  val xMax: Double = coordinates(2)
  val yMin: Double = coordinates(1)
  val yMax: Double = coordinates(3)
  val tMin: Double = coordinates(4)
  val tMax: Double = coordinates(5)

  val low: Point = Point(Array(xMin, yMin))
  val high: Point = Point(Array(xMax, yMax))

  var timeStamp: (Long, Long) = (0L, 0L)

  require(xMin <= xMax && yMin <= yMax && tMin <= tMax,
    "The order should be (xMin, yMin, xMax, yMax, tMin, tMax)")

  val area: Double = (xMax - xMin) * (yMax - yMin)

  override def center(): Point = Point(Array((xMax + xMin) / 2, (yMax + yMin) / 2))

  override var id: String = ID

  override def intersect(other: Shape): Boolean = other match {
    case p: Point => p.intersect(this)
    //    case r: Rectangle => this.overlappingArea(r) > 0
    case r: Rectangle => r.isOverlap(this.toRectangle)
    case l: Line => l.intersect(this)
    case c: Cube => c.toRectangle.intersect(this.toRectangle) && c.temporalOverlap(this)
  }

  override def geoDistance(other: Shape): Double = other.geoDistance(center())

  override def minDist(other: Shape): Double = {
    other match {
      case p: Point => minDist(p)
      case r: Cube => minDist(r)
      case l: Line => l.minDist(this)
    }
  }

  def toRectangle: Rectangle = Rectangle(this.coordinates.take(4), this.id)

  def temporalOverlap(cube: Cube): Boolean = {
    val t1 = this.coordinates.takeRight(2)
    val t2 = cube.coordinates.takeRight(2)
    if (t1(1) - t1(0) + t2(1) - t2(0) >= max(t2(1), t1(1)) - min(t2(0), t1(0))) true
    else false
  }

  def minDist(p: Point): Double = {
    require(low.coordinates.length == p.coordinates.length)
    var ans = 0.0
    for (i <- p.coordinates.indices) {
      if (p.coordinates(i) < low.coordinates(i)) {
        ans += (low.coordinates(i) - p.coordinates(i)) * (low.coordinates(i) - p.coordinates(i))
      } else if (p.coordinates(i) > high.coordinates(i)) {
        ans += (p.coordinates(i) - high.coordinates(i)) * (p.coordinates(i) - high.coordinates(i))
      }
    }
    Math.sqrt(ans)
  }

  def minDist(other: Cube): Double = {
    require(low.coordinates.length == other.low.coordinates.length)
    var ans = 0.0
    for (i <- low.coordinates.indices) {
      var x = 0.0
      if (other.high.coordinates(i) < low.coordinates(i)) {
        x = Math.abs(other.high.coordinates(i) - low.coordinates(i))
      } else if (high.coordinates(i) < other.low.coordinates(i)) {
        x = Math.abs(other.low.coordinates(i) - high.coordinates(i))
      }
      ans += x * x
    }
    Math.sqrt(ans)
  }


  def assignID(i: String): Cube = {
    id = i
    this
  }

  def overlappingArea(r: Rectangle): Double = {
    val overlapX = max((xMax - xMin) + (r.xMax - r.xMin) - (max(xMax, r.xMax) - min(xMin, r.xMin)), 0)
    val overlapY = max((yMax - yMin) + (r.yMax - r.yMin) - (max(yMax, r.yMax) - min(yMin, r.yMin)), 0)
    overlapX * overlapY
  }

  // Two rectangles intersect only on boundaries are considered as true
  def isOverlap(other: Rectangle): Boolean = {
    require(low.coordinates.length == other.low.coordinates.length)
    for (i <- low.coordinates.indices)
      if (low.coordinates(i) > other.high.coordinates(i) || high.coordinates(i) < other.low.coordinates(i)) {
        return false
      }
    true
  }

  def overlappingRatio(r: Rectangle): Double = {
    val intersect_low = low.coordinates.zip(r.low.coordinates).map(x => Math.max(x._1, x._2))
    val intersect_high = high.coordinates.zip(r.high.coordinates).map(x => Math.min(x._1, x._2))
    val diff_intersect = intersect_low.zip(intersect_high).map(x => x._2 - x._1)
    if (diff_intersect.forall(_ > 0)) 1.0 * diff_intersect.product / area
    else 0.0
  }

  override def inside(r: Rectangle): Boolean = {
    if (xMin >= r.xMin && yMin >= r.yMin && xMax <= r.xMax && yMax <= r.yMax) true
    else false
  }

  override def mbr: Rectangle = this.toRectangle

  val vertices: Array[Point] = Array(Point(Array(xMin, yMin)), Point(Array(xMin, yMax)), Point(Array(xMax, yMax)), Point(Array(xMax, yMin)))
  val edges: Array[Line] = Array(Line(vertices(0), vertices(1)), Line(vertices(1), vertices(2)), Line(vertices(2), vertices(3)), Line(vertices(3), vertices(0)))
  val diagonals: Array[Line] = Array(Line(vertices(0), vertices(2)), Line(vertices(1), vertices(3)))

  def setTimeStamp(t: (Long, Long)): Cube = {
    timeStamp = t
    this
  }

  def setTimeStamp(t: Long): Cube = {
    timeStamp = (t, t)
    this
  }

  override def toString = s"${this.xMin},${this.yMin},${this.xMax},${this.yMax}"
}