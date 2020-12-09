package geometry

import scala.math.{max, min}

case class Rectangle(coordinates: Array[Double], ID: Long = 0) extends Shape with Serializable {
  require(coordinates.length == 4,
    s"Rectangle should have 4 coordinates(xMin, yMin, xMax, yMax) " +
      s"while ${coordinates.mkString("Array(", ", ", ")")} has ${coordinates.length} dimensions.")

  val xMin: Double = coordinates(0)
  val xMax: Double = coordinates(2)
  val yMin: Double = coordinates(1)
  val yMax: Double = coordinates(3)

  var timeStamp = 0L

  require(xMin <= xMax && yMin <= yMax, "The order should be (xMin, yMin, xMax, yMax)")

  val area: Double = (xMax - xMin) * (yMax - yMin)

  override def center(): Point = Point(Array((xMax + xMin) / 2, (yMax + yMin) / 2))

  var id: Long = ID

  override def intersect(other: Shape): Boolean = other match {
    case p: Point => p.intersect(this)
    case r: Rectangle => this.overlappingArea(r) > 0
    case l: Line => l.intersect(this)
  }

  override def geoDistance(other: Shape): Double = other.geoDistance(center())

  def assignID(i: Long): Rectangle = {
    id = i
    this
  }

  def overlappingArea(r: Rectangle): Double = {
    val overlapX = max((xMax - xMin) + (r.xMax - r.xMin) - (max(xMax, r.xMax) - min(xMin, r.xMin)), 0)
    val overlapY = max((yMax - yMin) + (r.yMax - r.yMin) - (max(yMax, r.yMax) - min(yMin, r.yMin)), 0)
    overlapX * overlapY
  }


  override def inside(r: Rectangle): Boolean = {
    if (xMin >= r.xMin && yMin >= r.yMin && xMax <= r.xMax && yMax <= r.yMax) true
    else false
  }


  override def mbr(): Rectangle = this


  val vertices: Array[Point] = Array(Point(Array(xMin, yMin)), Point(Array(xMin, yMax)), Point(Array(xMax, yMax)), Point(Array(xMax, yMin)))
  val edges: Array[Line] = Array(Line(vertices(0), vertices(1)), Line(vertices(1), vertices(2)), Line(vertices(2), vertices(3)), Line(vertices(3), vertices(0)))
  val diagonals: Array[Line] = Array(Line(vertices(0), vertices(2)), Line(vertices(1), vertices(3)))

  def setTimeStamp(t: Long): Rectangle = {
    timeStamp = t
    this
  }

  override def toString = s"${this.xMin},${this.yMin},${this.xMax},${this.yMax}"
}
