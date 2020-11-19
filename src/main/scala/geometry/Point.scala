package geometry

import Distances.greatCircleDistance


case class Point(x: Double, y: Double, t: Long = 0) extends Shape {

  def hasTimestamp: Boolean = t != 0

  def dimensions: Int = {
    if (hasTimestamp) 3
    else 2
  }

  def +(other: Point): Point = {
    if (hasTimestamp && other.hasTimestamp) Point(x + other.x, y + other.y, t + other.t)
    else Point(x + other.x, y + other.y)
  }

  def -(other: Point): Point = {
    if (hasTimestamp && other.hasTimestamp) Point(x - other.x, y - other.y, t - other.t)
    else Point(x - other.x, y - other.y)
  }

  def dot(other: Point): Double = {
    if (hasTimestamp && other.hasTimestamp) x * other.x + y * other.y + t * other.t
    else x * other.x + y * other.y
  }

  def *(scalar: Double): Point = {
    if (hasTimestamp) Point(x * scalar, y * scalar, (t * scalar).toLong)
    else Point(x * scalar, y * scalar)
  }

  def normSquare: Double = x * x + y * y

  def geoDistance(other: Shape): Double = other match {
    case p:Point => greatCircleDistance(this, p)
    case r: Rectangle => greatCircleDistance(this, r.center())
  }

  def ==(other: Point): Boolean = {
    if (other.x != x || other.y != y) false
    else true
  }

  def <=(other: Point): Boolean = {
    if (other.x > x || other.y > y) false
    else true
  }



  def calSpeed(other: Point): Double = {
    if (other.t == this.t) 0
    else this.geoDistance(other) / (other.t - this.t)
  }

  def assignID(i: Long): Point = {
    id = i
    this
  }

  def assignTimeStamp(timestamp: Long): Point = {
    t = timestamp
    this
  }

  override def intersect(rectangle: Rectangle): Boolean = {
    inside(rectangle)
  }

  override def inside(rectangle: Rectangle): Boolean = {
    if (x >= rectangle.x_min && x <= rectangle.x_max && y >= rectangle.y_min && y <= rectangle.y_max) true
    else false
  }

  override def mbr(): Array[Double] = Array(x, y, x, y)

  override def dist(point: Point): Double = math.sqrt(math.pow(x - point.x, 2) + math.pow(y - point.y, 2))

  override def center(): Point = this

  override def toString = s"Point(${x},${y},${t})"
}
