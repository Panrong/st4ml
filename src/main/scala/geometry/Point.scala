package main.scala.geometry

import Distances.greatCircleDistance

case class Point(lon: Double, lat: Double, t: Long = 0, ID:Long = 0) extends Shape with Serializable {
  val x: Double = lon
  val y: Double = lat
  override var id:Long = ID

  def +(other: Point): Point = Point(this.lon + other.lon, this.lat + other.lat)
  def -(other: Point): Point = Point(this.lon - other.lon, this.lat - other.lat)
  def dot(other: Point): Double = this.lon * other.lon + this.lat * other.lat
  def *(scalar: Double): Point = Point(this.lon * scalar, this.lat * scalar)
  def normSquare: Double = this.lon * this.lon + this.lat * this.lat

  def geoDistance(other: Point): Double = greatCircleDistance(this, other)

  def assignID(i:Long):Point = {
    id = i
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
}
