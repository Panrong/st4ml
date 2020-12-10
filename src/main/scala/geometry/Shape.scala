package geometry

abstract class Shape() extends Serializable {

  def coordinates: Array[Double]

  def mbr: Rectangle

  def intersect(other: Shape): Boolean

  def center(): Point

  var attr: Map[String, Array[String]] = Map()

  def geoDistance(other: Shape): Double

  def minDist(other: Shape): Double

  def inside(rectangle: Rectangle): Boolean

  var id: Long

  implicit def point2Shape(x: Point): Shape = x.asInstanceOf[Shape]

  implicit def rectangle2Shape(x: Rectangle): Shape = x.asInstanceOf[Shape]

}
