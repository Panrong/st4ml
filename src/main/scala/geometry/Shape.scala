package geometry


abstract class Shape() extends Serializable {

  def intersect(other: Shape): Boolean
  def mbr: MBR
  def dist(other: Shape): Double

  def inside(rectangle: Rectangle): Boolean

  def intersect(rectangle: Rectangle): Boolean


  def dist(point: Point): Double

  def center(): Point

  var id: Long

  var attr: Map[String, Array[String]] = Map()

  implicit def point2Shape(x: Point): Shape = x.asInstanceOf[Shape]

  implicit def rectangle2Shape(x: Rectangle): Shape = x.asInstanceOf[Shape]

}