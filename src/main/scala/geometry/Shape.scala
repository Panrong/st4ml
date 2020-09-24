package main.scala.geometry

abstract class Shape() {
  def inside(rectangle: Rectangle): Boolean

  def intersect(rectangle: Rectangle): Boolean

  def mbr(): Array[Double]

  def dist(point: Point): Double

  def center(): Point

  var id: Long

  var attr: Map[String, Array[String]] = Map()
}