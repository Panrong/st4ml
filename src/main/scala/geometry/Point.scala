package main.scala.geometry

import Distances.greatCircleDistance

case class Point(lon: Double, lat: Double) extends Serializable {
  def +(other: Point): Point = Point(this.lon + other.lon, this.lat + other.lat)
  def -(other: Point): Point = Point(this.lon - other.lon, this.lat - other.lat)
  def dot(other: Point): Double = this.lon * other.lon + this.lat * other.lat
  def *(scalar: Double): Point = Point(this.lon * scalar, this.lat * scalar)
  def normSquare: Double = this.lon * this.lon + this.lat * this.lat

  def geoDistance(other: Point): Double = greatCircleDistance(this, other)
}
