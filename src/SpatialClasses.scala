package SpatialClasses


import scala.math._


case class Point(lat: Double, long: Double, t: Long = 0) extends Serializable {

}
case class Rectangle(topLeft:Point, bottomRight:Point) extends Serializable{
  val center = Point((topLeft.lat + bottomRight.lat)/2,  (topLeft.long + bottomRight.long)/2)


}
case class Line(start: Point, end: Point, id: Long = 0) extends Serializable {
  val length = greatCircleDist(start, end)

  def dist2Point(p: Point): Double = {
    val a = length
    val b = greatCircleDist(p, start)
    val c = greatCircleDist(p, end)
    val alpha = acos((b * b + c * c - a * a) / (2 * b * c))
    val beta = acos((-b * b + c * c + a * a) / (2 * a * c))
    val gamma = acos((b * b - c * c + a * a) / (2 * b * a))
    if (beta < Pi / 2 && gamma < Pi / 2) sqrt((a + b + c) * (a + b - c) * (a + c - b) * (b + c - a)) / (2 * a)
    else if (beta >= Pi / 2) b
    else c
  }

  def projectByPoint(p: Point): Array[Double] = {
    val a = length
    val b = greatCircleDist(p, start)
    val c = greatCircleDist(p, end)
    val alpha = acos((b * b + c * c - a * a) / (2 * b * c))
    val beta = acos((-b * b + c * c + a * a) / (2 * a * c))
    val gamma = acos((b * b - c * c + a * a) / (2 * b * a))
    if (beta < Pi / 2 && gamma < Pi / 2) Array(c * sin(beta), a - c * sin(beta))
    else if (beta >= Pi / 2) Array(0, a)
    else Array(a, 0)
  }

  def mbr():Rectangle = {
    val lat_min = min(start.lat, end.lat)
    val long_min = min(start.long, end.long)
    val lat_max = max(start.lat, end.lat)
    val long_max = max(start.long, end.long)
    Rectangle(Point(lat_min, long_min), Point(lat_max, long_max))
  }
}

case class Trajectory(tripID: Long, taxiID: Long, startTime: Long, points: Array[Point]) extends Serializable {

}

object greatCircleDist {
  def apply(s: Point, f: Point): Double = {
    val r = 6371009 // earth radius in meter
    val phiS = s.lat.toRadians
    val lambdaS = s.long.toRadians
    val phiF = f.lat.toRadians
    val lambdaF = f.long.toRadians
    val deltaSigma = acos(sin(phiS) * sin(phiF) + cos(phiS) * cos(phiF) * cos(abs(lambdaF - lambdaS)))
    //val deltaSigma = 2*asin(sqrt(pow(sin(abs(phiF-phiS)/2),2)+cos(phiF)*cos(phiS)*pow(sin(abs(lambdaF-lambdaS)/2),2)))
    r * deltaSigma
  }
}