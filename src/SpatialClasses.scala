package SpatialClasses


import scala.math._

abstract class Shape() {
  def inside(rectangle: Rectangle): Boolean

  def intersect(rectangle: Rectangle): Boolean

  def mbr(): Array[Double]

  def dist(point: Point): Double

  def center(): Point

  var id: Long
}

case class Point(lat: Double, long: Double, t: Long = 0, ID:Long = 0) extends Shape with Serializable {
  val x: Double = lat
  val y: Double = long
  override var id:Long = ID
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

case class Rectangle(bottomLeft: Point, topRight: Point, ID:Long = 0) extends Shape with Serializable {
  override def center(): Point = Point((bottomLeft.lat + topRight.lat) / 2, (bottomLeft.long + topRight.long) / 2)
  override var id:Long = ID
  def assignID(i:Long) :Rectangle= {
    id = i
    this
  }
  def dilate(d: Double): Rectangle = {
    // dilate the rectangle with d meter
    //approx 1 latitude degree = 111km
    // approx 1 longitude degree =111.413*cos(phi)-0.094*cos(3*phi) where phi is latitude
    val newLatMin = bottomLeft.lat - d / 111000
    val newLatMax = topRight.lat + d / 111000
    val newLongMin = bottomLeft.long - (111413 * cos(bottomLeft.lat.toRadians) - 94 * cos(3 * bottomLeft.lat.toRadians))
    val newLongMax = topRight.long - (111.413 * cos(topRight.lat.toRadians) - 0.094 * cos(3 * topRight.lat.toRadians))
    Rectangle(Point(newLatMin, newLongMin), Point(newLatMax, newLongMax))
  }

  val x_min = bottomLeft.x
  val x_max = topRight.x
  val y_min = bottomLeft.y
  val y_max = topRight.y
  val area: Double = (topRight.x - bottomLeft.x) * (topRight.y - bottomLeft.y)

  def overlap(r: Rectangle): Double = {
    val overlapX = max((x_max - x_min) + (r.x_max - r.x_min) - (max(x_max, r.x_max) - min(x_min, r.x_min)), 0)
    val overlapY = max((y_max - y_min) + (r.y_max - r.y_min) - (max(y_max, r.y_max) - min(y_min, r.y_min)), 0)
    overlapX * overlapY
  }

  def includeEntry(shape: Shape): Rectangle = {
    // return a new Rectangle
    if (shape.isInstanceOf[Point]) {
      val point = shape.asInstanceOf[Point]
      val new_x_min = min(point.x, this.x_min)
      val new_x_max = max(point.x, this.x_max)
      val new_y_min = min(point.y, this.y_min)
      val new_y_max = max(point.y, this.y_max)
      //println(Point(new_x_min, new_y_min), Point(new_x_max, new_y_max))
      Rectangle(Point(new_x_min, new_y_min), Point(new_x_max, new_y_max))
    }
    else if (shape.isInstanceOf[Rectangle]) {
      val rectangle = shape.asInstanceOf[Rectangle]
      val new_x_min = min(rectangle.x_min, this.x_min)
      val new_x_max = max(rectangle.x_max, this.x_max)
      val new_y_min = min(rectangle.y_min, this.y_min)
      val new_y_max = max(rectangle.y_max, this.y_max)
      Rectangle(Point(new_x_min, new_y_min), Point(new_x_max, new_y_max))
    }
    else throw new IllegalArgumentException
  }

  def enlargement(shape: Shape): Double = {
    //calculate the enlargement needed to include one entry
    val newRectangle = this.includeEntry(shape)
    newRectangle.area - this.area
  }

  def margin(): Double = {
    (x_max - x_min + y_max - y_min) * 2
  }

  override def inside(rectangle: Rectangle): Boolean = {
    if (x_min >= rectangle.x_min && y_min >= rectangle.y_min && x_max <= rectangle.x_max && y_max <= rectangle.y_max) true
    else false
  }

  override def intersect(r: Rectangle): Boolean = {
    if (abs((x_min + x_max) / 2 - (r.x_min + r.x_max) / 2) < ((x_max + r.x_max - x_min - r.x_min) / 2)
      && abs((y_min + y_max) / 2 - (r.y_min + r.y_max) / 2) < ((y_max + r.y_max - y_min - r.y_min) / 2)) true
    else false
  }

  override def mbr(): Array[Double] = Array(x_min, y_min, x_max, y_max)

  override def dist(point: Point): Double = math.sqrt(math.pow(center.x - point.x, 2) + math.pow(center.y - point.y, 2))

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

  def mbr(): Rectangle = {
    val lat_min = min(start.lat, end.lat)
    val long_min = min(start.long, end.long)
    val lat_max = max(start.lat, end.lat)
    val long_max = max(start.long, end.long)
    Rectangle(Point(lat_min, long_min), Point(lat_max, long_max))
  }
}

case class Trajectory(tripID: Long, taxiID: Long, startTime: Long, points: Array[Point]) extends Serializable {
  def mbr:Rectangle = {
    var lat = new Array[Double](0)
    var long = new Array[Double](0)
    for(p <- points){
      lat = lat :+ p.lat
      long  =long :+ p.long
    }
    Rectangle(Point(lat.min, long.min), Point(lat.max, long.max))
  }
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