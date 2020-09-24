package main.scala.geometry

import scala.math.{cos, abs, max, min}

case class Rectangle(bottomLeft: Point, topRight: Point, ID: Long = 0) extends Shape with Serializable {
  override def center(): Point = Point((bottomLeft.lon + topRight.lon) / 2, (bottomLeft.lat + topRight.lat) / 2)
  override var id: Long = ID
  def assignID(i: Long): Rectangle = {
    id = i
    this
  }

  def dilate(d: Double): Rectangle = {
    // dilate the rectangle with d meter
    //approx 1 latitude degree = 111km
    // approx 1 longitude degree =111.413*cos(phi)-0.094*cos(3*phi) where phi is latitude
    val newLatMin = bottomLeft.lat - d / 111000
    val newLatMax = topRight.lat + d / 111000
    val newLongMin = bottomLeft.lon - (111413 * cos(bottomLeft.lat.toRadians) - 94 * cos(3 * bottomLeft.lat.toRadians))
    val newLongMax = topRight.lon - (111.413 * cos(topRight.lat.toRadians) - 0.094 * cos(3 * topRight.lat.toRadians))
    Rectangle(Point(newLongMin, newLatMin), Point(newLongMax, newLatMax))
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

  def addPointAttr(points: Array[Point]): Rectangle = {
    var s = new Array[String](0)
    for (p <- points) s = s :+ p.lon.toString + "," + p.lat.toString
    this.attr += ("points" -> s)
    this
  }
  var trajectory: Trajectory = Trajectory(0,0,0,Array(Point(0,0)))
  def addTrajAttr(traj: Trajectory): Rectangle = {
    this.trajectory = traj
    this
  }

  val vertices = Array(Point(x_min, y_min), Point(x_min, y_max), Point(x_max, y_max), Point(x_max, y_min))
  val edges = Array(Line(vertices(0), vertices(1)), Line(vertices(1), vertices(2)), Line(vertices(2), vertices(3)), Line(vertices(3), vertices(0)))
  val diagonals = Array(Line(vertices(0), vertices(2)), Line(vertices(1), vertices(3)))
}
