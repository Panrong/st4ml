package geometry

import scala.math.{max, min}

case class Rectangle(coordinates: Array[Double], ID: Long = 0) extends Shape with Serializable {
  require(coordinates.length == 4,
    s"Rectangle should have 4 coordinates(xMin, yMin, xMax, yMax) while ${coordinates.mkString("Array(", ", ", ")")} has ${coordinates.length} dimensions.")
  val xMin: Double = coordinates.head
  val xMax: Double = coordinates(2)
  val yMin: Double = coordinates(1)
  val yMax: Double = coordinates(3)

  var timeStamp = 0L

  require(xMin <= xMax && yMin <= yMax, "The order should be (xMin, yMin, xMax, yMax)")

  val area: Double = (xMax - xMin) * (yMax - yMin)

  override def center(): Point = Point(Array((xMax + xMin) / 2, (yMax + yMin) / 2))

  override var id: Long = ID

  override def intersect(other: Shape): Boolean = other match {
    case p: Point => p.intersect(this)
    case r: Rectangle => this.overlap(r) > 0
    case l: Line => l.intersect(this)
  }

  override def geoDistance(other: Shape): Double = other.geoDistance(center())

  def assignID(i: Long): Rectangle = {
    id = i
    this
  }

  //  def dilate(d: Double): Rectangle = {
  //    // dilate the rectangle with d meter
  //    //approx 1 latitude degree = 111km
  //    // approx 1 longitude degree =111.413*cos(phi)-0.094*cos(3*phi) where phi is latitude
  //    val newLatMin = bottomLeft.lat - d / 111000
  //    val newLatMax = topRight.lat + d / 111000
  //    val newLongMin = bottomLeft.lon - (111413 * cos(bottomLeft.lat.toRadians) - 94 * cos(3 * bottomLeft.lat.toRadians))
  //    val newLongMax = topRight.lon - (111.413 * cos(topRight.lat.toRadians) - 0.094 * cos(3 * topRight.lat.toRadians))
  //    Rectangle(Point(newLongMin, newLatMin), Point(newLongMax, newLatMax))
  //  }


  def overlap(r: Rectangle): Double = {
    val overlapX = max((xMax - xMin) + (r.xMax - r.xMin) - (max(xMax, r.xMax) - min(xMin, r.xMin)), 0)
    val overlapY = max((yMax - yMin) + (r.yMax - r.yMin) - (max(yMax, r.yMax) - min(yMin, r.yMin)), 0)
    overlapX * overlapY
  }

  //  def includeEntry(shape: Shape): Rectangle = {
  //    // return a new Rectangle
  //    if (shape.isInstanceOf[Point]) {
  //      val point = shape.asInstanceOf[Point]
  //      val new_x_min = min(point.x, this.x_min)
  //      val new_x_max = max(point.x, this.x_max)
  //      val new_y_min = min(point.y, this.y_min)
  //      val new_y_max = max(point.y, this.y_max)
  //      //println(Point(new_x_min, new_y_min), Point(new_x_max, new_y_max))
  //      Rectangle(Point(new_x_min, new_y_min), Point(new_x_max, new_y_max))
  //    }
  //    else if (shape.isInstanceOf[Rectangle]) {
  //      val rectangle = shape.asInstanceOf[Rectangle]
  //      val new_x_min = min(rectangle.x_min, this.x_min)
  //      val new_x_max = max(rectangle.x_max, this.x_max)
  //      val new_y_min = min(rectangle.y_min, this.y_min)
  //      val new_y_max = max(rectangle.y_max, this.y_max)
  //      Rectangle(Point(new_x_min, new_y_min), Point(new_x_max, new_y_max))
  //    }
  //    else throw new IllegalArgumentException
  //  }
  //
  //  def enlargement(shape: Shape): Double = {
  //    //calculate the enlargement needed to include one entry
  //    val newRectangle = this.includeEntry(shape)
  //    newRectangle.area - this.area
  //  }
  //
  //  def margin(): Double = {
  //    (x_max - x_min + y_max - y_min) * 2
  //  }

  override def inside(r: Rectangle): Boolean = {
    if (xMin >= r.xMin && yMin >= r.yMin && xMax <= r.xMax && yMax <= r.yMax) true
    else false
  }


  override def mbr(): Rectangle = this


  //  def addPointAttr(points: Array[Point]): Rectangle = {
  //    var s = new Array[String](0)
  //    for (p <- points) s = s :+ p.lon.toString + "," + p.lat.toString
  //    this.attr += ("points" -> s)
  //    this
  //  }
  //
  //  var trajectory: Trajectory = Trajectory(0, 0, 0, Array(Point(0, 0)))
  //
  //  def addTrajAttr(traj: Trajectory): Rectangle = {
  //    this.trajectory = traj
  //    this
  //  }

  val vertices: Array[Point] = Array(Point(Array(xMin, yMin)), Point(Array(xMin, yMax)), Point(Array(xMax, yMax)), Point(Array(xMax, yMin)))
  val edges: Array[Line] = Array(Line(vertices(0), vertices(1)), Line(vertices(1), vertices(2)), Line(vertices(2), vertices(3)), Line(vertices(3), vertices(0)))
  val diagonals: Array[Line] = Array(Line(vertices(0), vertices(2)), Line(vertices(1), vertices(3)))

  def assignTimeStamp(t: Long): Rectangle = {
    timeStamp = t
    this
  }


  override def toString = s"${this.xMin},${this.yMin},${this.xMax},${this.yMax}"
}
