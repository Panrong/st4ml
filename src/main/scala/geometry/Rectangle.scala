package geometry

import scala.math.{max, min, cos}

case class Rectangle(coordinates: Array[Double], ID: Long = 0) extends Shape with Serializable {
  require(coordinates.length == 4,
    s"Rectangle should have 4 coordinates(xMin, yMin, xMax, yMax) " +
      s"while ${coordinates.mkString("Array(", ", ", ")")} has ${coordinates.length} dimensions.")
  val xMin: Double = coordinates(0)
  val xMax: Double = coordinates(2)
  val yMin: Double = coordinates(1)
  val yMax: Double = coordinates(3)

  val low: Point = Point(Array(xMin, yMin))
  val high: Point = Point(Array(xMax, yMax))

  var timeStamp = (0L, 0L)

  require(xMin <= xMax && yMin <= yMax, s"The order should be (xMin, yMin, xMax, yMax). Input($xMin, $yMin, $xMax, $yMax)")

  val area: Double = (xMax - xMin) * (yMax - yMin)

  override def center(): Point = Point(Array((xMax + xMin) / 2, (yMax + yMin) / 2))

  override var id: Long = ID

  override def intersect(other: Shape): Boolean = other match {
    case p: Point => p.intersect(this)
    //    case r: Rectangle => this.overlappingArea(r) > 0
    case r: Rectangle => {
      require(low.coordinates.length == r.low.coordinates.length)
      for (i <- low.coordinates.indices)
        if (low.coordinates(i) > r.high.coordinates(i) || high.coordinates(i) < r.low.coordinates(i)) {
          return false
        }
      true
    }
    case l: Line => l.intersect(this)
    case c: Cube => c.toRectangle.isOverlap(this)
  }

  override def geoDistance(other: Shape): Double = other.geoDistance(center())

  override def minDist(other: Shape): Double = {
    other match {
      case p: Point => minDist(p)
      case r: Rectangle => minDist(r)
      case l: Line => l.minDist(this)
      case c: Cube => minDist(c.toRectangle)
    }
  }

  def minDist(p: Point): Double = {
    require(low.coordinates.length == p.coordinates.length)
    var ans = 0.0
    for (i <- p.coordinates.indices) {
      if (p.coordinates(i) < low.coordinates(i)) {
        ans += (low.coordinates(i) - p.coordinates(i)) * (low.coordinates(i) - p.coordinates(i))
      } else if (p.coordinates(i) > high.coordinates(i)) {
        ans += (p.coordinates(i) - high.coordinates(i)) * (p.coordinates(i) - high.coordinates(i))
      }
    }
    Math.sqrt(ans)
  }

  def minDist(other: Rectangle): Double = {
    require(low.coordinates.length == other.low.coordinates.length)
    var ans = 0.0
    for (i <- low.coordinates.indices) {
      var x = 0.0
      if (other.high.coordinates(i) < low.coordinates(i)) {
        x = Math.abs(other.high.coordinates(i) - low.coordinates(i))
      } else if (high.coordinates(i) < other.low.coordinates(i)) {
        x = Math.abs(other.low.coordinates(i) - high.coordinates(i))
      }
      ans += x * x
    }
    Math.sqrt(ans)
  }


  def assignID(i: Long): Rectangle = {
    id = i
    this
  }

    def dilate(d: Double): Rectangle = {
      // dilate the rectangle with d meter
      //approx 1 latitude degree = 111km
      // approx 1 longitude degree =111.413*cos(phi)-0.094*cos(3*phi) where phi is latitude
      val newLatMin = yMin - d / 111000
      val newLatMax = yMax + d / 111000
      val newLonMin = xMin - d / (111413 * cos(yMin.toRadians) - 94 * cos(3 * yMin.toRadians))
      val newLonMax = xMax + d / (111413 * cos(yMax.toRadians) - 94 * cos(3 * yMax.toRadians))
      Rectangle(Array(newLonMin, newLatMin, newLonMax, newLatMax))
    }

  def overlappingArea(r: Rectangle): Double = {
    val overlapX = max((xMax - xMin) + (r.xMax - r.xMin) - (max(xMax, r.xMax) - min(xMin, r.xMin)), 0)
    val overlapY = max((yMax - yMin) + (r.yMax - r.yMin) - (max(yMax, r.yMax) - min(yMin, r.yMin)), 0)
    overlapX * overlapY
  }

  // Two rectangles intersect only on boundaries are considered as true
  def isOverlap(other: Rectangle): Boolean = {
    require(low.coordinates.length == other.low.coordinates.length)
    for (i <- low.coordinates.indices)
      if (low.coordinates(i) > other.high.coordinates(i) || high.coordinates(i) < other.low.coordinates(i)) {
        return false
      }
    true
  }

  def overlappingRatio(r: Rectangle): Double = {
    val intersect_low = low.coordinates.zip(r.low.coordinates).map(x => Math.max(x._1, x._2))
    val intersect_high = high.coordinates.zip(r.high.coordinates).map(x => Math.min(x._1, x._2))
    val diff_intersect = intersect_low.zip(intersect_high).map(x => x._2 - x._1)
    if (diff_intersect.forall(_ > 0)) 1.0 * diff_intersect.product / area
    else 0.0
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


  override val mbr: Rectangle = this


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

  def setTimeStamp(t: (Long, Long)): Rectangle = {
    timeStamp = t
    this
  }

  def setTimeStamp(t: Long): Rectangle = {
    timeStamp = (t, t)
    this
  }

  override def toString = s"${this.xMin},${this.yMin},${this.xMax},${this.yMax}"

  /** find the reference point of the overlapping of two rectangles (the left bottom point) */
  def referencePoint(other: Shape): Option[Point] = {
    if (!this.intersect(other.mbr)) None
    else {
      val xs = Array(this.xMin, this.xMax, other.mbr.xMin, other.mbr.xMax).sorted
      val ys = Array(this.yMin, this.yMax, other.mbr.yMin, other.mbr.yMax).sorted
      Some(Point(Array(xs(1), ys(1))))
    }
  }
}