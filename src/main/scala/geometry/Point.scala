package geometry

import geometry.Distances.greatCircleDistance

case class Point(coordinates: Array[Double],
                 var t: Long = 0,
                 var id: String = "0",
                 attributes: Map[String, String] = Map())
  extends Shape with Serializable {

  //  require(coordinates.length == 2, s"Point should have 2 dimensions while " +
  //    s"${coordinates.mkString("Array(", ", ", ")")} has ${coordinates.length} dimensions.")

  def hasTimestamp: Boolean = t != 0L

  var timeStamp: (Long, Long) = (t, t)

  def dimensions: Int = {
    if (hasTimestamp) 3
    else 2
  }

  def x: Double = coordinates(0)

  def y: Double = coordinates(1)

  def lon: Double = coordinates(0)

  def lat: Double = coordinates(1)

  def +(other: Point): Point = Point((coordinates, other.coordinates).zipped.map(_ + _), t + other.t)

  def -(other: Point): Point = Point((coordinates, other.coordinates).zipped.map(_ - _), t + other.t)

  // deprecated
  def dot(other: Point): Double = {
    if (hasTimestamp && other.hasTimestamp) x * other.x + y * other.y + t * other.t
    else x * other.x + y * other.y
  }

  def *(other: Point): Double = (coordinates, other.coordinates).zipped.map(_ * _).sum + t * other.t

  def *(scalar: Double): Point = Point(coordinates.map(_ * scalar), (t * scalar).toLong)

  def normSquare: Double = coordinates.map(scala.math.pow(_, 2)).sum

  override def geoDistance(other: Shape): Double = other match {
    case p: Point => greatCircleDistance(this, p)
    case r: Rectangle => greatCircleDistance(this, r.center())
    case l: Line => l.geoDistance(this)
    case c: Cube => greatCircleDistance(this, c.toRectangle.center())
  }

  override def minDist(other: Shape): Double = other match {
    case p: Point => minDist(p)
    case r: Rectangle => r.minDist(this)
    case l: Line => l.minDist(this)
    case c: Cube => c.minDist(this)
  }

  def minDist(other: Point): Double = {
    require(coordinates.length == other.coordinates.length)
    var ans = 0.0
    for (i <- coordinates.indices) {
      ans += (coordinates(i) - other.coordinates(i)) * (coordinates(i) - other.coordinates(i))
    }
    Math.sqrt(ans)
  }

  override def intersect(other: Shape): Boolean = other match {
    case p: Point => coordinates sameElements p.coordinates
    case r: Rectangle => this.inside(r)
    case l: Line => l.intersect(this)
    case c: Cube => this.inside(c.toRectangle)
  }

  def ==(other: Point): Boolean = { // timestamp not considered
    if (coordinates.deep == other.coordinates.deep) false
    else true
  }

  def <=(other: Point): Boolean = {
    if (other.x > x || other.y > y) false
    else true
  }

  def calSpeed(other: Point): Double = {
    if (other.t == this.t) 0
    else this.geoDistance(other) / (other.t - this.t)
  }

  def setID(i: String): Point = {
    id = i
    this
  }

  def setTimeStamp(t: Long): Point = {
    timeStamp = (t, t)
    this
  }

  def setTimeStamp(t: (Long, Long)): Point = {
    timeStamp = t
    this
  }

  override def inside(rectangle: Rectangle): Boolean = {
    if (x >= rectangle.xMin && x < rectangle.xMax && y >= rectangle.yMin && y < rectangle.yMax) true
    else false
  }

  override def mbr: Rectangle = Rectangle(Array(x, y, x, y))

  override def center(): Point = this

  override def toString = s"Point($x,$y,$timeStamp,id=$id)"

  def setAttributes(a: Map[String, String]): Point = this.copy(attributes = a)
}