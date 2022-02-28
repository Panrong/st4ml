package st4ml.instances

import st4ml.instances.GeometryImplicits.withExtraPointOps
import org.locationtech.jts.geom.Envelope


case class ExtentRangeError(msg: String) extends Exception(msg)

case class Extent(
                   xMin: Double,
                   yMin: Double,
                   xMax: Double,
                   yMax: Double
                 ) extends Ordered[Extent] {

  // Validation
  if (xMin > xMax) {
    throw ExtentRangeError(s"Invalid Extent: xmin must be less than xmax (xmin=$xMin, xmax=$xMax)")
  }
  if (yMin > yMax) {
    throw ExtentRangeError(s"Invalid Extent: ymin must be less than ymax (ymin=$yMin, ymax=$yMax)")
  }

  // Accessors
  def width: Double = xMax - xMin

  def height: Double = yMax - yMin

  def area: Double = width * height

  def isEmpty: Boolean = area == 0

  def jtsEnvelope: Envelope = new Envelope(xMin, xMax, yMin, yMax)

  def min: Point = Point(xMin, yMin)

  def max: Point = Point(xMax, yMax)

  def lowerLeft: Point = Point(xMin, yMin)

  def lowerRight: Point = Point(xMax, yMin)

  def upperLeft: Point = Point(xMin, yMax)

  def upperRight: Point = Point(xMax, yMax)

  def center: Point = Point((xMin + xMax) / 2.0, (yMin + yMax) / 2.0)

  /** Predicates */

  // intersects: shared edges or points are regarded true
  def intersects(other: Extent): Boolean =
    !(other.xMax < xMin || other.xMin > xMax) &&
      !(other.yMax < yMin || other.yMin > yMax)

  def intersects(x: Double, y: Double): Boolean =
    x >= xMin && x <= xMax && y >= yMin && y <= yMax

  def intersects(g: Geometry): Boolean =
    intersects(Extent(g.getEnvelopeInternal))

  // contains: if a point lies on the edge --> false;
  // if other geometries are inside and touch the edge --> true;
  def contains(other: Extent): Boolean = {
    if (isEmpty) false // Empty extent contains nothing
    else
      other.xMin >= xMin &&
        other.xMax <= xMax &&
        other.yMin >= yMin &&
        other.yMax <= yMax
  }

  def contains(x: Double, y: Double): Boolean =
    x > xMin && x < xMax && y > yMin && y < yMax

  def contains(g: Geometry): Boolean =
    this.toPolygon.contains(g)

  /** Ops */
  // the minimum distance between two extents
  def distance(other: Extent): Double = {
    if (intersects(other)) 0
    else {
      val dx = if (xMax < other.xMin) other.xMin - xMax
      else if (xMin > other.xMax) xMin - other.xMax
      else 0.0
      val dy = if (yMax < other.yMin) other.yMin - yMax
      else if (yMin > other.yMax) yMin - other.yMax
      else 0.0
      // if either is zero, the extents overlap either vertically or horizontally
      if (dx == 0.0) dy
      else if (dy == 0.0) dx
      else math.sqrt(dx * dx + dy * dy)
    }
  }

  def intersection(other: Extent): Option[Extent] = {
    val xMinNew = if (xMin > other.xMin) xMin else other.xMin
    val yMinNew = if (yMin > other.yMin) yMax else other.yMin
    val xMaxNew = if (xMax < other.xMax) xMax else other.xMax
    val yMaxNew = if (yMax < other.yMax) yMax else other.yMax
    if (xMinNew < xMaxNew && yMinNew < yMaxNew) Some(Extent(xMinNew, yMinNew, xMaxNew, yMaxNew))
    else None
  }

  def &(other: Extent): Option[Extent] = intersection(other)

  /** Orders two extents by their lower-left corner. The bounding box
   * that is further south (or west in the case of a tie) comes first.
   *
   * If the lower-left corners are the same, the upper-right corners are
   * compared. This is mostly to assure that 0 is only returned when the
   * extents are equal.
   *
   * Return type signals:
   *
   * -1 this extent comes first
   * 0 the extents have the same lower-left corner
   * 1 the other extent comes first
   */
  def compare(other: Extent): Int = {
    var cmp = yMin compare other.yMin
    if (cmp != 0) return cmp

    cmp = xMin compare other.xMin
    if (cmp != 0) return cmp

    cmp = yMax compare other.yMax
    if (cmp != 0) return cmp

    xMax compare other.xMax
  }

  def combine(other: Extent): Extent =
    Extent(
      if (xMin < other.xMin) xMin else other.xMin,
      if (yMin < other.yMin) yMin else other.yMin,
      if (xMax > other.xMax) xMax else other.xMax,
      if (yMax > other.yMax) yMax else other.yMax
    )

  def |(other: Extent): Extent = combine(other)

  def expandToInclude(other: Extent): Extent = combine(other)

  def expandToInclude(x: Double, y: Double): Extent =
    Extent(
      if (xMin < x) xMin else x,
      if (yMin < y) yMin else y,
      if (xMax > x) xMax else x,
      if (yMax > y) yMax else y
    )

  def expandToInclude(p: Point): Extent = expandToInclude(p.x, p.y)

  def expandBy(deltaX: Double, deltaY: Double): Extent =
    Extent(
      xMin - deltaX,
      yMin - deltaY,
      xMax + deltaX,
      yMax + deltaY
    )

  def expandBy(delta: Double): Extent =
    Extent(
      xMin - delta,
      yMin - delta,
      xMax + delta,
      yMax + delta
    )

  def translateBy(deltaX: Double, deltaY: Double): Extent =
    Extent(
      xMin + deltaX,
      yMin + deltaY,
      xMax + deltaX,
      yMax + deltaY
    )

  def translateBy(delta: Double): Extent =
    Extent(
      xMin + delta,
      yMin + delta,
      xMax + delta,
      yMax + delta
    )

  override def equals(other: Any): Boolean =
    other match {
      case o: Extent =>
        xMin == o.xMin && yMin == o.yMin &&
          xMax == o.xMax && yMax == o.yMax
      case _ => false
    }

  override def hashCode(): Int = (xMin, yMin, xMax, yMax).hashCode

  override def toString: String = s"Extent($xMin, $yMin, $xMax, $yMax)"

  def toPolygon: Polygon = {
    val exterior = Seq(
      (xMin, yMin),
      (xMin, yMax),
      (xMax, yMax),
      (xMax, yMin),
      (xMin, yMin)
    )
    Polygon(LineString(exterior))
  }
}

object Extent {
  def apply(e: Envelope): Extent =
    new Extent(e.getMinX, e.getMinY, e.getMaxX, e.getMaxY)

  def apply(s: String): Extent = {
    val Array(xMin, yMin, xMax, yMax) = s.split(",").map(_.toDouble)
    new Extent(xMin, yMin, xMax, yMax)
  }

  def apply(extents: Array[Extent]): Extent = {
    val xMin = extents.map(_.xMin).min
    val xMax = extents.map(_.xMax).max
    val yMin = extents.map(_.yMin).min
    val yMax = extents.map(_.yMax).max
    new Extent(xMin, yMin, xMax, yMax)
  }

  def apply(a: Array[Double]): Extent = {
    new Extent(a(0), a(1), a(2), a(3))
  }

  implicit def toPolygon(extent: Extent): Polygon =
    extent.toPolygon

  implicit def envelope2Extent(envelope: Envelope): Extent =
    Extent(envelope)

}
