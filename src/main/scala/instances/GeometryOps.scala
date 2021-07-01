package instances

import org.locationtech.jts.operation.distance.DistanceOp

trait GeometryOps[+T <: Geometry] extends Serializable {
  def self: T

  def extent: Extent =
    if (self.isEmpty) Extent(0.0, 0.0, 0.0, 0.0)
    else Extent(self.getEnvelopeInternal)

  /** Tests whether this geometry is topologically equal to the input geometry. */
  def ==(geom: Geometry): Boolean = self.equals(geom)

  /** Returns the euclidean distance between centroids of the two geometries */
  def euclidean(geom: Geometry): Double = {
    val p1: Point = self.getCentroid
    val p2: Point = geom.getCentroid
    Distances.euclideanDistance(p1.getX, p1.getY, p2.getX, p2.getY)
  }

  /** Returns the great circle distance between centroids of the two geometries */
  def greatCircle(geom: Geometry): Double = {
    val p1: Point = self.getCentroid
    val p2: Point = geom.getCentroid
    Distances.greatCircleDistance(p1.getX, p1.getY, p2.getX, p2.getY)
  }

  /** Returns the distance between the nearest points on the input geometries */
  def minDistance(geom: Geometry): Double = self.distance(geom)

  /** Returns the nearest points in the input geometries */
  def nearestPoints(geom: Geometry): Array[Point] =
    DistanceOp.nearestPoints(self, geom).map(x =>
      Point(x)
    )
}

trait PointOps extends GeometryOps[Point] {
  def x: Double = self.getX
  def y: Double = self.getY
  def lon: Double = self.getX
  def lat: Double = self.getY

  def +(deltaX: Double, deltaY: Double): Point = Point(
    self.getX + deltaX,
    self.getY + deltaY
  )

  def +(p: Point): Point = Point(
    self.getX + p.getX,
    self.getY + p.getY
  )

  def *(factorX: Double, factorY: Double): Point = Point(
    self.getX * factorX,
    self.getY * factorY
  )

  def *(p: Point): Point = Point(
    self.getX * p.getX,
    self.getY * p.getY
  )

}

trait LineStringOps extends GeometryOps[LineString] {
  def xs: Array[Double] = self.getCoordinates.map(_.x)
  def ys: Array[Double] = self.getCoordinates.map(_.y)
  def lons: Array[Double] = self.getCoordinates.map(_.x)
  def lats: Array[Double] = self.getCoordinates.map(_.y)
}

trait PolygonOps extends GeometryOps[Polygon] {
  def xs: Array[Double] = self.getCoordinates.map(_.x)
  def ys: Array[Double] = self.getCoordinates.map(_.y)
  def lons: Array[Double] = self.getCoordinates.map(_.x)
  def lats: Array[Double] = self.getCoordinates.map(_.y)
}

