package instances

trait GeometryOps[+T <: Geometry] extends Serializable {
  def self: T

  def extent: Extent =
    if (self.isEmpty) Extent(0.0, 0.0, 0.0, 0.0)
    else Extent(self.getEnvelopeInternal)

  def ==(other: Geometry): Boolean = ???
}

trait PointOps extends GeometryOps[Point] {
  def x: Double = self.getCoordinate.getX
  def y: Double = self.getCoordinate.getY
  def lon: Double = self.getCoordinate.getX
  def lat: Double = self.getCoordinate.getY

  def +(deltaX: Double, deltaY: Double): Point = ???
  def *(deltaX: Double, deltaY: Double): Point = ???

  def euclidean(p: Point): Double = ???

  def minDistance(geom: Geometry): Double = ???
}

trait LineStringOps extends GeometryOps[LineString] {

}

trait PolygonOps extends GeometryOps[Polygon] {

}

