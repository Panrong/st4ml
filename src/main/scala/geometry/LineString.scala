package main.scala.geometry


case class LineString(points: Array[Point]) extends Serializable {
  def lines: Array[Line] = this.points.sliding(2).map(x => Line(x(0), x(1))).toArray

  def midPointDistance(p: Point): (Double, Point) = {
    val distances = this.lines.map(_.midPointDistance(p))
    distances.reduceLeft((x, y) => if (x._1 < y._1) x else y)
  }

  def projectionDistance(p: Point): (Double, Point) = {
    val distances = this.lines.map(_.projectionDistance(p))
    distances.reduceLeft((x, y) => if (x._1 < y._1) x else y)
  }

  def mbr: Rectangle = {
    val lons = points.map(x => x.lon).sorted
    val lats = points.map(x => x.lat).sorted
    Rectangle(Point(lons(0), lats(0)), Point(lons.last, lats.last))
  }
}
