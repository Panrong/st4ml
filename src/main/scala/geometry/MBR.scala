package geometry

case class MBR(low: Point, high: Point) extends Shape {
  require(low.dimensions == high.dimensions)
  require(low <= high)

  val dimensions: Int = low.dimensions
  val centroid = new Point((low.x + high.x) / 2, (low.y + high.y) / 2)

  override def intersect(other: Shape): Boolean = {
    other match {
      case p: Point => contains(p)
      case mbr: MBR => intersects(mbr)
    }
  }

  override def minDist(other: Shape): Double = {
    other match {
      case p: Point => minDist(p)
      case mbr: MBR => minDist(mbr)
    }
  }

  def intersects(other: MBR): Boolean = {
    require(low.coord.length == other.low.coord.length)
    for (i <- low.coord.indices)
      if (low.coord(i) > other.high.coord(i) || high.coord(i) < other.low.coord(i)) {
        return false
      }
    true
  }

  def contains(p: Point): Boolean = {
    require(low.coord.length == p.coord.length)
    for (i <- p.coord.indices)
      if (low.coord(i) > p.coord(i) || high.coord(i) < p.coord(i)) {
        return false
      }
    true
  }

  def minDist(p: Point): Double = {
    require(low.coord.length == p.coord.length)
    var ans = 0.0
    for (i <- p.coord.indices) {
      if (p.coord(i) < low.coord(i)) {
        ans += (low.coord(i) - p.coord(i)) * (low.coord(i) - p.coord(i))
      } else if (p.coord(i) > high.coord(i)) {
        ans += (p.coord(i) - high.coord(i)) * (p.coord(i) - high.coord(i))
      }
    }
    Math.sqrt(ans)
  }

  def maxDist(p: Point): Double = {
    require(low.coord.length == p.coord.length)
    var ans = 0.0
    for (i <- p.coord.indices) {
      ans += Math.max((p.coord(i) - low.coord(i)) * (p.coord(i) - low.coord(i)),
        (p.coord(i) - high.coord(i)) * (p.coord(i) - high.coord(i)))
    }
    Math.sqrt(ans)
  }

  def minDist(other: MBR): Double = {
    require(low.coord.length == other.low.coord.length)
    var ans = 0.0
    for (i <- low.coord.indices) {
      var x = 0.0
      if (other.high.coord(i) < low.coord(i)) {
        x = Math.abs(other.high.coord(i) - low.coord(i))
      } else if (high.coord(i) < other.low.coord(i)) {
        x = Math.abs(other.low.coord(i) - high.coord(i))
      }
      ans += x * x
    }
    Math.sqrt(ans)
  }

  def area: Double = low.coord.zip(high.coord).map(x => x._2 - x._1).product

  def calcRatio(query: MBR): Double = {
    val intersect_low = low.coord.zip(query.low.coord).map(x => Math.max(x._1, x._2))
    val intersect_high = high.coord.zip(query.high.coord).map(x => Math.min(x._1, x._2))
    val diff_intersect = intersect_low.zip(intersect_high).map(x => x._2 - x._1)
    if (diff_intersect.forall(_ > 0)) 1.0 * diff_intersect.product / area
    else 0.0
  }

  override def toString: String = "MBR(" + low.toString + "," + high.toString + ")"

  def getMBR: MBR = this.copy()
}
