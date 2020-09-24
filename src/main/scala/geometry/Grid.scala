package main.scala.geometry

import math.{min, max, ceil, floor}

case class Grid(minLon: Double, minLat: Double, maxLon: Double, maxLat: Double, gridSize: Double)
  extends Serializable {
  val gridStride: Double = gridSize * (360.0/40075.0)  // 40075 is the circumference of the Earth in kilometers
  val gridNumOverLon: Int = ceil((maxLon - minLon) / gridStride).toInt
  val gridNumOverLat: Int = ceil((maxLat - minLat) / gridStride).toInt
  val gridNum: Int =  gridNumOverLon * gridNumOverLat
  val grids: Map[GridId, GridBoundary] = buildSimpleGrids()

  /**
  A grid's index, starting from 0 at the bottom left point (minLon, minLat)
    x is the grid index over longitude
    y is the grid index over latitude
   */
  final case class GridId(x: Int, y: Int)

  /**
  A grid's boundary, represented by the gps of its bottom-left point and upper-right point
   */
  final case class GridBoundary(bottomLeftLon: Double, bottomLeftLat: Double,
                                upperRightLon: Double, upperRightLat: Double)


  def buildSimpleGrids(): Map[GridId, GridBoundary] = {
    (for {
      idOverLat <- Range(0, gridNumOverLat)
      idOverLon <- Range(0, gridNumOverLon)
      bottomLeftLon = minLon + idOverLon*gridStride
      bottomLeftLat = minLat + idOverLat*gridStride
      upperRightLon = bottomLeftLon + gridStride
      upperRightLat = bottomLeftLat + gridStride
    } yield GridId(idOverLon, idOverLat) -> GridBoundary(
      bottomLeftLon, bottomLeftLat,
      upperRightLon, upperRightLat)
      ).toMap
  }

  def getSimpleGrid(p: Point): GridId = {
    val lon = p.lon
    val lat = p.lat
    var x = 0
    var y = 0
    if (lon < minLon || lon > maxLon || lat < minLat || lat > maxLat ) {
      throw new Exception(s"Input Error: input point (lon=$lon, lat=$lat) " +
        s"exceeds map boundary (minLon=$minLon, minLat=$minLat, maxLon=$maxLon, maxLat=$maxLat)")
    } else {
      x += floor((lon-minLon)/gridStride).toInt
      y += floor((lat-minLat)/gridStride).toInt
    }
    GridId(x, y)
  }

  def getSurroundingSimpleGrids(gridId: GridId, radius: Int): Array[GridId] = {
    if (radius < 0) {
      throw new Exception(s"Input Error: radius must be a non-negative integer, but got $radius")
    }

    val base = (-radius to radius).toArray
    for {
    x <- base.map(_ + gridId.x).filter(_ >= 0).filter(_ < gridNumOverLon)
    y <- base.map(_ + gridId.y).filter(_ >= 0).filter(_ < gridNumOverLat)
    } yield GridId(x, y)
  }

  def getSurroundingSimpleGrids(sourceGridId: GridId, targetGridId: GridId, radius: Int): Array[GridId] = {
    if (sourceGridId == targetGridId) getSurroundingSimpleGrids(sourceGridId, radius)
    else {
      val lx = max(min(sourceGridId.x, targetGridId.x) - radius, 0)
      val ly = max(min(sourceGridId.y, targetGridId.y) - radius, 0)
      val hx = min(max(sourceGridId.x, targetGridId.x) + radius, gridNumOverLon)
      val hy = min(max(sourceGridId.y, targetGridId.y) + radius, gridNumOverLat)
      for {
        x <- (lx to hx).toArray
        y <- (ly to hy).toArray
      } yield GridId(x, y)
    }
  }

}
