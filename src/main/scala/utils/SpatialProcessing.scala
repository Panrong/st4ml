package utils

import geometry.Rectangle

object SpatialProcessing {
  def gridPartition(sRange: Array[Double], gridSize: Int): Map[Int, Rectangle] = {
    val longInterval = (sRange(2) - sRange(0)) / gridSize
    val latInterval = (sRange(3) - sRange(1)) / gridSize
    val longSeparations = (0 until gridSize)
      .map(t => (sRange(0) + t * longInterval, sRange(0) + (t + 1) * longInterval)).toArray
    val latSeparations = (0 until gridSize)
      .map(t => (sRange(1) + t * latInterval, sRange(1) + (t + 1) * latInterval)).toArray
    val rectangles = for ((longMin, longMax) <- longSeparations;
                          (latMin, latMax) <- latSeparations)
      yield Rectangle(Array(longMin, latMin, longMax, latMax))
    rectangles.zipWithIndex.map(_.swap).toMap
  }
}
