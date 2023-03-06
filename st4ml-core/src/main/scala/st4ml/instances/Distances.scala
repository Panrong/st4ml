package st4ml.instances

import math.{abs, acos, cos, sin, sqrt, pow}

/**
 * Defining various distance calculation functions.
 */
object Distances {
  /**
   * Calculating euclidean distance of two locations (points).
   * @param x1 : the x value of the first location.
   * @param y1 : the y value of the first location.
   * @param x2 : the x value of the second location.
   * @param y2 : the y value of the second location.
   * @return euclidean distance of the two locations.
   */
  def euclideanDistance(x1: Double, y1: Double, x2: Double, y2: Double): Double = {
    sqrt(pow(x1 - x2, 2) + pow(y1 - y2, 2))
  }

  /**
   * Calculating great-circle distance of two locations (points).
   * @param lon1 : the longitude value of the first location.
   * @param lat1 : the latitude value of the first location.
   * @param lon2 : the longitude value of the second location.
   * @param lat2 : the latitude value of the second location.
   * @return great circle distance of the two locations (in meter).
   */
  def greatCircleDistance(lon1: Double, lat1: Double, lon2: Double, lat2: Double): Double = {
    val r = 6371009 // earth radius in meter
    val phi1 = lat1.toRadians
    val lambda1 = lon1.toRadians
    val phi2 = lat2.toRadians
    val lambda2 = lon2.toRadians
    val deltaSigma = acos(sin(phi1) * sin(phi2) + cos(phi1) * cos(phi2) * cos(abs(lambda2 - lambda1)))
    r * deltaSigma
  }
}
