package instances

import math.{abs, acos, cos, sin, sqrt, pow}

object Distances {
  def euclideanDistance(x1: Double, y1: Double, x2: Double, y2: Double): Double = {
    sqrt(pow(x1 - x2, 2) + pow(y1 - y2, 2))
  }

  def greatCircleDistance(x1: Double, y1: Double, x2: Double, y2: Double): Double = {
    val r = 6371009 // earth radius in meter
    val phi1 = y1.toRadians
    val lambda1 = x1.toRadians
    val phi2 = y2.toRadians
    val lambda2 = x2.toRadians
    val deltaSigma = acos(sin(phi1) * sin(phi2) + cos(phi1) * cos(phi2) * cos(abs(lambda2 - lambda1)))
    r * deltaSigma
  }

}
