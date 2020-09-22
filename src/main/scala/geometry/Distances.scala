package main.scala.geometry

import math.{sin, cos, acos, abs}

object Distances {
  def greatCircleDistance(p1: Point, p2: Point): Double = {
    val r = 6371009 // earth radius in meter
    val phi1 = p1.lat.toRadians
    val lambda1 = p1.lon.toRadians
    val phi2 = p2.lat.toRadians
    val lambda2 = p2.lon.toRadians
    val deltaSigma = acos(sin(phi1) * sin(phi2) + cos(phi1) * cos(phi2) * cos(abs(lambda2 - lambda1)))
    r * deltaSigma
  }
}
