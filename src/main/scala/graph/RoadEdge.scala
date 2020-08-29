package main.scala.graph

final case class RoadEdge(id: String,
                          from: String,
                          to: String,
                          midLat: Double,
                          midLon: Double,
                          length: Double,
                          gpsArray: Array[(Double, Double)])