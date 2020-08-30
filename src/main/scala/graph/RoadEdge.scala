package main.scala.graph

final case class RoadEdge(id: String,
                          from: String,
                          to: String,
                          midLon: Double,
                          midLat: Double,
                          length: Double,
                          gpsArray: Array[(Double, Double)])