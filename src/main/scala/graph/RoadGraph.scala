package main.scala.graph

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.math.{ceil, floor, sin, cos, abs, acos}


class RoadGraph(vertexes: Array[RoadVertex], edges: Array[RoadEdge]) extends Serializable {
  // parameters
  val gridSize: Double = 0.1 // kilometers

  // fields
  val minLat: Double = vertexes.map(x => x.lat).min
  val minLon: Double = vertexes.map(x => x.lon).min
  val maxLat: Double = vertexes.map(x => x.lat).max
  val maxLon: Double = vertexes.map(x => x.lon).max
  val gridStride: Double = gridSize * (360.0/40075.0)  // 40075 is the circumference of the Earth in kilometers
  val gridNumOverLat: Int = ceil((maxLat - minLat) / gridStride).toInt
  val gridNumOverLon: Int = ceil((maxLon - minLon) / gridStride).toInt
  val gridNum: Int = gridNumOverLat * gridNumOverLon
  val grids: Map[GridId, GridBoundary] = buildSimpleGrids()
  val grid2Vertex: Map[GridId, Array[RoadVertex]] = buildGrid2Vertex()
  val g: RouteGraph[String] = buildGraph()
  val edgeId2Length: Map[String, Double] = edges.map(x => x.id -> x.length).toMap

  /**
    A grid's index, starting from 0 at the bottom left point (minLat, minLon)
    x is the grid index over longitude
    y is the grid index over latitude
   */
  final case class GridId(x: Int, y: Int)

  /**
    A grid's boundary, represented by the gps of its bottom-left point and upper-right point
   */
  final case class GridBoundary(bottomLeftLat: Double, bottomLeftLon: Double,
                                upperRightLat: Double, upperRightLon: Double)


  final case class RouteGraph[N](succs: Map[N, Map[N, Int]]) extends Graph[N] {
    def apply(n: N): Map[N, Int] = succs.getOrElse(n, Map.empty)

    def nodes: Set[N] = succs.keySet
  }


  def buildSimpleGrids(): Map[GridId, GridBoundary] = {
    (for {
      idOverLat <- Range(0, gridNumOverLat)
      idOverLon <- Range(0, gridNumOverLon)
      bottomLeftLat = minLat + idOverLat*gridStride
      bottomLeftLon = minLon + idOverLon*gridStride
      upperRightLat = bottomLeftLat + gridStride
      upperRightLon = bottomLeftLon + gridStride
    } yield GridId(idOverLat, idOverLon) -> GridBoundary(bottomLeftLat, bottomLeftLon,
                                                         upperRightLat, upperRightLon)
    ).toMap
  }

  def buildGrid2Vertex(): Map[GridId, Array[RoadVertex]] = {
    val vertex2grid = vertexes.map(x => x -> getSimpleGrid(x.lat, x.lon)).toMap
    val grid2vertex = vertex2grid.groupBy(_._2).map { case (k, v) =>
      k -> v.keys.toArray
    }
    grid2vertex.withDefaultValue(Array.empty)  // returns an empty Array when the input key doesn't exist
  }

  def buildGraph() : RouteGraph[String] = {
    val gInfo = edges.map(x => x.from -> (x.to, x.length.toInt)).groupBy(_._1).map{ case (k,v) =>
      k -> v.map(x => x._2._1 -> x._2._2).toMap
    }
    RouteGraph(gInfo)
  }

  def getSimpleGrid(lat: Double, lon: Double): GridId = {
    var y = 0
    var x = 0
    if (lat < minLat || lat > maxLat || lon < minLon || lon > maxLon) {
      throw new Exception(s"Input Error: input point ($lat, $lon) " +
        s"exceeds map boundary ($minLat, $minLon, $maxLat, $maxLon)")
    } else {
      y += floor((lat-minLat)/gridStride).toInt
      x += floor((lon-minLon)/gridStride).toInt
    }
    GridId(x, y)
  }

  def getSurroundingSimpleGrids(gridId: GridId): Array[GridId] = for {
      y <- Array(-1, 0, 1).map(_ + gridId.y).filter(_ >= 0).filter(_ < gridNumOverLat)
      x <- Array(-1, 0, 1).map(_ + gridId.x).filter(_ >= 0).filter(_ < gridNumOverLon)
  } yield GridId(x, y)

  def greatCircleDistance(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double ={
    val r = 6371009 // earth radius in meter
    val phi1 = lat1.toRadians
    val lambda1 = lon1.toRadians
    val phi2 = lat2.toRadians
    val lambda2 = lon2.toRadians
    val deltaSigma = acos(sin(phi1) * sin(phi2) + cos(phi1) * cos(phi2) * cos(abs(lambda2 - lambda1)))
    r * deltaSigma
  }

  def getNearestVertex(lat: Double, lon: Double, k: Int): Array[(RoadVertex, Double)] = {
    val grid = getSimpleGrid(lat, lon)
    val grids = getSurroundingSimpleGrids(grid)
    val vertexes = grids.flatMap(x => grid2Vertex(x))
    val distance = vertexes.map(x => x -> greatCircleDistance(lat, lon, x.lat, x.lon)).sortBy(_._2)
    distance.take(k)
  }

  def getShortestPath(sourceVertexId: String, targetVertexId: String): Option[List[String]] = {
    val router = DijkstraPriorityMap
    router.shortestPath(g)(sourceVertexId, targetVertexId)
  }

  def getShortestPathLength(sourceVertexId: String, targetVertexId: String): Double = {
    val path = getShortestPath(sourceVertexId, targetVertexId)
    val length = path match {
      case Some(l) => l.sliding(2).map(x => edgeId2Length(s"${x(0)}-${x(1)}")).sum
      case None => Double.MaxValue
    }
    length
  }

}

object RoadGraph {
  def fromCSV(csvFilePath: String) : (Array[RoadVertex], Array[RoadEdge]) = {
    val source = Source.fromFile(csvFilePath)

    var vertexArrayBuffer = ArrayBuffer[RoadVertex]()
    var edgeArrayBuffer = ArrayBuffer[RoadEdge]()

    for (line <- source.getLines) {
      if (line.startsWith("node")) {
        val Array(_, nodeId, nodeLat, nodeLon) = line.split(",").map(_.trim)
        vertexArrayBuffer += RoadVertex(nodeId,
                                        nodeLat.toDouble,
                                        nodeLon.toDouble)
      } else if (line.startsWith("edge")) {
        val Array(_, fromNodeId, toNodeId, isOneWay, length, gpsString) = line.split(",")
                                                                              .map(_.trim)
        edgeArrayBuffer += RoadEdge(s"$fromNodeId-$toNodeId",
                                    fromNodeId,
                                    toNodeId,
                                    length.toDouble,
                                    gpsString)
        if (isOneWay.toInt == 0) {
          edgeArrayBuffer += RoadEdge(s"$toNodeId-$fromNodeId",
                                      toNodeId,
                                      fromNodeId,
                                      length.toDouble,
                                      gpsString)}
      } else {
        throw new Exception(s"CSV Parsing Error: unknown type in line: $line")
      }
    }
    source.close

    (vertexArrayBuffer.toArray,edgeArrayBuffer.toArray)
  }

  def apply(sourceFilePath: String): RoadGraph = {
    val (vertexes, edges) = fromCSV(sourceFilePath)
    val rg = new RoadGraph(vertexes, edges)
    rg
  }

}


