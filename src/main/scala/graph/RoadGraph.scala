package main.scala.graph

import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.math.{abs, acos, ceil, cos, floor, sin, min, max}

import SpatialDistances.greatCircleDistance

class RoadGraph(vertexes: Array[RoadVertex], edges: Array[RoadEdge], gridSize: Double) extends Serializable {
  // parameters
  //val gridSize: Double = 0.1 // kilometers

  // fields
  val minLon: Double = min(vertexes.map(_.lon).min, edges.map(_.midLon).min)
  val minLat: Double = min(vertexes.map(_.lat).min, edges.map(_.midLat).min)
  val maxLon: Double = max(vertexes.map(_.lon).max, edges.map(_.midLon).max)
  val maxLat: Double = max(vertexes.map(_.lat).max, edges.map(_.midLat).max)
  val id2vertex: Map[String, RoadVertex] = vertexes.map(x => x.id -> x).toMap
  val id2edge: Map[String, RoadEdge] = edges.map(x => x.id -> x).toMap

  val gridStride: Double = gridSize * (360.0/40075.0)  // 40075 is the circumference of the Earth in kilometers
  val gridNumOverLon: Int = ceil((maxLon - minLon) / gridStride).toInt
  val gridNumOverLat: Int = ceil((maxLat - minLat) / gridStride).toInt
  val gridNum: Int =  gridNumOverLon * gridNumOverLat
  val grids: Map[GridId, GridBoundary] = buildSimpleGrids()
  val grid2Vertex: Map[GridId, Array[RoadVertex]] = buildGrid2Vertex()
  val grid2Edge: Map[GridId, Array[RoadEdge]] = buildGrid2Edge()

  val g: RouteGraph[String] = buildGraph()

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


  /**
    RouteGraph for calculating dijkstra and shortest path
   */
  final case class RouteGraph[N](succs: Map[N, Map[N, Int]]) extends Graph[N] {
    def apply(n: N): Map[N, Int] = succs.getOrElse(n, Map.empty)

    def nodes: Set[N] = succs.keySet
  }


  def buildSimpleGrids(): Map[GridId, GridBoundary] = {
    (for {
      idOverLat <- Range(0, gridNumOverLat)
      idOverLon <- Range(0, gridNumOverLon)
      bottomLeftLon = minLon + idOverLon*gridStride
      bottomLeftLat = minLat + idOverLat*gridStride
      upperRightLon = bottomLeftLon + gridStride
      upperRightLat = bottomLeftLat + gridStride
    } yield GridId(idOverLon, idOverLat) -> GridBoundary(bottomLeftLon, bottomLeftLat,
                                                         upperRightLon, upperRightLat)
    ).toMap
  }

  def buildGrid2Vertex(): Map[GridId, Array[RoadVertex]] = {
    val vertex2grid = vertexes.map(x => x -> getSimpleGrid(x.lon, x.lat)).toMap
    val grid2vertex = vertex2grid.groupBy(_._2).map { case (k, v) =>
      k -> v.keys.toArray
    }
    grid2vertex
  }

  def buildGrid2Edge(): Map[GridId, Array[RoadEdge]] = {
    val edge2grid = edges.map(x => x -> getSimpleGrid(x.midLon, x.midLat)).toMap
    val grid2edge = edge2grid.groupBy(_._2).map { case (k, v) =>
      k -> v.keys.toArray
    }
    grid2edge
  }

  def buildGraph() : RouteGraph[String] = {
    val gInfo = edges.map(x => x.from -> (x.to, x.length.toInt)).groupBy(_._1).map{ case (k,v) =>
      k -> v.map(x => x._2._1 -> x._2._2).toMap
    }
    RouteGraph(gInfo)
  }

  def getSimpleGrid(lon: Double, lat: Double): GridId = {
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

  def getSurroundingSimpleGrids(gridId: GridId): Array[GridId] = for {
    x <- Array(-1, 0, 1).map(_ + gridId.x).filter(_ >= 0).filter(_ < gridNumOverLon)
    y <- Array(-1, 0, 1).map(_ + gridId.y).filter(_ >= 0).filter(_ < gridNumOverLat)
  } yield GridId(x, y)


  def getNearestVertex(lon: Double, lat: Double, k: Int): Array[(RoadVertex, Double)] = {
    val grid = getSimpleGrid(lon, lat)
    val grids = getSurroundingSimpleGrids(grid)
    val vertexes = grids.flatMap(x => grid2Vertex.getOrElse(x, Array.empty))
    val distance = vertexes.map(x => x -> greatCircleDistance(Point(lon, lat), Point(x.lon, x.lat))).sortBy(_._2)
    distance.take(k)
  }

  def getNearestEdge(lon: Double, lat: Double, k: Int): Array[(RoadEdge, Double)] = {
    val grid = getSimpleGrid(lon, lat)
    val grids = getSurroundingSimpleGrids(grid)
    val edges = grids.flatMap(x => grid2Edge.getOrElse(x, Array.empty))
    val distance = edges.map(x => x -> greatCircleDistance(Point(lon, lat), Point(x.midLon, x.midLat))).sortBy(_._2)
    distance.take(k)
  }

  def getShortestPath(sourceVertexId: String, targetVertexId: String): Option[List[String]] = {
    val router = DijkstraPriorityMap
    router.shortestPath(g)(sourceVertexId, targetVertexId)
  }

  def getShortestPathAndLength(sourceVertexId: String, targetVertexId: String): (List[String], Double) = {
    val res = getShortestPath(sourceVertexId, targetVertexId)
    val path = res match {
      case Some(l) => l
      case None => List.empty
    }
    val length = path match {
      case Nil => Double.MaxValue
      case l if l.size == 1 => 0
      case l if l.size > 1 => l.sliding(2).map(x => id2edge(s"${x(0)}-${x(1)}").length).sum
    }
    (path, length)
  }

}

object RoadGraph {
  def fromCSV(csvFilePath: String) : (Array[RoadVertex], Array[RoadEdge]) = {
    val source = Source.fromFile(csvFilePath)

    var vertexArrayBuffer = ArrayBuffer[RoadVertex]()
    var edgeArrayBuffer = ArrayBuffer[RoadEdge]()

    for (line <- source.getLines) {
      if (line.startsWith("node")) {
        val Array(_, nodeId, nodeLon, nodeLat) = line.split(",").map(_.trim)
        vertexArrayBuffer += RoadVertex(nodeId,
                                        nodeLon.toDouble,
                                        nodeLat.toDouble)
      } else if (line.startsWith("edge")) {
        val Array(_, fromNodeId, toNodeId, isOneWay, length, gpsString) = line.split(",")
                                                                              .map(_.trim)

        val gpsArray = gpsString.split("%").map(x => x.split(" ")).map(x => (x(0).toDouble, x(1).toDouble))
        val midLon = gpsArray.map(_._1).sum / gpsArray.length
        val midLat = gpsArray.map(_._2).sum / gpsArray.length

        edgeArrayBuffer += RoadEdge(s"$fromNodeId-$toNodeId",
                                    fromNodeId,
                                    toNodeId,
                                    midLon,
                                    midLat,
                                    length.toDouble,
                                    gpsArray)
        if (isOneWay.toInt == 0) {
          edgeArrayBuffer += RoadEdge(s"$toNodeId-$fromNodeId",
                                      toNodeId,
                                      fromNodeId,
                                      midLon,
                                      midLat,
                                      length.toDouble,
                                      gpsArray)}
      } else {
        throw new Exception(s"CSV Parsing Error: unknown type in line: $line")
      }
    }
    source.close

    (vertexArrayBuffer.toArray,edgeArrayBuffer.toArray)
  }

  def apply(sourceFilePath: String, gridSize: Double = 0.1): RoadGraph = {
    val (vertexes, edges) = fromCSV(sourceFilePath)
    val rg = new RoadGraph(vertexes, edges, gridSize)
    rg
  }

}


