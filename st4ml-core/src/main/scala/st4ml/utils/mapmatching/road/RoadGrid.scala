package st4ml.utils.mapmatching.road

import st4ml.utils.mapmatching.Grid
import st4ml.instances.{LineString, Point}

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

class RoadGrid(val vertexes: Array[RoadVertex], val edges: Array[RoadEdge],
               minLon: Double, minLat: Double, maxLon: Double, maxLat: Double, gridSize: Double)
  extends Grid(minLon, minLat, maxLon, maxLat, gridSize) {

  // fields
  val id2vertex: Map[String, RoadVertex] = vertexes.map(x => x.id -> x).toMap
  val id2edge: Map[String, RoadEdge] = edges.map(x => x.id -> x).toMap
  val grid2Vertex: Map[GridId, Array[RoadVertex]] = buildGrid2Vertex()
  val grid2Edge: Map[GridId, Array[RoadEdge]] = buildGrid2Edge()

  override def toString: String =
    s"""RoadGrid built with:
       |Grids: ${grids.size}
       |--gridSize: $gridSize in kilometer / $gridStride in decimal
       |--gridNum: $gridNumOverLon(over lon), $gridNumOverLat(over lat)
       |--boundary: $minLon(minLon), $minLat(minLat), $maxLon(maxLon), $maxLat(maxLat)
       |Vertexes: ${vertexes.length}
       |Edges: ${edges.length}
       |""".stripMargin


  def buildGrid2Vertex(): Map[GridId, Array[RoadVertex]] = {
    val vertex2grid = vertexes.map(x => x -> getSimpleGrid(x.point)).toMap
    val grid2vertex = vertex2grid.groupBy(_._2).map { case (k, v) =>
      k -> v.keys.toArray
    }
    grid2vertex
  }

  def buildGrid2Edge(): Map[GridId, Array[RoadEdge]] = {
    val edge2grid = edges.map(x => x -> getSimpleGrid(x.midPoint)).toMap
    val grid2edge = edge2grid.groupBy(_._2).map { case (k, v) =>
      k -> v.keys.toArray
    }
    grid2edge
  }

  def getNearestVertex(p: Point, k: Int, r: Int = 1): Array[(String, Double)] = {
    val grid = getSimpleGrid(p)
    val grids = getSurroundingSimpleGrids(grid, r)
    val vertexes = grids.flatMap(x => grid2Vertex.getOrElse(x, Array.empty))
    vertexes.map(x => x.id -> x.geoDistance(p)).sortBy(_._2).take(k)
  }

  def getNearestEdge(p: Point, k: Int, metric: String = "projection", r: Int = 1): Array[(String, Double, Point)] = {
    val grid = getSimpleGrid(p)
    var grids = getSurroundingSimpleGrids(grid, r)
    var edges = grids.flatMap(x => grid2Edge.getOrElse(x, Array.empty))

    // double the search area if not enough edges
    if (edges.length <= k) {
      grids = getSurroundingSimpleGrids(grid, 2 * r)
      edges = grids.flatMap(x => grid2Edge.getOrElse(x, Array.empty))
    }

    metric match {
      case "projection" => edges.map(_.projectionDistance(p)).sortBy(_._2).take(k)
      case "middle" => edges.map(_.midPointDistance(p)).sortBy(_._2).take(k)
      case _ => throw new Exception(s"Invalid metric error: " +
        s"metric can be either 'projection' or 'middle', but got $metric")
    }
  }

  // TODO: dynamically determine radius
  def getGraphEdgesByPoint(o: Point, d: Point, radius: Int = 5): Array[RoadEdge] = {
    val oGridId = getSimpleGrid(o)
    val dGridId = getSimpleGrid(d)
    val surroundingGrids = getSurroundingSimpleGrids(oGridId, dGridId, radius)
    surroundingGrids.flatMap(x => grid2Edge.getOrElse(x, Array.empty))
  }

  def getGraphEdgesByEdgeId(o: String, d: String, radius: Int = 5): Array[RoadEdge] = {
    val oGridId = getSimpleGrid(id2edge(o).midPoint)
    val dGridId = getSimpleGrid(id2edge(d).midPoint)
    val surroundingGrids = getSurroundingSimpleGrids(oGridId, dGridId, radius)
    surroundingGrids.flatMap(x => grid2Edge.getOrElse(x, Array.empty))
  }

}

object RoadGrid {

  //  implicit def tuple2Array[T: ClassTag](x: (T, T)): Array[T] = Array(x._1, x._2)

  def fromCSV(csvFilePath: String): (Array[RoadVertex], Array[RoadEdge]) = {
    val source = Source.fromFile(csvFilePath)

    val vertexArrayBuffer = ArrayBuffer[RoadVertex]()
    val edgeArrayBuffer = ArrayBuffer[RoadEdge]()

    for (line <- source.getLines) {
      if (line.startsWith("node")) {
        val Array(_, nodeId, nodeLon, nodeLat) = line.split(",").map(_.trim)
        vertexArrayBuffer += RoadVertex(nodeId, Point(nodeLon.toDouble, nodeLat.toDouble))
      } else if (line.startsWith("edge")) {
        val Array(_, fromNodeId, toNodeId, _, length, gpsString) = line.split(",").map(_.trim)

        val gpsArray = gpsString.split("%").map(x => x.split(" "))
        val gpsLineString = LineString(gpsArray.map(x => Point(x(0).toDouble, x(1).toDouble)))

        edgeArrayBuffer += RoadEdge(
          s"$fromNodeId-$toNodeId",
          fromNodeId,
          toNodeId,
          length.toDouble,
          gpsLineString
        )

      } else {
        throw new Exception(s"CSV Parsing Error: unknown type in line: $line")
      }
    }
    source.close

    (vertexArrayBuffer.toArray, edgeArrayBuffer.toArray)
  }

  def apply(sourceFilePath: String, gridSize: Double = 0.1): RoadGrid = {
    val (vertexes, edges) = fromCSV(sourceFilePath)
    val minLon: Double = List(
      vertexes.map(_.point.getX).min,
      edges.flatMap(_.ls.getCoordinates.map(_.x)).min
    ).min
    val minLat: Double = List(
      vertexes.map(_.point.getY).min,
      edges.flatMap(_.ls.getCoordinates.map(_.y)).min
    ).min
    val maxLon: Double = List(
      vertexes.map(_.point.getX).max,
      edges.flatMap(_.ls.getCoordinates.map(_.x)).max
    ).max
    val maxLat: Double = List(
      vertexes.map(_.point.getY).max,
      edges.flatMap(_.ls.getCoordinates.map(_.y)).max
    ).max
    new RoadGrid(vertexes, edges, minLon, minLat, maxLon, maxLat, gridSize)
  }
}

