package main.scala.graph

import main.scala.geometry.{DijkstraPriorityMap, Graph}

class RoadGraph(edges: Array[RoadEdge]) extends Serializable {
  // fields
  val g: RouteGraph[String] = buildGraph()
  private val id2edge: Map[String, RoadEdge] = edges.map(x => x.id -> x).toMap

  /**
    RouteGraph for calculating dijkstra and shortest path
   */
  final case class RouteGraph[N](succs: Map[N, Map[N, Int]]) extends Graph[N] {
    def apply(n: N): Map[N, Int] = succs.getOrElse(n, Map.empty)

    def nodes: Set[N] = succs.keySet
  }

  def buildGraph() : RouteGraph[String] = {
    val gInfo = edges.map(x => x.from -> (x.to, x.length.toInt)).groupBy(_._1).map{ case (k,v) =>
      k -> v.map(x => x._2._1 -> x._2._2).toMap
    }
    RouteGraph(gInfo)
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
  def apply(edges: Array[RoadEdge]): RoadGraph = new RoadGraph(edges)
}

