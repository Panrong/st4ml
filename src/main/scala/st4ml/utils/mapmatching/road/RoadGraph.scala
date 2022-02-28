package st4ml.utils.mapmatching.road

import st4ml.utils.mapmatching.{DijkstraPriorityMap, Graph}

class RoadGraph(edges: Array[RoadEdge]) extends Serializable {
  // fields
  val g: RouteGraph[String] = buildGraph()
  val id2edge: Map[String, RoadEdge] = edges.map(x => x.id -> x).toMap

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

  def getShortestPath(sourceVertexId: String, targetVertexId: String): List[String] = {
    val router = DijkstraPriorityMap
    val res = router.shortestPath(g)(sourceVertexId, targetVertexId)
    res match {
      case Some(l) => l
      case None => List.empty
    }
  }

  def getShortestPathAndLength(sourceId: String, targetId: String): (List[String], Double) = {
    var src = sourceId
    var dst = targetId

    if (sourceId.contains('-')) {
      // input Ids are edge id
      src = sourceId.split("-")(0)
      dst = targetId.split("-")(1)
    }

    val path = getShortestPath(src, dst)
    val length = path match {
      case Nil => Double.MaxValue
      case l if l.size == 1 => 0
      case l if l.size > 1 => l.sliding(2).map(x => id2edge(s"${x(0)}-${x(1)}").length).sum
    }
    (path, length)
  }

  def hasEdge(from: String, to:String): Boolean = {
    id2edge.get(s"$from-$to") match {
      case None => false
      case _ => true
    }
  }

}

object RoadGraph {
  def apply(edges: Array[RoadEdge]): RoadGraph = new RoadGraph(edges)
}

