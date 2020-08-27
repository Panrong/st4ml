package dijkstra

import scala.annotation.tailrec
import scala.collection.mutable

/**
  * Algorithm is based on Java version implemented by Princeton University https://algs4.cs.princeton.edu/44sp/
  */

final case class DirectedEdge(from: Int, to: Int, weight: Double)

final case class EdgeWeightedDigraph(adj: Map[Int, List[DirectedEdge]] = Map.empty)

object EdgeWeightedDigraphOps {

  implicit class EdgeWeightedDigraphOps(g: EdgeWeightedDigraph) {
    /**
      * Adds directed edge 'e' to EdgeWeightedDigraph by creating a full-copy adjacency list, i.e. this operation
      * leaves current graph 'g' immutable
      *
      * @param e - DirectedEdge to add into the graph 'g'
      * @return returns new graph with edge 'e' added to it
      */
    def addEdge(e: DirectedEdge): EdgeWeightedDigraph = {
      val list = g.adj.getOrElse(e.from, List.empty)
      val adj = g.adj + (e.from -> (list :+ e))
      EdgeWeightedDigraph(adj)
    }
  }

}

object ShortestPath {
  /**
    * Function tries to find a shortest path from source vertex to all other vertices in the graph
    *
    * @param g       EdgeWeightedDigraph to find a shortest path
    * @param sourceV source vertex to find a shortest path from
    * @return return either error as string when input parameters are invalid or return shortest path result
    */
  def run(g: EdgeWeightedDigraph, sourceV: Int): Either[String, ShortestPathCalc] = {
    val size = g.adj.size

    if (sourceV >= size) Left(s"Source vertex must in range [0, $size)")
    else {
      val edgeTo = mutable.ArrayBuffer.fill[Option[DirectedEdge]](size)(None)
      val distTo = mutable.ArrayBuffer.fill(size)(Double.PositiveInfinity)

      //init source distance and add to the queue
      distTo(sourceV) = 0.0
      val sourceDist = (sourceV, distTo(sourceV))
      val sortByWeight: Ordering[(Int, Double)] = (a, b) => a._2.compareTo(b._2)
      val queue = mutable.PriorityQueue[(Int, Double)](sourceDist)(sortByWeight)

      while (queue.nonEmpty) {
        val (minDestV, _) = queue.dequeue()
        val edges = g.adj.getOrElse(minDestV, List.empty)

        edges.foreach { e =>
          if (distTo(e.to) > distTo(e.from) + e.weight) {
            distTo(e.to) = distTo(e.from) + e.weight
            edgeTo(e.to) = Some(e)
            if (!queue.exists(_._1 == e.to)) queue.enqueue((e.to, distTo(e.to)))
          }
        }
      }

      Right(new ShortestPathCalc(edgeTo, distTo))
    }
  }
}

/**
  *
  * @param edgeTo a sequence which represents the last edge on the shortest path from 'sourceV' to vertex i.
  *               None means there is no path to vertex i
  * @param distTo a sequence of distances from source vertex to a specific i vertex
  */
class ShortestPathCalc(edgeTo: Seq[Option[DirectedEdge]], distTo: Seq[Double]) {
  /**
    *
    * @param v vertex to get the path for
    * @return returns error when v is invalid or sequence of edges which form the path from source vertex to v vertex
    */
  def pathTo(v: Int): Either[String, Seq[DirectedEdge]] = {

    @tailrec
    def go(list: List[DirectedEdge], vv: Int): List[DirectedEdge] =
      edgeTo(vv) match {
        case Some(e) => go(e +: list, e.from)
        case None => list
      }

    hasPath(v).map(b => if (!b) Seq() else go(List(), v))
  }

  /**
    *
    * @param v vertex to check whether path from source vertex to v vertex exists
    * @return returns error when v is invalid or Boolean when some path from source vertex to vertex v exists
    */
  def hasPath(v: Int): Either[String, Boolean] =
    distTo.lift(v).map(_ < Double.PositiveInfinity).toRight(s"Vertex $v does not exist")

  /**
    *
    * @param v vertex to get the distance for
    * @return returns error when v is invalid or the Double distance which is a sum of weights
    */
  def distToV(v: Int): Either[String, Double] =
    distTo.lift(v).toRight(s"Vertex $v does not exist")
}
