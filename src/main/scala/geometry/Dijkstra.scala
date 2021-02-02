package geometry

import geometry.road.Util.iterateRight

import scala.language.implicitConversions

/** A trait for Dijkstra's algorithm. */
trait Dijkstra {
  /** Runs Dijkstra's algorithm on the given graph from the given source node.
   *
   * Returns a map with distances from the source and another map that maps
   * each node reachable from the source to a predecessor that lies on the
   * shortest path from the source.
   *
   * If a node is unreachable from the source, neither of the two returned maps
   * contains that node as a key.
   */
  def dijkstra[N](g: Graph[N])(source: N): (Map[N, Int], Map[N, N])

  /** Optionally computes a shortest path between two given nodes in a graph.
   *
   * Returns a `Some` with a shortest path from `source` to `target` if `target`
   * is reachable from `source`; returns `None` otherwise.
   */
  def shortestPath[N](g: Graph[N])(source: N, target: N): Option[List[N]] = {
    val pred = dijkstra(g)(source)._2
    if (pred.contains(target) || source == target) Some(iterateRight(target)(pred.get))
    else None
  }
}

/** Companion object for the Dijkstra trait.
 *
 * Defines an implicit conversion which can be used to call `dijkstra` and
 * `shortestPath` as methods on `Graph` instances.
 *
 * To enable the implicit conversion, you can either mix in the trait
 * `Dijkstra.Syntax` or import `Dijkstra.syntax._`.
 */
object Dijkstra {
  case class DijkstraOps[N](graph: Graph[N], instance: Dijkstra) {
    def dijkstra(source: N): (Map[N, Int], Map[N, N]) =
      instance.dijkstra(graph)(source)

    def shortestPath(source: N, target: N): Option[List[N]] =
      instance.shortestPath(graph)(source, target)
  }

  trait Syntax {
    implicit def toDijkstraOps[N](graph: Graph[N])(implicit d: Dijkstra): DijkstraOps[N] =
      DijkstraOps(graph, d)
  }

  object syntax extends Syntax
}