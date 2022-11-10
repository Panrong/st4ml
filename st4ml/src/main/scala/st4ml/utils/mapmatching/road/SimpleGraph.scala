package st4ml.utils.mapmatching.road

import st4ml.utils.mapmatching.Graph

/** Simple implementation of graphs using a map. */
case class SimpleGraph[N](succs: Map[N, Map[N, Int]]) extends Graph[N] {
  def apply(n: N): Map[N, Int] = succs.getOrElse(n, Map.empty)

  def nodes: Set[N] = succs.keySet

  override def -(n1: N): SimpleGraph[N] = SimpleGraph(for {
    (n, nbs) <- succs if n1 != n
  } yield n -> (nbs - n1))

  override def -(n1: N, n2: N): SimpleGraph[N] =
    SimpleGraph(succs.updated(n1, succs(n1) - n2))

  /** Returns a new graph with all edges in reversed. */
  def reversed: SimpleGraph[N] = {
    val edges = for {
      (n, nbs) <- succs.toSeq
      succ <- nbs.toSeq
    } yield n -> succ
    val preds = (Map.empty[N, Map[N, Int]] /: edges){ (g, e) =>
      val (n1, (n2, c)) = e
      val nbs = g.getOrElse(n2, Map.empty[N, Int])
      g.updated(n2, nbs + (n1 -> c))
    }
    SimpleGraph(preds)
  }
}