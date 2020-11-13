package geometry

import prioritymap.PriorityMap

import scala.annotation.tailrec

/** Implementation of Dijkstra's algorithm using a priority map. */
object DijkstraPriorityMap extends Dijkstra {
  def dijkstra[N](g: Graph[N])(source: N): (Map[N, Int], Map[N, N]) = {
    @tailrec
    def go(active: PriorityMap[N, Int], acc: Map[N, Int], pred: Map[N, N]):
    (Map[N, Int], Map[N, N]) =
      if (active.isEmpty) (acc, pred)
      else {
        val (node, cost) = active.head
        val neighbours = for {
          (n, c) <- g(node) if !acc.contains(n) && cost + c < active.getOrElse(n, Int.MaxValue)
        } yield n -> (cost + c)
        val preds = neighbours mapValues (_ => node)
        go(active.tail ++ neighbours, acc + (node -> cost), pred ++ preds)
      }

    go(PriorityMap(source -> 0), Map.empty, Map.empty)
  }

  override def toString = "DijkstraPriorityMap"
}
