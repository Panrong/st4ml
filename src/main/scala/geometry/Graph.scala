package main.scala.geometry

import scala.annotation.tailrec

/**
 * A trait for weighted graphs.
 */
trait Graph[N] { self =>
  /** Returns a map with the successors of `n` and their costs. */
  def apply(n: N): Map[N, Int]

  /** Returns a new graph with the given node `n` removed. */
  def -(n1: N): Graph[N] = (n: N) => if (n == n1) Map.empty else self(n) - n1

  /** Returns a new graph with the edge from `n1` to `n2` removed. */
  def -(n1: N, n2: N): Graph[N] = (n: N) => if (n == n1) self(n) - n2 else self(n)

  /** Performs a breadth-first search on this graph.
   *
   * Returns the set of nodes reachable from the given source node.
   *
   * Does not terminate if infinitely many nodes are reachable.
   */
  def bfs(source: N): Set[N] = {
    @tailrec
    def go(initial: Set[N], acc: Set[N]): Set[N] =
      if (initial.isEmpty) acc
      else {
        val explored = acc ++ initial
        val neighbours = for {
          node <- initial
          (n, _) <- apply(node) if !explored(n)
        } yield n
        go(neighbours, explored)
      }

    go(Set(source), Set.empty)
  }

  /** Returns whether there is a path from `source` to `target`
   * in this graph.
   */
  def isReachable(source: N, target: N): Boolean =
    bfs(source) contains target
}
