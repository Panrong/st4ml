package geometry

import road.SimpleGraph
import org.scalacheck.Gen

object DijkstraPriorityMapTest extends App {
  def randomGraph(nodes: Int, outDegree: Int, maxCost: Int): SimpleGraph[Int] = {
    val genEdge = Gen.zip(Gen.choose(0, nodes - 1), Gen.choose(0, maxCost))
    val genNeighbours = Gen.mapOfN(outDegree, genEdge)
    val genGraph = Gen.sequence[Map[Int, Map[Int, Int]], (Int, Map[Int, Int])] {
      (0 until nodes) map (i => Gen.zip(i, genNeighbours))
    }
    SimpleGraph(genGraph.sample.get)
  }

  val nodes = 5
  val outDegree = 2

  val g = randomGraph(nodes, outDegree, 10)
  println(g)

  val dijkstra = DijkstraPriorityMap

  println(s"Running $dijkstra on random graph with $nodes nodes...")
  dijkstra.dijkstra(g)(0)
  println(dijkstra.dijkstra(g)(0))
  println(dijkstra.shortestPath(g)(0, 0))
  println(dijkstra.shortestPath(g)(0, 1))
  println(dijkstra.shortestPath(g)(0, 2))
  println(dijkstra.shortestPath(g)(0, 3))
  println(dijkstra.shortestPath(g)(0, 4))

}
