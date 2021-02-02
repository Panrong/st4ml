package geometry.road

import java.lang.System.nanoTime

import org.scalatest.funsuite.AnyFunSuite
import geometry.Point
import geometry.road.{RoadGraph, RoadGrid}

class RoadGraphSuite extends AnyFunSuite {
  val rGrid = RoadGrid("preprocessing/porto.csv")
  println(rGrid)
  println("--------------------")

  val p0 = Point(Array(-8.6152995, 41.1427807))
  val p1 = Point(Array(-8.614661, 41.143212))

  val edges = rGrid.getGraphEdgesByPoint(p0, p1) // using default r=50, about 500 meter
  val rGraph = RoadGraph(edges)

  test("RoadGraph: getShortestPath - normal query") {
    val res = rGraph.getShortestPath("6551282170", "2214758555")
    val ans = List("6551282170", "129557542", "2214758555")
    assert(res == ans)
  }

  test("RoadGraph: getShortestPath - query where sourceVertex == tarGraphetVertex") {
    val res = rGraph.getShortestPath("2214758555", "2214758555")
    val ans = List("2214758555")
    assert(res == ans)
  }

  test("RoadGraph: getShortestPath - query  where targetVertex are non-reachable from sourceVertex") {
    val res = rGraph.getShortestPath("2214758555", "6426764335")
    val ans = List()
    assert(res == ans)
  }

  test("RoadGraph: getShortestPathAndLength - normal query") {
    val res = rGraph.getShortestPathAndLength("6551282170", "2214758555")
    val ans = (List("6551282170", "129557542", "2214758555"), 52.855000000000004)
    assert(res == ans)
  }

  test("RoadGraph: getShortestPathAndLength - query where sourceVertex == tarGraphetVertex") {
    val res = rGraph.getShortestPathAndLength("2214758555", "2214758555")
    val ans = (List("2214758555"), 0.0)
    assert(res == ans)
  }

  test("RoadGraph: getShortestPathAndLength - query where targetVertex are non-reachable from sourceVertex") {
    val res = rGraph.getShortestPathAndLength("2214758555", "6426764335")
    val ans = (List(), Double.MaxValue)
    assert(res == ans)
  }

  test("RoadGraph: getShortestPathAndLength - performance") {
    val rGraphAllEdges = RoadGraph(rGrid.edges)
    var t = nanoTime
    val selectEdgeResult = rGraph.getShortestPathAndLength("6551282170", "2214758555")
    val selectEdgeTime = (nanoTime - t) / 1e9d
    t = nanoTime
    val allEdgeResult = rGraphAllEdges.getShortestPathAndLength("6551282170", "2214758555")
    val allEdgeTime = (nanoTime - t) / 1e9d
    val speedup = allEdgeTime / selectEdgeTime
    println(s"--Running getShortestPathAndLength on selected edges took: $selectEdgeTime s")
    println(s"--Running getShortestPathAndLength on all edges took: $allEdgeTime s")
    println(s"--selected edges result: $selectEdgeResult")
    println(s"--all edges result: $allEdgeResult")
    println(s"Speedup $speedup times for one pair of OD")
  }





}
