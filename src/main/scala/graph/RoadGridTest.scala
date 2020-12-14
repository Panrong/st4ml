package graph

import System.nanoTime
import geometry.Point

import scala.reflect.ClassTag

object RoadGridTest extends App {
//  val rGrid = RoadGrid("D:\\spark-projects\\spark-map-matching\\preprocessing\\test.csv")
  val rGrid = RoadGrid("datasets/porto.csv")
  println(rGrid)
  println("--------------------")

  val p0 = Point(Array(-8.6152995, 41.1427807))
  val k =5
  println("Test getNearestVertex")
  rGrid.getNearestVertex(p0, k).foreach(println)
  println("--------------------")
  println("Test getNearestVertex with expanding search region")
  rGrid.getNearestVertex(p0, k, r=6).foreach(println)
  println("--------------------")
  println("Test getNearestEdge with middle point distance")
  rGrid.getNearestEdge(p0, k, "middle", r=6).foreach(println)
  println("--------------------")
  println("Test getNearestEdge with projection point distance")
  rGrid.getNearestEdge(p0, k, "projection", r=6).foreach(println)
  println("--------------------")

  val p1 = Point(Array(-8.614661, 41.143212))
  val edges = rGrid.getGraphEdgesByPoint(p0, p1) // using default r=50, about 500 meter
  val rGraph = RoadGraph(edges)

  println("Test getShortestPath: normal query")
  println("Expected result: Some(List(2214758555, 129557542, 6551282170))")
  println(rGraph.getShortestPath("2214758555", "6551282170"))
  println("Test getShortestPath: query where sourceVertex = tarGraphetVertex")
  println("Expected result: Some(List(2214758555))")
  println(rGraph.getShortestPath("2214758555", "2214758555"))
  println("--------------------")
  println("Test getShortestPath: query where tarGraphetVertex are non-reachable from sourceVertex")
  println("Expected result: None")
  println(rGraph.getShortestPath("2214758555", "7138039584"))
  println("--------------------")

  println("Test getShortestPathAndLength: normal query")
  println("Expected result: (List(2214758555, 129557542, 6551282170),52.855000000000004)")
  println(rGraph.getShortestPathAndLength("2214758555", "6551282170"))
  println("Test getShortestPathAndLength: query where sourceVertex = tarGraphetVertex")
  println("Expected result: (List(2214758555),0.0)")
  println(rGraph.getShortestPathAndLength("2214758555", "2214758555"))
  println("--------------------")
  println("Test getShortestPathAndLength: query where tarGraphetVertex are non-reachable from sourceVertex")
  println("Expected result: (List(),1.7976931348623157E308)")
  println(rGraph.getShortestPathAndLength("2214758555", "7138039584"))
  println("--------------------")

  println("Test getShortestPathAndLength: performance")
  val rGraphAllEdges = RoadGraph(rGrid.edges)
  var t = nanoTime
  val selectEdgeResult = rGraph.getShortestPathAndLength("2214758555", "6551282170")
  val selectEdgeTime = (nanoTime - t) / 1e9d
  t = nanoTime
  val allEdgeResult = rGraphAllEdges.getShortestPathAndLength("2214758555", "6551282170")
  val allEdgeTime = (nanoTime - t) / 1e9d
  val speedup = allEdgeTime / selectEdgeTime
  println(s"--Running getShortestPathAndLength on selected edges took: $selectEdgeTime s")
  println(s"--Running getShortestPathAndLength on all edges took: $allEdgeTime s")
  println(s"--selected edges result: $selectEdgeResult")
  println(s"--all edges result: $allEdgeResult")
  println(s"Speedup $speedup times for one pair of OD")
  println("--------------------")



}
