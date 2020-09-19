package main.scala.graph

object RoadGraphTest extends App {
  override def main(args: Array[String]): Unit = {
//    val rg = RoadGraph("D:\\spark-projects\\spark-map-matching\\preprocessing\\test.csv")
    val rg = RoadGraph("C:\\Users\\kaiqi001\\Documents\\GitHub\\spark-map-matching\\preprocessing\\porto.csv")

    println("Test getNearestVertex")
    rg.getNearestVertex(-8.6152995, 41.1427807, 3).foreach(println)
    println("--------------------")
    println("Test getNearestEdge")
    rg.getNearestEdge( -8.6152995, 41.1427807, 3).foreach(println)
    println("--------------------")

    println("Test getShortestPath: normal query")
    println("Expected result: Some(List(2214758555, 129557542, 6551282170))")
    println(rg.getShortestPath("2214758555", "6551282170"))
    println("Test getShortestPath: query where sourceVertex = targetVertex")
    println("Expected result: Some(List(2214758555))")
    println(rg.getShortestPathAndLength("2214758555", "2214758555"))
    println("--------------------")
    println("Test getShortestPath: query where targetVertex are non-reachable from sourceVertex")
    println("Expected result: None")
    println(rg.getShortestPath("2214758555", "7138039584"))
    println("--------------------")

    println("Test getShortestPathAndLength: normal query")
    println("Expected result: (List(2214758555, 129557542, 6551282170),52.855000000000004)")
    println(rg.getShortestPathAndLength("2214758555", "6551282170"))
    println("Test getShortestPathAndLength: query where sourceVertex = targetVertex")
    println("Expected result: (List(2214758555),0.0)")
    println(rg.getShortestPathAndLength("2214758555", "2214758555"))
    println("--------------------")
    println("Test getShortestPathAndLength: query where targetVertex are non-reachable from sourceVertex")
    println("Expected result: (List(),1.7976931348623157E308)")
    println(rg.getShortestPathAndLength("2214758555", "7138039584"))
    println("--------------------")

  }
}
