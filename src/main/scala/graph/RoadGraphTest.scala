package main.scala.graph

object RoadGraphTest extends App {
  override def main(args: Array[String]): Unit = {
    val rg = RoadGraph("D:\\spark-projects\\spark-map-matching\\preprocessing\\test.csv")
    //    RoadGraph("/Users/panrong/MapMatching/preprocessing/test.csv")
    rg.getNearestVertex(-8.6152995, 41.1427807, 3).foreach(println)

    // normal query
    // Expected result: Some(List(2214758555, 129557542, 6551282170))
    println(rg.getShortestPath("2214758555", "6551282170"))
    // query where sourceVertex = targetVertex
    // Expected result: Some(List(2214758555))
    println(rg.getShortestPath("2214758555", "2214758555"))
    // query where targetVertex are non-reachable from sourceVertex
    // Expected result: None
    println(rg.getShortestPath("2214758555", "7138039584"))

    // normal query
    // Expected result: (List(2214758555, 129557542, 6551282170),52.855000000000004)
    println(rg.getShortestPathAndLength("2214758555", "6551282170"))
    // query where sourceVertex = targetVertex
    // Expected result: (List(2214758555),0.0)
    println(rg.getShortestPathAndLength("2214758555", "2214758555"))
    // query where targetVertex are non-reachable from sourceVertex
    // Expected result: (List(),1.7976931348623157E308)
    println(rg.getShortestPathAndLength("2214758555", "7138039584"))
  }
}
