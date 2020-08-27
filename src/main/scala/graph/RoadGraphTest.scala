package main.scala.graph

object RoadGraphTest extends App {
  override def main(args: Array[String]): Unit = {
    val rg = RoadGraph("D:\\spark-projects\\MapMatching\\preprocessing\\test.csv")
    //    RoadGraph("/Users/panrong/MapMatching/preprocessing/test.csv")
    rg.getNearestVertex(-8.6152995, 41.1427807, 3).foreach(println)
    println(rg.getShortestPath("2214758555", "6551282170"))
    println(rg.getShortestPathLength("2214758555", "6551282170"))
  }
}
