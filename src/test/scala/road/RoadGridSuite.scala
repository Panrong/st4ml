package road

import org.scalatest.funsuite.AnyFunSuite

import geometry.Point
import road.RoadGrid

class RoadGridSuite extends AnyFunSuite {
  val rGrid = RoadGrid("preprocessing/porto.csv")
  println(rGrid)
  println("--------------------")

  val p0 = Point(Array(-8.6152995, 41.1427807))
  val k = 5

  test("RoadGrid: getNearestVertex") {
    rGrid.getNearestVertex(p0, k).foreach(println)
  }

  test("RoadGrid: getNearestVertex with expanding search region") {
    rGrid.getNearestVertex(p0, k, r=6).foreach(println)
  }

  test("Test getNearestEdge with middle point distance") {
    rGrid.getNearestEdge(p0, k, "middle", r=6).foreach(println)
  }

  test("Test getNearestEdge with projection point distance") {
    rGrid.getNearestEdge(p0, k, "projection", r=6).foreach(println)
  }

}
