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

  test("RoadGrid: getNearestEdge with middle point distance") {
    rGrid.getNearestEdge(p0, k, "middle", r=6).foreach(println)
  }

  test("RoadGrid: getNearestEdge with projection point distance") {
    rGrid.getNearestEdge(p0, k, "projection", r=6).foreach(println)
  }

  test("RoadGrid: test with trajectory") {
    val trajString = "[[-8.612964, 41.140359000000004], [-8.613378, 41.14035], [-8.614215, 41.140278], [-8.614773, 41.140368], [-8.615907, 41.140449000000004], [-8.616609, 41.140602], [-8.618471999999999, 41.141412], [-8.620623, 41.142789], [-8.622558, 41.144094], [-8.62506, 41.144805], [-8.627436000000001, 41.144733], [-8.630082, 41.145174], [-8.6319, 41.146461], [-8.632584, 41.147316000000004], [-8.631252000000002, 41.148774], [-8.629712999999999, 41.150628000000005], [-8.628803999999999, 41.152077], [-8.628579, 41.152464], [-8.62875, 41.15266200000001], [-8.630424, 41.15277], [-8.632683, 41.152779], [-8.635131, 41.152563], [-8.637705, 41.153013], [-8.640360000000001, 41.15358], [-8.642205, 41.154021], [-8.644068, 41.154507], [-8.646453000000001, 41.154336], [-8.648613000000001, 41.154300000000006], [-8.649504, 41.154336], [-8.649837000000002, 41.154354000000005], [-8.649837000000002, 41.154300000000006], [-8.649882000000002, 41.154282], [-8.649936, 41.154300000000006], [-8.649899999999999, 41.154264], [-8.599383000000001, 41.141735999999995], [-8.59653, 41.140566], [-8.65008, 41.15429099999999], [-8.650395, 41.153814], [-8.650377, 41.153832], [-8.650359, 41.15378700000001], [-8.649891, 41.153166000000006], [-8.649369, 41.152572000000006], [-8.649198000000002, 41.15237400000001], [-8.649711, 41.151213], [-8.649117, 41.150466], [-8.649117, 41.149062], [-8.648613000000001, 41.14826099999999], [-8.648424, 41.148225], [-8.647587, 41.148405000000004], [-8.64594, 41.148413999999995], [-8.643861, 41.148135], [-8.642763, 41.148027], [-8.640918000000001, 41.14836], [-8.637758999999999, 41.148351000000005], [-8.635337999999999, 41.147964], [-8.633277000000001, 41.147172], [-8.631513, 41.146146], [-8.629776000000001, 41.14503], [-8.627814, 41.144642999999995], [-8.625996, 41.144769000000004], [-8.624088, 41.144463], [-8.621325, 41.143401], [-8.619444000000001, 41.141960999999995], [-8.617365, 41.140862999999996], [-8.61597, 41.140530000000005]]"
    val traj = trajString.split(", ").map(_.replace("[", "").replace("]", ""))

    val points = traj.grouped(2).map{case Array(lon, lat) => Point(Array(lon.toDouble, lat.toDouble))}.toArray
    println(s"Total points: ${points.length}")

    val candidates = points.map(rGrid.getNearestEdge(_, k))
    candidates.indices.foreach(i =>
      if (candidates(i).length != k) {

        println(s"$i only has ${candidates(i).length} candidates")
        println(points(i))
        candidates(i).foreach(println)
        println("--------------------")
      })


    val shortestPathPairs = for {
      i <- 0 until candidates.length-1
      src <- candidates(i)
      dst <- candidates(i+1)
    } yield (src._1, dst._1)


    val shortestPathDist = shortestPathPairs.map { x =>
      val g = RoadGraph(rGrid.getGraphEdgesByEdgeId(x._1, x._2))
      val dist = g.getShortestPathAndLength(x._1, x._2)
      println(s"Pair:$x")
      println(s"dist:$dist")
      dist
    }

  }

}
