import MapMatcher._
import org.apache.spark.{SparkConf, SparkContext}
import RStarTree.{Node, RTree, queryWithTable}
import preprocessing._
import SpatialClasses._
object RunMapMatching extends App{
  override def main(args: Array[String]): Unit = {
    val filename = args(0) //read file name from argument input
      //val filename = "/datasets/porto.csv"
      //set up spark environment
      val conf = new SparkConf()
      conf.setAppName("MapMatching_v1").setMaster("spark://Master:7077")
      val sc = new SparkContext(conf)
      sc.setLogLevel("ERROR")

      val trajRDD = preprocessing(filename)
      //val mapMatchedRDD = trajRDD.map(traj => MapMatcher(traj, roadIDMap, roadGraph)

      val rectangles = trajRDD.map(x => x.mbr.assignID(x.tripID)).collect

      //generate rtree
      var rootNode = Node(Rectangle(Point(0, 0), Point(10, 10)), isLeaf = true)
      val capacity = 20000
      var rtree = RTree(rootNode, capacity)
      var i = 0
      for (rectangle <- rectangles) {
        RTree.insertEntry(rectangle, rtree)
          i += 1
      }
    // range query
    val res = rtree.genTable()
      val table = res._1
      val entries = rtree.leafEntries
      val queryRange = Rectangle(Point(-8.625, 41.145), Point(-8.615, 41.155))
      val retrieved = queryWithTable(table.map {case (key, value) => (key, value.mbr)}, entries, capacity, queryRange)
      //printRetrieved(retrieved)
      for(r <- retrieved) println(r.id)
        println(retrieved.length + " trajectories retrieved in the range "+ queryRange.x_min, queryRange.y_min, queryRange.x_max, queryRange.y_max)
          sc.stop()

          def printRetrieved(retrieved: Array[Shape]) {
            println("-------------")
              for (i <- retrieved) {
                if (i.isInstanceOf[Point]) {
                  val p = i.asInstanceOf[Point]
                    println(p.x, p.y)
                }
                else {
                  val r = i.asInstanceOf[Rectangle]
                    println(r.x_min, r.y_min, r.x_max, r.y_max)
                }
              }
            println("-------------")
          }
  }
}
