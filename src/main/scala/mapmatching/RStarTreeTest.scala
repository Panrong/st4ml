import org.apache.spark.{SparkConf, SparkContext}
import RStarTree._
import main.scala.mapmatching.SpatialClasses._

object RStarTreeTest extends App {
  override def main(args: Array[String]): Unit = {
    /*
    val conf = new SparkConf()
    conf.setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //var fileName = args
    var fileName = "C://Users//kaiqi001//Desktop//taxi_log_2008_by_id//2.txt"
    var numPartition = 9
    val lines = sc.textFile(fileName, numPartition)
    var points = lines.map(line => (line.split(",")(2).toDouble, line.split(",")(3).toDouble)) // Array((Double, Double))
    //points.collect().foreach(point => println(point))

     */
    /*
    var node4 = Node(Rectangle(Point(20, 60), Point(30, 80)),isLeaf = true)
    //node4.addEntry(Array(Point(21,63),Point(26, 76),Point(28,73), Point(22, 72)))
    var node5 = Node(Rectangle(Point(10, 10), Point(25, 50)), isLeaf = true)
    var node1 = Node(Rectangle(Point(0, 0), Point(40, 80)), Array(node4, node5), false)
    */
    /*
    var node1 = Node(Rectangle(Point(0, 0), Point(40, 80)), isLeaf = true)
    var node2 = Node(Rectangle(Point(50, 40), Point(95, 80)), isLeaf = true)
    var node3 = Node(Rectangle(Point(50, 0), Point(90, 30)), isLeaf = true)
    var node0 = Node(Rectangle(Point(0, 0), Point(100, 100)), Array(node1), false)
    var rootNode = Node(Rectangle(Point(0, 0), Point(100, 100)), Array(node0), false)
    var superRootNode = Node(Rectangle(Point(-10, -10), Point(110, 110)), Array(node0), false)

     */

    var rootNode = Node(Rectangle(Point(0, 0), Point(10, 10)), isLeaf = true)
    val capacity = 5
    var rtree = RTree(rootNode, capacity)
    //for(i <- rtree.leaves) println(i.mbr)

    val points = genRandomPoints(100, 100)
    val rectangles = genRandomRectangles(50, 100)
    var i = 0
    /*
    for (point <- points) {
      println(i)
      RTree.insertPoint(point, rtree)
      i += 1
    }

     */

    for (rectangle <- rectangles) {
      RTree.insertEntry(rectangle, rtree)
      i += 1
    }
    /*
    RTree.checkBalance(rootNode, node4, 3)
    var n = RTree.insert(Point(25, 110), rtree, rootNode)
    RTree.checkBalance(rootNode, n, 3)
    n = RTree.insert(Point(30, 20), rtree, rootNode)
    RTree.checkBalance(rootNode, n, 3)
    */
    //RTree.splitSubtree(rootNode)

    //RTree.delete(Array(Point(99, 51), Point(98, 65), Point(55, 57), Point(47, 60)), rtree)
    printTree(rtree.root)

    val res = rtree.genTable()
    val table = res._1
    val childParent = res._2
    for (i <- table.keys.toArray.sorted) println(i, table(i).mbr)
    //for(i <- childParent.keys.toArray.sorted) println(i, childParent(i))


    //val retrieved = RTree.rangeQuery(Rectangle(Point(0, 0), Point(20, 20)), rtree.root)
    val entries = rtree.leafEntries
    val retrieved2 = queryWithTable(table.map {case (key, value) => (key, value.mbr)}, entries, capacity, Rectangle(Point(0, 0), Point(20, 20)))
    //printRetrieved(retrieved)
    printRetrieved(retrieved2)


    //sc.stop()
  }

  def printTree(node: Node): Unit = {
    if (node != null) println(node.mbr.x_min, node.mbr.y_min, node.mbr.x_max, node.mbr.y_max, node.level())
    for (i <- node.child) {
      if (i != null) printTree(i)
    }
  }

  def genRandomPoints(num: Int, range: Int): Array[Point] = {
    val r = new scala.util.Random(5)
    var x: Int = 0
    var y: Int = 0
    var points = new Array[Point](0)
    for (i <- 0 to num - 1) {
      x = r.nextInt(range)
      y = r.nextInt(range)
      points = points :+ Point(x, y)
      println(x, y)
    }
    points
  }

  def genRandomRectangles(num: Int, range: Int): Array[Rectangle] = {
    val r = new scala.util.Random(5)
    var x: Int = 0
    var y: Int = 0
    var d: Int = 0
    var e: Int = 0
    var rectangles = new Array[Rectangle](0)
    for (i <- 0 to num - 1) {
      x = r.nextInt(range)
      y = r.nextInt(range)
      d = r.nextInt((range * 0.1).toInt) + 1
      e = r.nextInt((range * 0.1).toInt) + 1
      rectangles = rectangles :+ Rectangle(Point(x, y), Point(x + d, y + e))
      println(x, y, x + d, y + e)
    }
    rectangles
  }

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
