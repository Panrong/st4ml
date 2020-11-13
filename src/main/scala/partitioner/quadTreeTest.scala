package partitioner

import geometry.{Point, Shape, Rectangle}
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassTag
import scala.util.Random


object quadTreeTest extends App {
  override def main(args: Array[String]): Unit = {
    var data = new Array[Point](0)
    val r = new Random(10)
    for (i <- 0 until 1000) data = data :+ Point(r.nextDouble * 100, r.nextDouble * 100)

    /** set up Spark */
    val conf = new SparkConf()
    conf.setAppName("QuadTree-Partitioner-Test").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    /** generate RDD and partition it */
    val rdd = sc.parallelize(data)
    val (pRDD, quadTree, nodeIdPartitionMap) = quadTreePartitioner(rdd, 11, 0.5)
    pRDD.mapPartitionsWithIndex {
      (index, partitionIterator) => {
        val partitionsMap = scala.collection.mutable.Map[Int, List[Point]]()
        var partitionList = List[Point]()
        while (partitionIterator.hasNext) {
          partitionList = partitionIterator.next() :: partitionList
        }
        partitionsMap(index) = partitionList
        partitionsMap.iterator
      }
    }.collect.foreach(x => {
      print(x._1 + " ")
      x._2.foreach(x => print(x + " "))
      println()
    })
    println(nodeIdPartitionMap)


    /** single quadtree test */
    val queryRes = quadTree.query(Rectangle(Point(10, 10), Point(60, 60)))
      .map(x => nodeIdPartitionMap(x)).filter(_ != -1)
  }

  def printTree[T <: Shape : ClassTag](tree: QuadTree[T],
                                       nodeList: scala.collection.mutable.LinkedHashMap[Int, Node[T]]): Unit = {
    println("root: ")
    println(s"${tree.root.r}, ${tree.root.capacity}, ${tree.root.isLeaf}")
    var level = 1
    var levelNodes = Array(tree.root)
    while (levelNodes.map(x => x.isLeaf).contains(false)) {
      println(s"level $level: ")
      var childNodes = new Array[Node[T]](0)
      for (parentNode <- levelNodes if !parentNode.isLeaf) {
        childNodes = childNodes :+ nodeList(parentNode.childNW) :+
          nodeList(parentNode.childNE) :+
          nodeList(parentNode.childSW) :+
          nodeList(parentNode.childSE)
        childNodes.foreach(node => println(s"${node.id}, ${node.r}, ${node.capacity}, ${node.isLeaf}")) // have duplication problem
      }
      levelNodes = childNodes
      level += 1
    }
  }
}

