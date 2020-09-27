package main.scala.rangequery

import org.apache.spark.Partitioner

import main.scala.RTree.{RTree, Node}
import main.scala.geometry.{Shape, Point, Rectangle}
import System.nanoTime

object rangeQuery {

  val timeCount = true

  def genRTree(entries: Array[Shape], capacity: Int): (RTree, Map[Int, Node]) = {
    val t = nanoTime
    val rootNode = Node(Rectangle(Point(0, 0), Point(0, 0)), isLeaf = true)
    assert(capacity < entries.length, "Capacity(" + capacity.toString + ") should be smaller than total number of trajectories(" + entries.length.toString + ")")
    val rtree = RTree(rootNode, capacity)
    for (entry <- entries) {
      RTree.insertEntry(entry, rtree)
    }
    rtree.root.shrinkMBR()
    if (timeCount) println("... Generating RTree: " + (nanoTime - t) / 1e9d + "s")
    (rtree, rtree.genTable()._1)
  }

  def query(rtree: RTree, rtreeTable: Map[Int, Node], queryRange: Rectangle): Array[Shape] = {
    val entries = rtree.leafEntries
    RTree.queryWithTable(rtreeTable.map { case (key, value) => (key, value.mbr) }, entries, rtree.M, queryRange).distinct
  }

  def refinement(box: Shape, range: Rectangle): Boolean = {
    for (p <- box.attr("points")) {
      val long = p.split(",")(0).toDouble
      val lat = p.split(",")(1).toDouble
      if (Point(long, lat).inside(range)) return true
    }
    false
  }

}


case class keyPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = key.toString.toInt % numParts
}
