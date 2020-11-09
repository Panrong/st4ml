package partitioner

import main.scala.geometry.Point
import main.scala.partitioner.gridPartitioner
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object gridPartitionerTest extends App {
  override def main(args: Array[String]): Unit = {
    var data = new Array[Point](0)
    val r = new Random()
    for (_ <- 0 until 1000) data = data :+ Point(r.nextDouble * 100, r.nextDouble * 100)

    /** set up Spark */
    val conf = new SparkConf()
    conf.setAppName("Grid-Partitioner-Test").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    /** generate RDD and partition it */
    val rdd = sc.parallelize(data)
    val (pRDD, gridBound) = gridPartitioner(rdd, 10, 0.2)
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
    }.collect.foreach(x => println(x))
    println(gridBound)
  }
}