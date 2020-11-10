package main.scala.partitioner

import main.scala.geometry.Point
import main.scala.partitioner.voronoiPartitioner
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.Random

object voronoiTest extends App {
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
    val numPartitions = 10
    val pivot = data.take(numPartitions)
    val rdd = sc.parallelize(data)
    val pRDD = voronoiPartitioner(rdd, pivot)
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
  }
}