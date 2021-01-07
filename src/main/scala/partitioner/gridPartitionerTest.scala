//package partitioner
//
//import org.apache.spark.{SparkConf, SparkContext}
//
//import scala.util.Random
//
//object gridPartitionerTest extends App {
//  override def main(args: Array[String]): Unit = {
//    var dataRDD = new Array[Point](0)
//    val r = new Random(5)
//    for (_ <- 0 until 1000) dataRDD = dataRDD :+ Point(r.nextDouble * 100, r.nextDouble * 100)
//
//    /** set up Spark */
//    val conf = new SparkConf()
//    conf.setAppName("Grid-partitioner-Test").setMaster("local")
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("ERROR")
//
//    /** generate RDD and partition it */
//    val dataRDD = sc.parallelize(dataRDD)
//    val (pRDD, gridBound) = gridPartitioner(dataRDD, 10, 0.1)
//    pRDD.mapPartitionsWithIndex {
//      (selection.indexer, partitionIterator) => {
//        val partitionsMap = scala.collection.mutable.Map[Int, List[Point]]()
//        var partitionList = List[Point]()
//        while (partitionIterator.hasNext) {
//          partitionList = partitionIterator.next() :: partitionList
//        }
//        partitionsMap(selection.indexer) = partitionList
//        partitionsMap.iterator
//      }
//    }.collect.foreach(x => {
//      print(x._1 + " ")
//      x._2.foreach(x => print(x + " "))
//      println()
//    })
//    println(gridBound)
//  }
//}