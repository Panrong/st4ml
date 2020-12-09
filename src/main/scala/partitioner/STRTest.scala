//package partitioner
//
//import geometry.Point
//import org.apache.spark.sql.SparkSession
//
//import scala.util.Random
//
//object STRTest extends App {
//  override def main(args: Array[String]): Unit = {
//    val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
//    val sc = spark.sparkContext
//    sc.setLogLevel("ERROR")
//
//    var data = new Array[Point](0)
//    val r = new Random(5)
//    for (_ <- 0 until 1000) data = data :+ Point(r.nextDouble * 100, r.nextDouble * 100)
//    val numPartition = 10
//    val (indexedRDD, idBoundaryMap) = STRPartitioner(sc.parallelize(data), numPartition, 0.1)
//
//    //print content of each partition
//    val p = indexedRDD
//    val pointsWithIndex = p.mapPartitionsWithIndex {
//      (index, partitionIterator) => {
//        val partitionsMap = scala.collection.mutable.Map[Int, List[Point]]()
//        var partitionList = List[Point]()
//        while (partitionIterator.hasNext) {
//          partitionList = partitionIterator.next() :: partitionList
//        }
//        partitionsMap(index) = partitionList
//        partitionsMap.iterator
//      }
//    }
//    pointsWithIndex.collect.foreach(x => {
//      print(x._1 + " ")
//      x._2.foreach(x => print(x + " "))
//      println()
//    })
//    println(idBoundaryMap)
//
//    sc.stop()
//
//  }
//}
