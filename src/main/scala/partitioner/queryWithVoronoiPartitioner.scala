//package partitioner
//
//import preprocessing.{preprocessingOld, ReadQueryFile}
//import geometry.Shape
//import org.apache.spark.{SparkConf, SparkContext}
//
//import java.lang.System.nanoTime
//
//
//object queryWithVoronoiPartitioner extends App {
//  override def main(args: Array[String]): Unit = {
//
//    val master = args(0)
//    val trajectoryFile = args(1)
//    val queryFile = args(2)
//    val numPartitions = args(3).toInt
//    val samplingRate = args(4).toDouble
//    val dataSize = args(5).toInt
//
//    /** set up Spark */
//    val conf = new SparkConf()
//    conf.setAppName("partitioner-Query2d-Test").setMaster(master)
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("ERROR")
//
//    /** generate trajectory MBR RDD */
//    val dataRDD = preprocessingOld.genTrajRDD(trajectoryFile, dataSize).map(_.mbr)
//
//    /** generate query RDD */
//    val queryRDD = ReadQueryFile(queryFile).dataRDD.map(x => x.query)
//
//    var t = nanoTime()
//    /** normal query */
//    val res1 = dataRDD.cartesian(queryRDD)
//      .filter { case (point, query) => point.inside(query) }
//      .coalesce(numPartitions)
//      .groupByKey()
//      .mapValues(_.toArray)
//    //    res1.foreach(x=> println(x._1, x._2.length))
//    res1.collect
//    println(s"Normal range query takes ${((nanoTime() - t) * 10e-9).formatted("%.3f")} seconds")
//
//    /** repartition */
//    t = nanoTime()
//
//    val (pRDD, pivotMap, pivotMaxDistMap) = voronoiPartitioner(dataRDD, numPartitions, samplingRate)
//
//    val pRDDWithIndex = pRDD.mapPartitionsWithIndex {
//      (selection.indexer, partitionIterator) => {
//        val partitionsMap = scala.collection.mutable.Map[Int, List[Shape]]()
//        var partitionList = List[Shape]()
//        while (partitionIterator.hasNext) {
//          partitionList = partitionIterator.next() :: partitionList
//        }
//        partitionsMap(selection.indexer) = partitionList
//        partitionsMap.iterator
//      }
//    }
//    println(s"Partitioning takes ${((nanoTime() - t) * 10e-9).formatted("%.3f")} seconds")
//    pRDD.cache()
//    t = nanoTime()
//
//    /** normal query on partitioned dataRDD */
//    val res2 = queryRDD.cartesian(pRDD)
//      .filter { case (query, point) => point.inside(query) }
//      .coalesce(numPartitions)
//      .groupByKey()
//      .mapValues(_.toArray)
//    //    res1.foreach(x=> println(x._1, x._2.length))
//    res2.collect
//    println(s"Normal range query on partitioned RDD takes ${((nanoTime() - t) * 10e-9).formatted("%.3f")} seconds")
//    t = nanoTime()
//
//    /** query with voronoi partitioning */
//    val relevantPartitions = queryRDD.map(query => (query, query.center(), query.diagonals(0).length / 2))
//      .map {
//        case (query, center, diagonal) => (query,
//          pivotMaxDistMap.keys.toArray
//            .filter(point => center.geoDistance(point) - diagonal < pivotMaxDistMap(point)).map(point => pivotMap(point)))
//      }
//      .flatMapValues(x => x) // (query, Int)
//
//    val res = relevantPartitions.cartesian(pRDDWithIndex)
//      .filter(x => x._1._2 == x._2._1)
//      .coalesce(numPartitions)
//      .map(x => (x._1._1, x._2._2.filter(y => y.inside(x._1._1.query))))
//      .groupByKey
//      .map(x => (x._1, x._2.toArray.flatten))
//
//    res.foreach(x => println(x._1, x._2.length))
//    println(s"Range query with voronoi Partitioning takes ${((nanoTime() - t) * 10e-9).formatted("%.3f")} seconds")
//
//    sc.stop()
//  }
//}