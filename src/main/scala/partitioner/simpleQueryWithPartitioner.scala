package partitioner

import java.lang.System.nanoTime

import geometry.{Rectangle, Shape}
import mapmatching.preprocessing
import org.apache.spark.{SparkConf, SparkContext}

//import scala.math.{max, min}
//import scala.reflect.ClassTag
//import scala.util.Random


object simpleQueryWithPartitioner extends App {
  override def main(args: Array[String]): Unit = {

    val master = args(0)
    val trajectoryFile = args(1)
    val queryFile = args(2)
    val numPartitions = args(3).toInt
    val samplingRate = args(4).toDouble
    val dataSize = args(5).toInt

    //    val master = "local"
    //    val trajectoryFile = "preprocessing/traj_short.csv"
    //    val queryFile = "datasets/queries.txt"
    //    val numPartitions = 4
    //    val samplingRate = 0.1
    //    val dataSize = 1000

    /** set up Spark */
    val conf = new SparkConf()
    conf.setAppName("Partitioner-Query-Test").setMaster(master)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    //    /** generate mock points */
    //    var data = new Array[Point](0)
    //    val r = new Random(10)
    //    for (_ <- 0 until DataNum) data = data :+
    //      Point(r.nextDouble * 100, r.nextDouble * 100)
    //    val rdd = sc.parallelize(data, numPartitions)

    //    /** generate mock queries */
    //    var queries = new Array[Rectangle](0)
    //    for (_ <- 0 until queryNum) {
    //      val v1 = r.nextDouble * 100
    //      val v2 = r.nextDouble * 100
    //      val v3 = r.nextDouble * 100
    //      val v4 = r.nextDouble * 100
    //      queries = queries :+
    //        Rectangle(Point(min(v1, v2), min(v3, v4)), Point(max(v1, v2), max(v3, v4)))
    //    }
    /** generate trajectory MBR RDD */
    val rdd = preprocessing.genTrajRDD(trajectoryFile, dataSize).map(_.mbr)

    /** generate query RDD */
    val queries = preprocessing.readQueryFile(queryFile)
    val queryRDD = sc.parallelize(queries)

    var t = nanoTime()

    /** normal query */
    val res1 = rdd.cartesian(queryRDD)
      .filter { case (point, query) => point.inside(query) }
      .coalesce(numPartitions)
      .groupByKey()
      .mapValues(_.toArray)
    //    res1.foreach(x=> println(x._1, x._2.length))
    res1.collect
    println(s"Normal range query takes ${((nanoTime() - t) * 10e-9).formatted("%.3f")} seconds")

    /** repartition */
    t = nanoTime()

    val (pRDD, gridBound) = gridPartitioner(rdd, numPartitions, samplingRate)
    val pRDDWithIndex = pRDD.mapPartitionsWithIndex {
      (index, partitionIterator) => {
        val partitionsMap = scala.collection.mutable.Map[Int, List[Shape]]()
        var partitionList = List[Shape]()
        while (partitionIterator.hasNext) {
          partitionList = partitionIterator.next() :: partitionList
        }
        partitionsMap(index) = partitionList
        partitionsMap.iterator
      }
    }
    println(s"Partitioning takes ${((nanoTime() - t) * 10e-9).formatted("%.3f")} seconds")
    pRDD.cache()
    t = nanoTime()

    /** normal query on partitioned rdd */
    val res2 = pRDD.cartesian(queryRDD)
      .filter { case (point, query) => point.inside(query) }
      .coalesce(numPartitions)
      .groupByKey()
      .mapValues(_.toArray)
    //    res1.foreach(x=> println(x._1, x._2.length))
    res2.collect
    println(s"Normal range query on partitioned RDD takes ${((nanoTime() - t) * 10e-9).formatted("%.3f")} seconds")
    t = nanoTime()

    /** query with grid partitioning */
    val res = pRDDWithIndex
      .cartesian(queryRDD.map(query => (query, gridBound.filter { case (_, bound) => bound.intersect(query) }.keys.toArray))
      .flatMapValues(x => x))
      .filter(x => x._2._2 == x._1._1)
      .coalesce(numPartitions)
      .map(x => (x._2._1, x._1._2))
      .map { case (query, points) => (query, points.filter(point => point.inside(query))) }
      .groupByKey()
      .map(x => (x._1, x._2.flatten.toArray))
    res.collect
    res.foreach(x => println(x._1, x._2.length))
    println(s"Range query with grid Partitioning takes ${((nanoTime() - t) * 10e-9).formatted("%.3f")} seconds")


    sc.stop()
  }
}
