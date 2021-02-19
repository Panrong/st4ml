package selection

import geometry.Rectangle
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.scalatest.BeforeAndAfter
import org.scalatest.funsuite.AnyFunSuite
import preprocessing.ReadTrajFile
import selection.partitioner.{HashPartitioner, QuadTreePartitioner, STRPartitioner}
import selection.selector.{RTreeSelector, TemporalSelector}

import scala.io.Source
import scala.math.{max, sqrt}

class SelectorSuite extends AnyFunSuite with BeforeAndAfter {

  var spark: SparkSession = _
  var sc: SparkContext = _

  def beforeEach() {
    var config: Map[String, String] = Map()
    val f = Source.fromFile("config")
    f.getLines
      .filterNot(_.startsWith("//"))
      .filterNot(_.startsWith("\n"))
      .foreach(l => {
        val p = l.split(" ")
        config = config + (p(0) -> p(1))
      })
    f.close()
    spark = SparkSession
      .builder()
      .master(config("master"))
      .appName(config("appName"))
      .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
  }

  /**
   * test if partition and indexing gives the same results as full scanning
   */
  test("test hash partitioner + rtree") {
    var config: Map[String, String] = Map()
    val f = Source.fromFile("config")
    f.getLines
      .filterNot(_.startsWith("//"))
      .filterNot(_.startsWith("\n"))
      .foreach(l => {
        val p = l.split(" ")
        config = config + (p(0) -> p(1))
      })
    f.close()
    spark = SparkSession
      .builder()
      .master(config("master"))
      .appName(config("appName"))
      .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val trajectoryFile = "preprocessing/traj_short.csv"
    val numPartitions = 4
    val dataSize = 10000

    val trajRDD = ReadTrajFile(trajectoryFile, num = dataSize, numPartitions)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val sQuery = Rectangle(Array(-8.682329739182336, 41.16930767535641, -8.553892156181982, 41.17336956864337))
    val tQuery = (1372700000L, 1372750000L)
    val rTreeCapacity = max(sqrt(dataSize / numPartitions).toInt, 100)

    /** benchmark */
    val fullSRDD = trajRDD.filter(x => x.intersect(sQuery))
    val fullSTRDD = fullSRDD.filter(x => {
      val (ts, te) = x.timeStamp
      ts <= tQuery._2 && ts >= tQuery._1 || te <= tQuery._2 && te >= tQuery._1
    })

    val hashPartitioner = new HashPartitioner(numPartitions)
    val pRDDHash = hashPartitioner.partition(trajRDD).cache()
    val partitionRangeHash = hashPartitioner.partitionRange
    val selectorHash = RTreeSelector(partitionRangeHash, Some(rTreeCapacity))
    val temporalSelectorH = new TemporalSelector

    val queriedRDDHash = selectorHash.query(pRDDHash)(sQuery).cache()

    val queriedRDDHashST = temporalSelectorH.query(queriedRDDHash)(tQuery)

    assert(queriedRDDHash.count == fullSRDD.count)
    assert(queriedRDDHashST.count == fullSTRDD.count)
  }

  test("test STR partitioner + rtree") {
    var config: Map[String, String] = Map()
    val f = Source.fromFile("config")
    f.getLines
      .filterNot(_.startsWith("//"))
      .filterNot(_.startsWith("\n"))
      .foreach(l => {
        val p = l.split(" ")
        config = config + (p(0) -> p(1))
      })
    f.close()
    spark = SparkSession
      .builder()
      .master(config("master"))
      .appName(config("appName"))
      .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val trajectoryFile = "preprocessing/traj_short.csv"
    val numPartitions = 4
    val dataSize = 10000

    val trajRDD = ReadTrajFile(trajectoryFile, num = dataSize, numPartitions)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val sQuery = Rectangle(Array(-8.682329739182336, 41.16930767535641, -8.553892156181982, 41.17336956864337))
    val tQuery = (1372700000L, 1372750000L)
    val rTreeCapacity = max(sqrt(dataSize / numPartitions).toInt, 100)

    /** benchmark */
    val fullSRDD = trajRDD.filter(x => x.intersect(sQuery))
    val fullSTRDD = fullSRDD.filter(x => {
      val (ts, te) = x.timeStamp
      ts <= tQuery._2 && ts >= tQuery._1 || te <= tQuery._2 && te >= tQuery._1
    })

    val strPartitioner = new STRPartitioner(numPartitions)
    val pRDD = strPartitioner.partition(trajRDD).cache()
    val partitionRange = strPartitioner.partitionRange
    val selector = RTreeSelector(partitionRange, Some(rTreeCapacity))
    val temporalSelector = new TemporalSelector

    val queriedRDD = selector.query(pRDD)(sQuery).cache()

    val queriedRDDST = temporalSelector.query(queriedRDD)(tQuery)

    assert(queriedRDD.count == fullSRDD.count)
    assert(queriedRDDST.count == fullSTRDD.count)

  }

  test("test quadTree partitioner + rtree") {
    var config: Map[String, String] = Map()
    val f = Source.fromFile("config")
    f.getLines
      .filterNot(_.startsWith("//"))
      .filterNot(_.startsWith("\n"))
      .foreach(l => {
        val p = l.split(" ")
        config = config + (p(0) -> p(1))
      })
    f.close()
    spark = SparkSession
      .builder()
      .master(config("master"))
      .appName(config("appName"))
      .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    val trajectoryFile = "preprocessing/traj_short.csv"
    val numPartitions = 4
    val dataSize = 10000

    val trajRDD = ReadTrajFile(trajectoryFile, num = dataSize, numPartitions)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val sQuery = Rectangle(Array(-8.682329739182336, 41.16930767535641, -8.553892156181982, 41.17336956864337))
    val tQuery = (1372700000L, 1372750000L)
    val rTreeCapacity = max(sqrt(dataSize / numPartitions).toInt, 100)

    /** benchmark */
    val fullSRDD = trajRDD.filter(x => x.intersect(sQuery))
    val fullSTRDD = fullSRDD.filter(x => {
      val (ts, te) = x.timeStamp
      ts <= tQuery._2 && ts >= tQuery._1 || te <= tQuery._2 && te >= tQuery._1
    })
    println(fullSRDD.count())
    val strPartitioner = new QuadTreePartitioner(numPartitions)
    val pRDD = strPartitioner.partition(trajRDD).cache()
    val partitionRange = strPartitioner.partitionRange
    val selector = RTreeSelector(partitionRange, Some(rTreeCapacity))
    val temporalSelector = new TemporalSelector

    val queriedRDD = selector.query(pRDD)(sQuery).cache()
    println(queriedRDD.count())
    val queriedRDDST = temporalSelector.query(queriedRDD)(tQuery)

    assert(queriedRDD.count == fullSRDD.count)
    assert(queriedRDDST.count == fullSTRDD.count)

  }

  def afterEach() {
    spark.stop()
  }
}
