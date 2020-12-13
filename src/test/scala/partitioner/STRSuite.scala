//package partitioner
//
//import geometry.{Point, Rectangle}
//import org.apache.spark.SparkContext
//import org.apache.spark.sql.SparkSession
//import org.scalatest.BeforeAndAfterEach
//import org.scalatest.funsuite.AnyFunSuite
//
//
//// reference:
//// https://medium.com/@phanikumaryadavilli/writing-tests-for-your-spark-code-using-funsuite-71a554f92106
//
//class STRSuite extends AnyFunSuite with BeforeAndAfterEach {
//  private val master = "local"
//
//  private val appName = "test"
//
//  var spark : SparkSession = _
//
//  var sc: SparkContext = _
//
//  override def beforeEach(): Unit = {
//    spark = new SparkSession.Builder().appName(appName).master(master).getOrCreate()
//  }
//
//  val entries = new Array[Point](1000)
//  var cnt = 0
//  for (i <- -10 to 10) {
//    for (j <- -10 to 10) {
//      if (Math.abs(i) + Math.abs(j) <= 10) {
//        entries(cnt) = Point(Array(i, j))
//        cnt = cnt + 1
//      }
//    }
//  }
//  test("STR: partitioner, simple") {
//    val sc = spark.sparkContext
//    val rdd = sc.parallelize(entries)
//    val (pRDD, idBoundMap) = STRPartitioner(rdd, 9, 0.5)
//    assert(pRDD.getNumPartitions == 9)
//  }
//  test("STR: coverage, simple") {
//    val sc = spark.sparkContext
//    val rdd = sc.parallelize(entries)
//    val (pRDD, idBoundMap) = STRPartitioner(rdd, 9, 0.5)
//    val wholeRange = Rectangle(Array(
//      rdd.map(_.coordinates(0)).min,
//      rdd.map(_.coordinates(1)).min,
//      rdd.map(_.coordinates(0)).min,
//      rdd.map(_.coordinates(1)).min))
//
//    for (r <- idBoundMap.values) assert(r.inside(wholeRange))
//
//    for (i <- idBoundMap.values) {
//      for (j <- idBoundMap.values) {
//        if (i.coordinates.toList != j.coordinates.toList) {
//          assert(i.overlappingArea(j) == 0)
//        }
//      }
//    }
//    var totalArea: Double = 0
//    for (r <- idBoundMap.values) totalArea += r.area
//    assert(totalArea == wholeRange)
//  }
//
//  override def afterEach(): Unit = {
//    spark.stop()
//  }
//}
