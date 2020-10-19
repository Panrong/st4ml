import main.scala.graph.RoadGrid
import main.scala.mapmatching.preprocessing
import org.apache.spark.{SparkConf, SparkContext}
import main.scala.od.odQuery.{genODRDD, strictQuery, thresholdQuery}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}

object runODQuery extends App {
  override def main(args: Array[String]): Unit = {
    /** input arguments */
    val master = args(0)
    val roadGraphFile = args(1)
    val numPartitions = args(2).toInt
    val queries = args(3)
    val trajectoryFile = args(4) //map-matched
    val resDir = args(5)

    /** set up Spark */
    val conf = new SparkConf()
    conf.setAppName("RangeQuery_v2").setMaster(master)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    /** generate odRDD */
    val mmTrajectoryRDD = preprocessing.readMMTrajFile(trajectoryFile)
    val groupedODRDD = genODRDD(mmTrajectoryRDD)
    val rg = RoadGrid(roadGraphFile)

    /** simple testing */
    //    println(groupedODRDD.count)
    //    println(groupedODRDD.take(3).deep)
    //    println("----single query test:")
    //    println(strictQuery("3446699979", "25632278", groupedODRDD).deep)
    //    println(thresholdQuery("3446699979", "25632278", groupedODRDD, 200, rg).deep)
    //    println("----multi queries test:")
    //    val queryRDD = sc.parallelize(Array("297369744->475341668", "3446699979->25632278", "128674452->5264784641"))
    //    println(strictQuery(queryRDD, groupedODRDD).deep)
    //    println(thresholdQuery(queryRDD, groupedODRDD, 200, rg).deep)
    /** test on all vertex pairs */
    var pairVertexRDD = sc.emptyRDD[String]
    if (queries == "all") {
      val vertices = rg.vertexes.map(x => x.id).take(100)
      val vertexRDD = sc.parallelize(vertices, numPartitions)
      pairVertexRDD = vertexRDD.cartesian(vertexRDD).map(x => s"${x._1}->${x._2}")
    }
    else pairVertexRDD = sc.parallelize(preprocessing.readODQueryFile(queries))
    //println(pairVertexRDD.take(5).deep)
    val resRDD = strictQuery(pairVertexRDD, groupedODRDD)
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    val df = resRDD.map(x => Row(x._1, x._2.deep.toString.drop(6).dropRight(1)))
      .map({
        case Row(val1: String, val2: String) => (val1, val2)
      }).toDF("queryOD", "trajs")
    df.write.option("header", value = true).option("encoding", "UTF-8").csv(resDir)
  }
}