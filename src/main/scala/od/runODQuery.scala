import main.scala.graph.RoadGrid
import main.scala.mapmatching.preprocessing
import org.apache.spark.{SparkConf, SparkContext}
import main.scala.od.odQuery.{genODRDD, strictQuery, thresholdQuery}

object runODQuery extends App {
  override def main(args: Array[String]): Unit = {
    /** input arguments */
    val master = args(0)
    val roadGraphFile = args(1)
    val numPartitions = args(2).toInt
    val queryTestNum = args(3)
    val trajectoryFile = args(4) //map-matched

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
    val vertices = rg.vertexes.map(x => x.id).take(100)
    val vertexRDD = sc.parallelize(vertices, numPartitions)
    val pairVertexRDD = vertexRDD.cartesian(vertexRDD).map(x => s"${x._1}->${x._2}")
    println(pairVertexRDD.take(5).deep)
    strictQuery(pairVertexRDD, groupedODRDD).foreach(x => println(x._1, x._2.length))
  }

}