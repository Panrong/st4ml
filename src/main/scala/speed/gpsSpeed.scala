//import graph.RoadGrid
//import mapmatching.preprocessing
//import org.apache.spark.{SparkConf, SparkContext}
//
//object gpsSpeed extends App {
//
//  override def main(args: Array[String]): Unit = {
//    val filename = args(0)
//
//    //set up spark environment
//    val conf = new SparkConf()
//    conf.setAppName("MapMatching_v1").setMaster("local")
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("ERROR")
//    val rGrid = RoadGrid(args(1))
//    val dataRDD = preprocessing(filename, List(rGrid.minLat, rGrid.minLon, rGrid.maxLat, rGrid.maxLon), true, 100).zipWithIndex()
//    val speedRDD = dataRDD.map(x => {
//      val gpsPoints = x._1.points
//      val tripID = x._1.tripID
//      val speeds = gpsPoints.sliding(2).toArray.map {
//        x => {
//          if (x.length > 1) {
//            val p1 = x(0)
//            val p2 = x(1)
//            p1.geoDistance(p2) / 15 * 3.6
//          }
//          else 0
//        }
//      }
//      (tripID, speeds)
//    })
//    speedRDD.foreach(x => println(s"tripID ${x._1}: ${x._2.deep}"))
//  }
//}
