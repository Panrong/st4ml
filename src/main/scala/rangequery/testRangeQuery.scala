import main.scala.graph.{RoadGrid, RoadVertex}
import main.scala.geometry._
import org.apache.spark.{SparkConf, SparkContext}
import main.scala.rangequery._
import main.scala.mapmatching.preprocessing

object testRangeQuery extends App {
  override def main(args: Array[String]): Unit = {
    /** input arguments */
    val master = args(0)
    val roadGraphFile = args(1)
    val numPartitions = args(2).toInt
    val queryTestNum = args(3)
    val trajectoryFile = args(4) //map-matched
    val gridSize = args(5).toDouble

    /** set up Spark */
    val conf = new SparkConf()
    conf.setAppName("RangeQuery_v2").setMaster(master)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    /** generate road vertex RDD */
    val rGrid = RoadGrid(roadGraphFile, gridSize)
    /*
    var roadVertices = new Array[Point](0)
    for(p <- rg.id2vertex.values) roadVertices = roadVertices :+ Point(p.lon, p.lat, ID = p.id.toLong)
    val roadVertexRDD = sc.parallelize(roadVertices)
     */
    val gridNumOverLat = rGrid.gridNumOverLat
    println("total grid: "+rGrid.gridNum)
    //println(rg.gridNumOverLon)

    //println(rg.gridNumOverLat)
    val gridVertexArray = rGrid.grid2Vertex.map{case(k,v) => (k.x * gridNumOverLat + k.y, v.map(x=>x.id))}.toArray // Array[grid index in single integer, Array[roadVertex]]
    val roadVertexRDD = sc.parallelize(gridVertexArray).partitionBy(keyPartitioner(numPartitions)) //k: grid index, v: Array[roadVertex]
    //println("roadVertexRDD sample: ")
    //roadVertexRDD.take(2).foreach(x=>println(x._1, x._2.deep))
    /** range query on road vertices */
    val gridBoundary = rGrid.grids.map { case (k, v) => (k.x * gridNumOverLat + k.y, Rectangle(Point(v.bottomLeftLon, v.bottomLeftLat), Point(v.upperRightLon, v.upperRightLat)))}.toArray
    val gridRDD = sc.parallelize(gridBoundary).partitionBy(keyPartitioner(numPartitions))
    val randRanges = genRandomQueryBoxes(Rectangle(Point(-8.6999794, 41.1000015), Point(-8.5000023, 41.2500677423077)), queryTestNum.toInt)
    //println(roadVertexRDD.count)
    val rangeRDD = sc.parallelize(randRanges)
    val queriedGridRDD = rangeRDD.cartesian(gridRDD).filter(x=> x._1.intersect(x._2._2)).map(x=> (x._2._1, x._1)).groupByKey().flatMapValues(_.toList) // key: gridID, value: queryRanges

    val resRDD = queriedGridRDD.join(roadVertexRDD).map(x=>x._2) //(range, Array[vertex])
    /** map road vertex to trajectory */
    val mmTrajectoryRDD = preprocessing.readMMTrajFile(trajectoryFile).flatMap(x => {
      var pairs = new Array[( String, String)](0)
      for(p <- x.points) pairs = pairs :+ (p, x.tripID)
      pairs
    }).groupByKey(numPartitions).map(x => (x._1, x._2.toArray)) // key: vertexID, value:mmTrajectoryID
    mmTrajectoryRDD.take(1).foreach(x=>println(x._1, x._2.deep))
    val queries = resRDD.collect
    println(queries(0)._1, queries(0)._2.deep)

    for(q <- queries){
      val queryRDD = sc.parallelize(q._2).map(x => (x, 1))
      val finalRDD = queryRDD.join(mmTrajectoryRDD).flatMap(x=> x._2._2)
      println("range: "+q._1+" trajectories: "+finalRDD.count)
    }
    sc.stop()
  }
  def genRandomQueryBoxes(r: Rectangle, n: Int): Array[Rectangle] = {
    var rectangles = new Array[Rectangle](0)
    val rdm = new scala.util.Random(5)
    for (i <- 0 to n) {
      var two = (rdm.nextDouble, rdm.nextDouble)
      if (two._1 > two._2) two = two.swap
      val (x_min, x_max) = two
      two = (rdm.nextDouble, rdm.nextDouble)
      if (two._1 > two._2) two = two.swap
      val (y_min, y_max) = two
      rectangles = rectangles :+ Rectangle(Point(x_min * (r.x_max - r.x_min) + r.x_min, y_min * (r.y_max - r.y_min) + r.y_min), Point(x_max * (r.x_max - r.x_min) + r.x_min, y_max * (r.y_max - r.y_min) + r.y_min))
    }
    rectangles
  }
}
