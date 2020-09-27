import main.scala.graph.{RoadGraph, RoadGrid}
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

    /** set up Spark */
    val conf = new SparkConf()
    conf.setAppName("RangeQuery_v2").setMaster(master)
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    /** generate road vertex RDD */
    val rGrid = RoadGrid(roadGraphFile, 2)
    val rg = RoadGraph(rGrid.edges)
    /*
    var roadVertices = new Array[Point](0)
    for(p <- rg.id2vertex.values) roadVertices = roadVertices :+ Point(p.lon, p.lat, ID = p.id.toLong)
    val roadVertexRDD = sc.parallelize(roadVertices)
     */
    val gridNumOverLat = rGrid.gridNumOverLat
    //println(rg.gridNum)
    //println(rg.gridNumOverLon)
    //println(rg.gridNumOverLat)
    val gridVertexMap = rGrid.grid2Vertex.map { case (k, v) => (k.x * gridNumOverLat + k.y, v.map(p => Point(p.point.lon, p.point.lat, ID = p.id.toLong))) } // k: grid index, v: Array[Point]
    var gridVertexArray = new Array[(Int, Array[Point])](0)
    for ((k, v) <- gridVertexMap) {
      gridVertexArray = gridVertexArray :+ (k, v)
    }
    val roadVertexRDD = sc.parallelize(gridVertexArray).partitionBy(keyPartitioner(numPartitions)) //k: grid index, v: Array[Point]
    /** to see the contents of each partition */
    /*
  val pointsWithIndex = roadVertexRDD.mapPartitionsWithIndex {
    (index, partitionIterator) => {
      val partitionsMap = scala.collection.mutable.Map[Int, List[Array[Point]]]()
      var partitionList = List[Array[Point]]()
      while (partitionIterator.hasNext) {
        //println(partitionIterator.next())
        partitionList = partitionIterator.next()._2 :: partitionList
      }
      partitionsMap(index) = partitionList
      partitionsMap.iterator
    }
  }
  val r = pointsWithIndex.collect
  for(i <- r){
    println(i._1)
    for(p <- i._2)
      println(p.deep)
  }
    */
    /** range query on road vertices */
    val gridBoundary = rGrid.grids.map { case (k, v) => (k.x * gridNumOverLat + k.y, Rectangle(Point(v.bottomLeftLon, v.bottomLeftLat), Point(v.upperRightLon, v.upperRightLat), ID = (k.x * gridNumOverLat + k.y).toLong)) }
    val randRanges = genRandomQueryBoxes(Rectangle(Point(-8.6999794, 41.1000015), Point(-8.5000023, 41.2500677423077)), queryTestNum.toInt)
    val randRangesRDD = sc.parallelize(randRanges)
    val queriedGridRDD = randRangesRDD.map(x => {
      var res = new Array[Int](0)
      for (g <- gridBoundary.keys) {
        if (gridBoundary(g).intersect(x)) res = res :+ g
      }
      (x, res)
    }).flatMap(x => {
      var res = new Array[(Int, Rectangle)](0)
      for (i <- x._2) res = res :+ (i, x._1)
      res
    }).groupByKey(numPartitions).map(x => (x._1, x._2.toArray)) // key: gridID, value: Array[queryRanges]
    val resRDD = queriedGridRDD.join(roadVertexRDD).flatMap(x => {
      var res = new Array[(Rectangle, String)](0)
      val queries = x._2._1
      val vertices = x._2._2
      for (q <- queries) {
        for (v <- vertices) {
          if (v.inside(q)) res = res :+ (q, v.ID.toString)
        }
      }
      res
    }).groupByKey(numPartitions).map(x => (x._1, x._2.toArray)) //key: queryRange, value: vertex IDs

    /** map road vertex to trajectory */
    val mmTrajectoryRDD = preprocessing.readMMTrajFile(trajectoryFile).flatMap(x => {
      var pairs = new Array[( String, mmTrajectory)](0)
      for(p <- x.points) pairs = pairs :+ (p, x.asInstanceOf[mmTrajectory])
      pairs
    }).groupByKey(numPartitions).map(x => (x._1, x._2.toArray)) // key: vertexID, value:mmTrajectory
    val queries = resRDD.collect
    for(q <- queries){
      val queryRDD = sc.parallelize(q._2).map(x => (x.toString, 1))
      val finalRDD = queryRDD.join(mmTrajectoryRDD).flatMap(x=> x._2._2).map(x=>(x.tripID,x.taxiID)).distinct
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
