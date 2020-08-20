package MapMatcher

import java.io.FileWriter

import MapMatcher._
import _root_.dijkstra.{DirectedEdge, EdgeWeightedDigraph, ShortestPath}
import org.apache.spark.rdd.RDD

import scala.math._
import scala.io
import org.apache.spark.{SparkConf, SparkContext}

import Array.concat
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Map}
import _root_.dijkstra.EdgeWeightedDigraphOps._
import _root_.dijkstra.ShortestPathTest.g

import SpatialClasses._

object MapMatcher {

  def roadDist(point1: Point, point2: Point, line1: Line, line2: Line, g: EdgeWeightedDigraph, roadIDMap: Map[Int, Line]): Double = {
    // project point1 to line 1 and point2 to line 2, calculate the road distance
    var d: Double = 0
    val roadIDs = dijkstra(g, line1.id.toInt, line2.id.toInt)
    //println(line1.id.toInt, line2.id.toInt)
    //println(roadIDs.deep)
    for (id <- roadIDs) {
      val l = roadIDMap(id)
      d += greatCircleDist(l.start, l.end)
    }
    //println(d - line1.projectByPoint(point1)(0) - line2.projectByPoint(point2)(1))
    d - line1.projectByPoint(point1)(0) - line2.projectByPoint(point2)(1)
    //line1.projectByPoint(point1)(1) + line2.projectByPoint(point2)(0)
  } //TODO

  def emissionProb(z: Point, r: Line, sigmaZ: Double = 4.07): Double = {
    val d = r.dist2Point(z)
    1 / (sqrt(2 * Pi) * sigmaZ) * pow(E, -0.5 * pow(d / sigmaZ, 2))
  }

  def transitionProb(point1: Point, point2: Point, road1: Line, road2: Line, beta: Double, g: EdgeWeightedDigraph, roadIDMap: Map[Int, Line]): Double = {
    val roaddist = roadDist(point1, point2, road1, road2, g, roadIDMap)
    val dt = abs(greatCircleDist(point1, point2) - roaddist)
    if (dt > 2000 || roaddist / (point2.t - point1.t) > 50) 0
    else 1 / beta * pow(E, -dt / beta)
  }

  def initialProb(point: Point, roads: Array[Line]): Array[Double] = {
    var res = new Array[Double](0)
    for (road <- roads) res = res :+ emissionProb(point, road)
    res
  }

  def transitionProbArray(point1: Point, point2: Point, roads1: Array[Line], roads2: Array[Line], beta: Double, g: EdgeWeightedDigraph, roadIDMap: Map[Int, Line]): Array[Array[Double]] = {
    var probs = new Array[Array[Double]](0)
    for (road1 <- roads1) {
      var prob = new Array[Double](0)
      for (road2 <- roads2) {
        prob = prob :+ transitionProb(point1, point2, road1, road2, beta, g, roadIDMap)
      }
      probs = probs :+ prob
    }
    probs

  }

  def viterbi(eProbs: Array[Array[Double]], tProbs: Array[Array[Array[Double]]]): Array[Int] = {
    var biggestProb = new Array[Array[Double]](0)
    var lastState = new Array[Array[Int]](0)
    biggestProb = biggestProb :+ eProbs(0)
    lastState = lastState :+ Range(0, eProbs(0).length).toArray
    for (t <- 1 to eProbs.length - 1) { // each time step
      var prob = new Array[Double](0)
      var state = new Array[Int](0)
      for (s <- 0 to eProbs(t).length - 1) { // each possible road of t
        val lastBiggest = biggestProb(t - 1).max
        var lastBiggestIndexCandidates = new Array[Int](0)
        for (p <- 0 to biggestProb(t - 1).length - 1) {
          if (biggestProb(t - 1)(p) == lastBiggest) lastBiggestIndexCandidates = lastBiggestIndexCandidates :+ p
        }
        var probs = new Array[Double](0)
        for (c <- lastBiggestIndexCandidates) {
          probs = probs :+ eProbs(t)(s) * tProbs(t - 1)(c)(s) * eProbs(t - 1)(c)
        }
        prob = prob :+ probs.max
        //val lastBiggestIndex = biggestProb(t - 1).indexOf(lastBiggest)
        val lastBiggestIndex = lastBiggestIndexCandidates(probs.indexOf(probs.max))
        state = state :+ lastBiggestIndex
        //prob = prob :+ eProbs(t)(s) * tProbs(t - 1)(lastBiggestIndex)(s) * eProbs(t - 1)(lastBiggestIndex)
      }
      biggestProb = biggestProb :+ prob
      lastState = lastState :+ state
    }
    val t = eProbs.length - 1
    var states = new Array[Int](0)
    var v = biggestProb(t).max
    states = states :+ biggestProb(t).indexOf(v)
    for (t <- eProbs.length - 1 to 1 by -1) {
      v = biggestProb(t).max
      val i = biggestProb(t).indexOf(v)
      states = states :+ lastState(t)(i)
    }
    states.reverse
  }

  def getBestRoads(pairs: mutable.LinkedHashMap[Point, Array[Line]], g: EdgeWeightedDigraph, roadIDMap: Map[Int, Line]): Array[Line] = {
    // pairs: Map(GPS point -> candidate road segments)
    val beta = 0.2
    // val deltaZ = 4.07
    var eProbs = new Array[Array[Double]](0)
    for ((k, v) <- pairs) {
      val prob = initialProb(k, v)
      eProbs = eProbs :+ prob
    }
    var tProbs = new Array[Array[Array[Double]]](0)
    val points = pairs.keys.toArray
    for (i <- 0 to points.length - 2) {
      //println(points(i), points(i + 1), pairs(points(i)).deep, pairs(points(i + 1)).deep)
      //println(transitionProbArray(points(i), points(i + 1), pairs(points(i)), pairs(points(i + 1)), beta, g, roadIDMap).deep)
      val tProb = transitionProbArray(points(i), points(i + 1), pairs(points(i)), pairs(points(i + 1)), beta, g, roadIDMap)
      tProbs = tProbs :+ tProb
    }
    val ids = viterbi(eProbs, tProbs)
    //println(ids.deep)
    var bestRoads = new Array[Line](0)
    for (j <- 0 to ids.length - 1) {
      bestRoads = bestRoads :+ pairs(points(j))(ids(j))
    }
    bestRoads
  }


  def connectRoads(bestRoads: Array[Line], roadIDMap: Map[Int, Line], g: EdgeWeightedDigraph): Array[Line] = { //TODO
    var roads = Array(bestRoads(0))
    for (r <- 1 to bestRoads.length - 1) {
      if (bestRoads(r).start == roads.last.end) {
        //println(roads.last.id.toInt, bestRoads(r).id.toInt)
        roads = roads :+ bestRoads(r)
      }
      else if (bestRoads(r).id != roads.last.id) {
        //print("yes ")
        //println(roads.last.id.toInt, bestRoads(r).id.toInt)
        val roadIDs = dijkstra(g, roads.last.id.toInt, bestRoads(r).id.toInt)
        //println(roadIDs.deep)
        for (id <- roadIDs.drop(1)) roads = roads :+ roadIDMap(id)
      }
    }
    roads
  }

  def getCandidates(trajectory: Trajectory, roads: Array[Line]): mutable.LinkedHashMap[Point, Array[Line]] = {
    var pairs: mutable.LinkedHashMap[Point, Array[Line]] = mutable.LinkedHashMap()
    for (point <- trajectory.points) {
      var candidates = new Array[Line](0)
      for (road <- roads) { //TODO can be optimised
        if (road.dist2Point(point) <= 200) {
          candidates = candidates :+ road
        }
      }
      if (candidates.length > 0) pairs += (point -> candidates)
    }
    pairs
  }

  def hmmBreak(pairs: mutable.LinkedHashMap[Point, Array[Line]], roadIDMap: Map[Int, Line], g: EdgeWeightedDigraph): Array[mutable.LinkedHashMap[Point, Array[Line]]] = {
    // check if all probs are 0 from one time to the next
    // if so, remove the points until prob != 0
    // if time interval > 180s, break into two trajs

    val beta = 0.2
    var filteredPoints = new Array[Point](0)
    val points = pairs.keys.toArray
    var i = 0
    var breakPoints = new Array[Int](0)
    var restart = true
    while (i < points.length) {
      var point1 = points(0)
      var point2 = points(0)
      if (!restart) {
        point1 = filteredPoints.last
        point2 = points(i)
      }
      else {
        point1 = points(i)
        i += 1
        point2 = points(i)
      }
      val tProb = transitionProbArray(point1, point2, pairs(point1), pairs(point2), beta, g, roadIDMap)
      var sum: Double = 0
      for (j <- tProb) sum += j.sum
      if (sum != 0) {
        if (filteredPoints contains (point1)) filteredPoints = filteredPoints :+ point2
        else filteredPoints = filteredPoints :+ point1 :+ point2
        restart = false
      }
      else if (point2.t - point1.t > 180) {
        breakPoints = breakPoints :+ i
        restart = true
      }
      i += 1
    }
    if (breakPoints.length == 0) {
      var newPairs: mutable.LinkedHashMap[Point, Array[Line]] = mutable.LinkedHashMap()
      for (p <- filteredPoints) {
        newPairs += (p -> pairs(p))
      }
      Array(newPairs)
    }
    else {
      breakPoints = -1 +: breakPoints
      var newPairs = new Array[mutable.LinkedHashMap[Point, Array[Line]]](0)
      for (b <- 0 to breakPoints.length - 2) {
        filteredPoints = filteredPoints.drop(breakPoints(b) + 1)
        val subPoints = filteredPoints.take(breakPoints(b + 1) + 1)
        var newPair: mutable.LinkedHashMap[Point, Array[Line]] = mutable.LinkedHashMap()
        for (p <- subPoints) {
          newPair += (p -> pairs(p))
        }
        newPairs = newPairs :+ newPair
      }
      newPairs
    }
  }

  def dijkstra(g: EdgeWeightedDigraph, start: Int, end: Int): Array[Int] = {
    if (start == end) {
      Array(start, end)
    }
    else {
      val sp = ShortestPath.run(g, start)
      val path = sp.right.get.pathTo(end)
      val actualPath = path.right.get
      //print(start, end)
      //println(actualPath)
      var route = new Array[Int](0)
      for (p <- actualPath) route = route :+ p.to
      actualPath(0).from +: route
    }
  }

  def apply(trajectory: Trajectory, roadIDMap: Map[Int, Line], g: EdgeWeightedDigraph): Array[Array[Line]] = {
    val roads = roadIDMap.values.toArray
    val orgPairs = getCandidates(trajectory, roads)
    val cleanedPairs = hmmBreak(orgPairs, roadIDMap, g)
    var bestRoadsCollection = new Array[Array[Line]](0)
    for (pairs <- cleanedPairs) {
      var bestRoads = getBestRoads(pairs, g, roadIDMap)
      bestRoads = connectRoads(bestRoads, roadIDMap, g)
      for (i <- bestRoads) print(i.id.toString + "->")
      println("done")
      bestRoadsCollection = bestRoadsCollection :+ bestRoads
    }
    bestRoadsCollection
  }
}


object MapMatcherTest extends App {

  val conf = new SparkConf()
  conf.setAppName("MapMatching_v1").setMaster("local")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val numPartition = 9
  val fileName = "C:\\Users\\kaiqi001\\Map Matching\\src\\cleaned.txt"

  val trajRDD = genTrajRDD(fileName, numPartition, sc)
  //println(trajRDD.take(1).deep)

  val processedTrajRDD = trajBreak(trajRDD)
  //println(processedTrajRDD.take(1).deep)
  /*
  val roads = new Array[Line](0)
  val mapmatchedRDD = processedTrajRDD.map(traj =>{
    MapMatcher(traj, roads)
  }) // RDD[Array[Line]]
   */

  //test with fake data
  var fake_roads = new Array[Line](0)
  var bufferedSource = io.Source.fromFile("C:\\Users\\kaiqi001\\Map Matching\\src\\fake_road.txt")
  var id = 0
  for (line <- bufferedSource.getLines) {
    val cols = line.split(" ")
    fake_roads = fake_roads :+ Line(Point(cols(0).toDouble, cols(1).toDouble), Point(cols(2).toDouble, cols(3).toDouble), id)
    id += 1
    fake_roads = fake_roads :+ Line(Point(cols(2).toDouble, cols(3).toDouble), Point(cols(0).toDouble, cols(1).toDouble), id)
    id += 1

  }
  bufferedSource.close
  //println(fake_roads.deep)
  var roadIDMap: Map[Int, Line] = Map()
  for (road <- fake_roads) roadIDMap += (road.id.toInt -> road)

  def genFakeMatrix(fake_roads: Array[Line]): Array[Array[Double]] = {
    val matrix = Array.ofDim[Double](fake_roads.length, fake_roads.length)
    for (i <- 0 to fake_roads.length - 1) {
      for (j <- 0 to fake_roads.length - 1) {
        if (i != j) {
          if (fake_roads(i).end == fake_roads(j).start) matrix(i)(j) = 1 / fake_roads(i).length
        }
      }
    }
    matrix
  }

  val mtx = genFakeMatrix(fake_roads)

  val fakePoints = Array(Point(0.004, 0.00025, 0), Point(0.0017, 0.0002, 10), Point(-0.0002, 0.0007, 20), Point(0.0003, 0.0022, 30), Point(0.0011, 0.002, 40))
  val fakeTrajectory = Trajectory(0, 0, 0, fakePoints)
  val fakeTrajectoryRDD = sc.parallelize(Array(fakeTrajectory))
  findCandidate(sc.parallelize(fake_roads), fakeTrajectory, 200)
  /*
  val mapmatchedRDD = fakeTrajectoryRDD.map(traj => {
    MapMatcher(traj, fake_roads)
  })
  mapmatchedRDD.collect()
  */
  var g = EdgeWeightedDigraph()
  for (i <- 0 to mtx.length - 1) {
    for (j <- 0 to mtx.length - 1) {
      if (mtx(i)(j) != 0) g = g.addEdge(DirectedEdge(i, j, mtx(i)(j)))
    }
  }
  //println(g.adj)
  val res = MapMatcher(fakeTrajectory, roadIDMap, g)

  import java.io.PrintWriter

  /*
  val t = brokenTrajRDD.collect
  println(t.length)
  var s: String = ""
  for (x <- t) {
    s = s + x.tripID.toString + ' ' + x.taxiID.toString + ' ' + x.startTime.toString + ' '
    for (i <- x.points) {
      s = s + i.lat.formatted("%.6f") + ' ' + i.long.formatted("%.6f") + ' '
    }
    s = s + '\n'
    new FileWriter("brokenTraj.txt", true) {
      write(s);
      close
    }
  }

   */


  //val trajs = trajPreprocessing()
  /*
  // read points
  var points = new Array[Point](0)
  var bufferedSource = io.Source.fromFile("/Users/kaiqi.liu/MapMatching/traj1.txt")
  for (line <- bufferedSource.getLines) {
    val cols = line.split(" ")
    points = points :+ Point(cols(0).toDouble, cols(1).toDouble)
  }
  bufferedSource.close

   */
  /*
  val points = trajRDD.take(1).points
  // read road network
  var roads = new Array[Line](0)
  val bufferedSource = io.Source.fromFile("C:\\Users\\kaiqi001\\Map Matching\\src\\roads.txt")
  for (line <- bufferedSource.getLines) {
    val cols = line.split(" ")
    roads = roads :+ Line(Point(cols(1).toDouble, cols(0).toDouble), Point(cols(3).toDouble, cols(2).toDouble))
  }
  bufferedSource.close

  def findCandidates(point: Point, roads: Array[Line]): Array[Line] = {
    var candidates = new Array[Line](0)
    for (road <- roads) {
      if (road.dist2Point(point) <= 200) candidates = candidates :+ road
    }
    candidates
  }

  var pairs: Map[Point, Array[Line]] = Map()
  for (point <- points) {
    pairs += (point -> findCandidates(point, roads))
  }

  val matchedRoads = MapMatcher(pairs)
  for (road <- matchedRoads) println(road.start.lat, road.start.long, road.end.lat, road.end.long)

   */

  //viterbi test
  /*
  val eProbs = Array(Array(0.6, 0.4), Array(0.5, 0.1), Array(0.1, 0.6))
  val tProbs = Array(Array(Array(0.7, 0.3), Array(0.4,0.6)),Array(Array(0.7, 0.3), Array(0.4,0.6)),Array(Array(0.7, 0.3), Array(0.4,0.6)))
  val t = MapMatcher.viterbi(eProbs, tProbs)
  for(i <- t) println(i)

   */

  def findCandidate(rdd: RDD[Line], traj: Trajectory, maxDist: Double): Map[Point, Array[Line]] = {
    // find candidate for one traj
    val pairedRDD = rdd.flatMap(l => {
      var candidatePairs = new Array[(Point, Line)](0)
      val points = traj.points
      for (p <- points) {
        if (l.dist2Point(p) <= maxDist) candidatePairs = candidatePairs :+ (p,l)
      }
      candidatePairs
    }).groupByKey.map(k => (k._1, k._2.toArray)) //pairRDD with key: Point value: Array[Line]
    // rdd1.collect.foreach(x => println(x._1, x._2.deep))
    var candidateMap: Map[Point, Array[Line]] = Map()
    for(x <- pairedRDD){ candidateMap += (x._1 -> x._2)}
    candidateMap
  }

  def genCandidateRDD(trajRDD: RDD[Trajectory], roadRDD: RDD[Line], maxDist:Double = 200): RDD[(Trajectory, Map[Point, Array[Line]])] = {
    // find candidate for all trajs
    val candidateRDD = trajRDD.map(traj =>{
      (traj, findCandidate(roadRDD, traj, maxDist))
    })
    candidateRDD
  } // if generate candidates for all trajs, each time need to scan, not efficient

    // preprocessing functions
    def trajPreprocessing(fileName: String = "C:\\Users\\kaiqi001\\Map Matching\\src\\cleaned.txt"): Array[Trajectory] = {
      //scala version
      val bufferedSource = io.Source.fromFile(fileName)
      // println(bufferedSource.getLines.length)
      var trajs = new Array[Trajectory](0)
      for (line <- bufferedSource.getLines) {
        val cols = line.split(" ")
        var points = new Array[Point](0)
        for (i <- 3 to cols.length - 1 by 2) {
          points = points :+ Point(cols(i).toDouble, cols(i + 1).toDouble, cols(2).toLong + 15 * (i - 3) / 2)
        }
        val traj = Trajectory(cols(0).toLong, cols(1).toLong, cols(2).toLong, points)
        trajs = trajs :+ traj
      }
      //println(trajs(0))
      //for(i<-trajs(0).points) println(i)
      trajs
    }

    def genTrajRDD(fileName: String, numPartition: Int, sc: SparkContext): RDD[Trajectory] = {
      //spark version
      // file: txt of "tripID taxiID startTime point1.long point1.lat point2.long point2.lat ..."
      val f = sc.textFile(fileName, numPartition)
      val lines = f.map(line => line.split(' '))
      val trajRDD = lines.map(line => {
        var points = new Array[Point](0)
        for (i <- 3 to line.length - 1 by 2) {
          points = points :+ Point(line(i).toDouble, line(i + 1).toDouble, line(2).toLong + 15 * (i - 3) / 2)
        }
        Trajectory(line(0).toLong, line(1).toLong, line(2).toLong, points)
      })
      trajRDD
    }

    def splitTraj(trajectory: Trajectory, splitPoints: Array[Int]): Array[Trajectory] = {
      if (splitPoints.length == 1) Array(trajectory)
      else {
        var trajs = new Array[Trajectory](0)
        //println(splitPoints.deep)
        for (i <- 0 to splitPoints.length - 2) {
          val points = trajectory.points.take(splitPoints(i + 1) + 1).drop(splitPoints(i))
          trajs = trajs :+ Trajectory(trajectory.tripID, trajectory.taxiID, points(0).t, points)
        }
        trajs
      }
    }

    def trajBreak(trajRDD: RDD[Trajectory], speed: Double = 50, timeInterval: Double = 180): RDD[Trajectory] = {
      // speed and time interval check
      trajRDD.flatMap(traj => {
        var splitPoints = new Array[Int](0)
        for (i <- 0 to traj.points.length - 2 by 2) {
          val t = traj.points(i + 1).t - traj.points(i).t
          val l = greatCircleDist(traj.points(i + 1), traj.points(i))
          if (l / t > speed || t > timeInterval) splitPoints = splitPoints :+ i
        }
        splitTraj(traj, 0 +: splitPoints)
      }).filter(traj => traj.points.length > 1)
    }

  }
