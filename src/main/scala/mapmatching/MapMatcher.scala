package main.scala.mapmatching.MapMatcher

import main.scala.graph.{RoadEdge, RoadGraph}
import scala.math._
import scala.collection.mutable
import main.scala.mapmatching.SpatialClasses._
import Array.concat
import System.nanoTime

object MapMatcher {

  val timeCount = true

  def emissionProb(d: Double, sigmaZ: Double = 4.07): Double = {
    1 / (sqrt(2 * Pi) * sigmaZ) * pow(E, -0.5 * pow(d / sigmaZ, 2))
  }

  def transitionProb(point1: Point, point2: Point, roadDist: Double, beta: Double = 0.2): Double = {
    val dt = abs(greatCircleDist(point1, point2) - roadDist)
    if (dt > 2000 || roadDist / (point2.t - point1.t) > 50) {
      0
    }
    else 1 / beta * pow(E, -dt / beta)
  }

  def transitionProbArray(point1: Point, point2: Point, roadDistArray: Array[Array[Double]], beta: Double): Array[Array[Double]] = {
    var probs = new Array[Array[Double]](0)
    for (road1 <- roadDistArray) {
      var prob = new Array[Double](0)
      for (road2 <- road1) {
        prob = prob :+ transitionProb(point1, point2, road2, beta)
      }
      probs = probs :+ prob
    }
    probs

  }

  /*
      def viterbi(eProbs: Array[Array[Double]], tProbs: Array[Array[Array[Double]]]): Array[Int] = {
        val numTimeStamps = eProbs.length
        val numCandidates = eProbs.map(x=>x.length).max
        var stateProb = Array.ofDim[Double](numTimeStamps, numCandidates) // the prob of getting to a certain candidate at a certain time
        var statePath = Array.ofDim[Array[Int]](numTimeStamps, numCandidates) // the path of getting to a certain candidate at a certain time
        stateProb(0) = eProbs(0)
        for(i<-0 to numCandidates - 1) statePath(0)(i) = Array(i)
        for(t<-1 to numTimeStamps - 1){
          for(c<- 0 to numCandidates -1){
            val probs = new Array[Double](numCandidates)
            for(lastTimeStamp <- 0 to numCandidates - 1) probs(lastTimeStamp) = stateProb(t-1)(lastTimeStamp) * tProbs(t-1)(lastTimeStamp)(c)
            val bestLastCandidate = probs.indexOf(probs.max)
            stateProb(t)(c) = probs.max * eProbs(t)(c)
            statePath(t)(c) = statePath(t-1)(bestLastCandidate) :+ c
          }
        }
        val finalBestIndex = stateProb(numTimeStamps - 1).indexOf(stateProb(numTimeStamps - 1).max)
        statePath(numTimeStamps-1)(finalBestIndex)
      }
  */

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


  def getCandidates(trajectory: Trajectory, g: RoadGraph, num: Int = 5): mutable.LinkedHashMap[Point, Array[(RoadEdge, Double)]] = {
    var pairs: mutable.LinkedHashMap[Point, Array[(RoadEdge, Double)]] = mutable.LinkedHashMap()
    for (point <- trajectory.points) {
      val c = g.getNearestEdge(point.long, point.lat, num)
      pairs += (point -> c)
    }
    pairs
  }

  def getRoadDistArray(candidates: mutable.LinkedHashMap[Point, Array[(RoadEdge, Double)]], g: RoadGraph): Array[Array[Array[Double]]] = {
    var roadDistArray = new Array[Array[Array[Double]]](0)
    val roadArray = candidates.values.toArray
    for (i <- 0 to roadArray.length - 2) {
      var pairRoadDist = new Array[Array[Double]](0)
      val candidateSet1 = roadArray(i)
      val candidateSet2 = roadArray(i + 1)
      for (road1 <- candidateSet1) {
        var roadDist = new Array[Double](0)
        for (road2 <- candidateSet2) {
          val startVertex = road1._1.to
          val endVertex = road2._1.from
          roadDist = roadDist :+ g.getShortestPathAndLength(startVertex, endVertex)._2
        }
        pairRoadDist = pairRoadDist :+ roadDist
      }
      roadDistArray = roadDistArray :+ pairRoadDist
    }
    roadDistArray
  }

  def connectRoads(ids: Array[String], g: RoadGraph): Array[(String, Int)] = {
    if (ids(0) == "-1") return Array(("-1", 0))
    else {
      var vertexIDs = Array(ids(0).split("-")(0))
      for (i <- ids) {
        for (v <- i.split("-")) {
          if (v != vertexIDs.last) vertexIDs = vertexIDs :+ v
        }
      }
      var roadIDs = Array(vertexIDs(0))
      try {
        for (i <- 0 to vertexIDs.length - 2) {
          roadIDs = concat(roadIDs, g.getShortestPath(vertexIDs(i), vertexIDs(i + 1)).get.toArray.drop(1))
        }
        var res = new Array[(String, Int)](0)
        for (e <- roadIDs) {
          if (vertexIDs.contains(e)) res = res :+ (e, 1)
          else res = res :+ (e, 0)
        }
        return res
      } catch {
        case ex: NoSuchElementException => {
          return Array(("-1", 1))
        }
      }
    }
  }

  def hmmBreak(pairs: mutable.LinkedHashMap[Point, Array[(RoadEdge, Double)]], roadDistArray: Array[Array[Array[Double]]], g: RoadGraph, beta: Double): (Array[mutable.LinkedHashMap[Point, Array[(RoadEdge, Double)]]], Array[Array[Array[Array[Double]]]]) = {
    // check if all probs are 0 from one time to the next
    // if so, remove the points until prob != 0
    // if time interval > 180s, break into two trajs
    // return new pairs array(if split then have multiple pairs) and new corresponding roadDistArray(to prevent recalculation)

    /** helper function */
    def splitArray[T](xs: Array[T], sep: T): List[Array[T]] = {
      var (res, i) = (List[Array[T]](), 0)

      while (i < xs.length) {
        var j = xs.indexOf(sep, i)
        if (j == -1) j = xs.length
        if (j != i) res ::= xs.slice(i, j)
        i = j + 1
      }

      res.reverse
    }

    /** */
    var filteredPoints = new Array[Point](0)
    var filteredPointsID = new Array[Int](0)
    val points = pairs.keys.toArray
    var i = 0
    var breakPoints = new Array[Int](0)
    var restart = true
    while (i < points.length - 2) {
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
      val tProb = transitionProbArray(point1, point2, roadDistArray(i), beta)
      var sum: Double = 0
      for (j <- tProb) sum += j.sum
      if (sum != 0) {
        if (filteredPoints contains (point1)) {
          filteredPoints = filteredPoints :+ point2
          filteredPointsID = filteredPointsID :+ i
        }
        else {
          filteredPoints = filteredPoints :+ point1 :+ point2
          filteredPointsID = filteredPointsID :+ i - 1 :+ i
        }
        restart = false
      }
      else if (point2.t - point1.t > 180) {
        breakPoints = breakPoints :+ i
        filteredPointsID = filteredPointsID :+ -1
        restart = true
      }
      i += 1
    }
    //generate new transProb
    var newTransPorbArray = new Array[Array[Array[Array[Double]]]](0)
    val ids = splitArray(filteredPointsID, -1)
    for (subTraj <- ids) {
      var subTransProbArray = new Array[Array[Array[Double]]](0)
      for (p <- 0 to subTraj.length - 2) {
        if (subTraj(p + 1) - subTraj(p) == 1) subTransProbArray = subTransProbArray :+ roadDistArray(p)
        else {
          val startCandidates = points(subTraj(p))
          val endCandidates = points(subTraj(p + 1))
          var newPair: mutable.LinkedHashMap[Point, Array[(RoadEdge, Double)]] = mutable.LinkedHashMap()
          newPair += (startCandidates -> pairs(startCandidates))
          newPair += (endCandidates -> pairs(endCandidates))
          subTransProbArray = subTransProbArray :+ getRoadDistArray(newPair, g)(0)
        }
      }
      newTransPorbArray = newTransPorbArray :+ subTransProbArray
    }
    if (breakPoints.length == 0) {
      if (filteredPoints.length < 2) (new Array[mutable.LinkedHashMap[Point, Array[(RoadEdge, Double)]]](0), new Array[Array[Array[Array[Double]]]](0))
      else {
        var newPairs: mutable.LinkedHashMap[Point, Array[(RoadEdge, Double)]] = mutable.LinkedHashMap()
        for (p <- filteredPoints) {
          newPairs += (p -> pairs(p))
        }
        (Array(newPairs), newTransPorbArray)
      }
    }
    else {
      breakPoints = -1 +: breakPoints
      var newPairs = new Array[mutable.LinkedHashMap[Point, Array[(RoadEdge, Double)]]](0)
      for (b <- 0 to breakPoints.length - 2) {
        filteredPoints = filteredPoints.drop(breakPoints(b) + 1)
        val subPoints = filteredPoints.take(breakPoints(b + 1) + 1)
        var newPair: mutable.LinkedHashMap[Point, Array[(RoadEdge, Double)]] = mutable.LinkedHashMap()
        for (p <- subPoints) {
          newPair += (p -> pairs(p))
        }
        if (newPair.size >= 2) newPairs = newPairs :+ newPair
      }
      (newPairs, newTransPorbArray)
    }
  }

  def apply(p: mutable.LinkedHashMap[Point, Array[(RoadEdge, Double)]], roadDistArray: Array[Array[Array[Double]]], g: RoadGraph): (Array[Point], Array[String]) = {
    // pairs: Map(GPS point -> candidate road segments)
    val beta = 0.2
    // val deltaZ = 4.07
    var t = nanoTime
    val (cleanedPairs, newRoadDistMatrix) = hmmBreak(p, roadDistArray, g, beta)
    if (timeCount) {
      println(".... Cleaning points with graph took: " + (nanoTime - t) / 1e9d + "s")
      t = nanoTime
    }
    //println(p)
    //println(cleanedPairs(0))
    //val cleanedPairs = Array(p)
    if (cleanedPairs.size < 1) return (p.keys.toArray, Array("-1"))
    else {
      var bestRoads = new Array[String](0)
      for ((pairs, id) <- cleanedPairs.zipWithIndex) {
        val newRoadDistArray = newRoadDistMatrix(id)
        var eProbs = new Array[Array[Double]](0)
        for (i <- pairs.values) {
          var p = new Array[Double](0)
          for (j <- i) {
            p = p :+ emissionProb(j._2)
          }
          eProbs = eProbs :+ p
        }
        var tProbs = new Array[Array[Array[Double]]](0)
        val points = pairs.keys.toArray
        for (i <- 0 to points.length - 2) {
          val tProb = transitionProbArray(points(i), points(i + 1), newRoadDistArray(i), beta)
          tProbs = tProbs :+ tProb
        }
        if (timeCount) {
          println(".... Generating transition prob matrix took: " + (nanoTime - t) / 1e9d + "s")
          t = nanoTime
        }
        var ids = Array(-1)
        try {
          ids = viterbi(eProbs, tProbs)
        } catch {
          case _: Throwable => {
            println("... Map matching failed ...")
            return (points, Array("-1"))
          }
        }
        if (timeCount) {
          println(".... Viterbi algorithm took: " + (nanoTime - t) / 1e9d + "s")
          t = nanoTime
        }
        var bestRoadsP = new Array[String](0)
        for (j <- 0 to ids.length - 1) {
          val candidates = pairs.values.toArray
          bestRoadsP = bestRoadsP :+ candidates(j)(ids(j))._1.id
        }
        bestRoads = concat(bestRoads, bestRoadsP)
      }
      var cleanedPoints = new Array[Point](0)
      for (p <- cleanedPairs) cleanedPoints = concat(cleanedPoints, p.keys.toArray)
      println(bestRoads.deep)
      (cleanedPoints, bestRoads)
    }
  }
}
