package main.scala.mapmatching.MapMatcher

import main.scala.graph.RoadVertex
import scala.math._
import scala.collection.mutable
import main.scala.graph.RoadGraph
import main.scala.mapmatching.SpatialClasses._
import Array.concat


object MapMatcher {

  def emissionProb(d: Double, sigmaZ: Double = 4.07): Double = {
    1 / (sqrt(2 * Pi) * sigmaZ) * pow(E, -0.5 * pow(d / sigmaZ, 2))
  }

  def transitionProb(point1: Point, point2: Point, roadDist: Double, beta: Double = 0.2): Double = {
    val dt = abs(greatCircleDist(point1, point2) - roadDist)
    if (dt > 2000 || roadDist / (point2.t - point1.t) > 50) 0
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

  def getCandidates(trajectory: Trajectory, g: RoadGraph, num: Int = 5): mutable.LinkedHashMap[Point, Array[(RoadVertex, Double)]] = {
    var pairs: mutable.LinkedHashMap[Point, Array[(RoadVertex, Double)]] = mutable.LinkedHashMap()
    for (point <- trajectory.points) {
      val c = g.getNearestVertex(point.lat, point.long, num)
      pairs += (point -> c)
    }
    pairs
  }

  def getRoadDistArray(candidates: mutable.LinkedHashMap[Point, Array[(RoadVertex, Double)]], g: RoadGraph): Array[Array[Array[Double]]] = {
    var roadDistArray = new Array[Array[Array[Double]]](0)
    val roadArray = candidates.values.toArray
    for (i <- 0 to roadArray.length - 2) {
      var pairRoadDist = new Array[Array[Double]](0)
      val candidateSet1 = roadArray(i)
      val candidateSet2 = roadArray(i + 1)
      for (road1 <- candidateSet1) {
        var roadDist = new Array[Double](0)
        for (road2 <- candidateSet2) roadDist = roadDist :+ g.getShortestPathLength(road1._1.id, road2._1.id)
        pairRoadDist = pairRoadDist :+ roadDist
      }
      roadDistArray = roadDistArray :+ pairRoadDist
    }
    roadDistArray
  }

  def connectRoads(ids: Array[String], g:RoadGraph): Array[String] = {
    var roadIDs = new Array[String](0)
    for(i <- 0 to ids.length - 2){
      roadIDs = concat(roadIDs, g.getShortestPath(ids(i), ids(i+1)).get.toArray.drop(1))
    }
    roadIDs
  }

  def apply(pairs: mutable.LinkedHashMap[Point, Array[(RoadVertex, Double)]], roadDistArray: Array[Array[Array[Double]]]): Array[String] = {
    // pairs: Map(GPS point -> candidate road segments)
    val beta = 0.2
    // val deltaZ = 4.07
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
      val tProb = transitionProbArray(points(i), points(i + 1), roadDistArray(i), beta)
      tProbs = tProbs :+ tProb
    }
    val ids = viterbi(eProbs, tProbs)
    var bestRoads = new Array[String](0)
    for (j <- 0 to ids.length - 1) {
      val candidates = pairs.values.toArray
      bestRoads = bestRoads :+ candidates(j)(ids(j))._1.id
    }
    bestRoads
  }
}
