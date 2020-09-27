package main.scala.mapmatching

import main.scala.graph.{RoadGraph, RoadGrid}
import scala.math.{sqrt, pow, abs, Pi, E, max}
import scala.collection.mutable.LinkedHashMap
import main.scala.geometry.{Point, Trajectory}
import main.scala.geometry.Distances.greatCircleDistance

import Array.concat
import System.nanoTime

object MapMatcher {

  val timeCount = true

  def emissionProb(d: Double, sigmaZ: Double = 4.07): Double = {
    1 / (sqrt(2 * Pi) * sigmaZ) * pow(E, -0.5 * pow(d / sigmaZ, 2))
  }

  def transitionProb(point1: Point, point2: Point, roadDist: Double, beta: Double = 0.2): Double = {
    if (roadDist == 0) 1 / beta * pow(E, 0)
    else {
      val dt = abs(greatCircleDistance(point1, point2) - roadDist)
      if (dt > 2000 || roadDist / (point2.t - point1.t) > 50) {
        0
      }
      else 1 / beta * pow(E, -dt / beta)
    }
  }

  def transitionProbArray(point1: Point, point2: Point, roadDistArray: Array[Array[Double]],
                          beta: Double): Array[Array[Double]] = roadDistArray.map(_.map(transitionProb(point1, point2, _, beta)))


  def viterbi(eProbs: Array[Array[Double]], tProbs: Array[Array[Array[Double]]]): Array[Int] = {
    //    println("--------------eProbs:")
    //    println("(" + eProbs.size + "," + eProbs(0).length + ")")
    var states = Array.ofDim[List[Int]](eProbs.length, eProbs(0).length) // records the route getting to a candidate
    var probs = Array.ofDim[Double](eProbs.length, eProbs(0).length) // records the probability getting to a candidate
    eProbs(0).indices.foreach(i => states(0)(i) = List(i)) // set initial states
    probs(0) = eProbs(0)
    //    println(states.size)
    //    println(probs.size)
    (1 until eProbs.length).foreach { t => //timestamp
      eProbs(t).indices.foreach { c => //candidate
        //        println(t, c)
        var candiProbs = new Array[Double](0) // to find the best way to get to this candidate
        eProbs(0).indices.foreach { last =>
          val newProb = probs(t - 1)(last) * tProbs(t - 1)(last)(c) * eProbs(t)(c)
          //          if(newProb.isNaN) println(probs(t - 1)(last) , tProbs(t - 1)(last)(c) , eProbs(t)(c))
          candiProbs = candiProbs :+ newProb
        }
        val index = max(0, candiProbs.indexOf(candiProbs.max))
        //        println("000")
        //        println(candiProbs.deep, index)
        probs(t)(c) = candiProbs(index)
        //        println("!!!")
        //        println(eProbs(0).length)
        //        println(candiProbs.size)
        //        println(probs.size)
        //        println(index)
        //        println(states(t-1)(index))
        states(t)(c) = c :: states(t - 1)(index)
      }
    }
    val finalProb = probs(eProbs.length - 1)
    val maxLastCandidate = finalProb.indexOf(finalProb.max)
    states(eProbs.length - 1)(maxLastCandidate).reverse.toArray
  }


  def getCandidates(trajectory: Trajectory, rGird: RoadGrid, num: Int = 5): LinkedHashMap[Point, Array[(String, Double, Point)]] = {
    var candidates: LinkedHashMap[Point, Array[(String, Double, Point)]] = LinkedHashMap()
    trajectory.points.map(x => (x, rGird.getNearestEdge(x, num))).foreach(
      x => candidates += (x._1 -> x._2)
    )
    candidates
  }

  def getRoadDistArray(candidates: LinkedHashMap[Point, Array[(String, Double, Point)]],
                       rGrid: RoadGrid): Array[Array[Array[Double]]] = {
    val roadArray = candidates.values.toArray

    val candiSetPairs = roadArray.sliding(2).toArray
    val roadDist = candiSetPairs.map { candiSetPair =>
      val pairs = candiSetPair(0).map(x => candiSetPair(1).map(y => (x, y)))
      pairs.map(pair =>
        pair.map { case (x, y) =>
          if (x == y) x._3.geoDistance(y._3)
          else {
            val startVertex = x._1.split("-")(1) // to vertex of the src edge
            val endVertex = y._1.split("-")(0) // from vertex of the dst edge
            val p0 = rGrid.id2vertex(startVertex).point
            val p1 = rGrid.id2vertex(endVertex).point
            val edges = rGrid.getGraphEdgesByPoint(p0, p1)
            val rGraph = RoadGraph(edges)
            rGraph.getShortestPathAndLength(startVertex, endVertex)._2 + x._3.geoDistance(p0) + y._3.geoDistance(p1)
          }
        })
    }
    roadDist
  }

  def connectRoads(ids: Array[String], g: RoadGraph): Array[(String, Int)] = {
    if (ids(0) == "-1") return Array(("-1", -1))
    else {

      //      val vertexIDs = ids.flatMap(_.split("-")).sliding(2).collect {
      //        case Array(a, b) if a != b => b
      //      }.toArray

      // remove consecutive duplicates
      val vertexIDs = ids(0) +: ids.flatMap(_.split("-")).sliding(2).collect {
        case Array(a, b) if a != b => b
      }.toArray


      //      var roadIDs = Array(vertexIDs(0))
      try {
        //        for (i <- 0 to vertexIDs.length - 2) {
        //          roadIDs = concat(roadIDs, g.getShortestPath(vertexIDs(i), vertexIDs(i + 1)).toArray.drop(1))
        //        }
        //        var res = new Array[(String, Int)](0)
        //        for (e <- roadIDs) {
        //          if (vertexIDs.contains(e)) res = res :+ (e, 1)
        //          else res = res :+ (e, 0)
        //        }

        val filledVertexIDs = vertexIDs.sliding(2).toArray.map {
          case Array(oVertex, dVertex) =>
            if (g.hasEdge(oVertex, dVertex)) Array((oVertex, 1), (dVertex, 1))
            else g.getShortestPath(oVertex, dVertex).toArray.map(x => (x, 0))
        }

        val filledVertexIDsFlatten = filledVertexIDs.flatten
        // remove consecutive duplicates
        filledVertexIDsFlatten(0) +: filledVertexIDsFlatten.sliding(2).collect {
          case Array(a, b) if a._1 != b._1 => b
        }.toArray

      } catch {
        case ex: NoSuchElementException => {
          return Array(("-1", -1))
        }
      }
    }
  }

  def hmmBreak(pairs: LinkedHashMap[Point, Array[(String, Double, Point)]],
               roadDistArray: Array[Array[Array[Double]]],
               g: RoadGrid, beta: Double): (Array[LinkedHashMap[Point,
    Array[(String, Double, main.scala.geometry.Point)]]], Array[Array[Array[Array[Double]]]]) = {
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
          var newPair: LinkedHashMap[Point, Array[(String, Double, Point)]] = LinkedHashMap()
          newPair += (startCandidates -> pairs(startCandidates))
          newPair += (endCandidates -> pairs(endCandidates))
          subTransProbArray = subTransProbArray :+ getRoadDistArray(newPair, g)(0)
        }
      }
      newTransPorbArray = newTransPorbArray :+ subTransProbArray
    }
    if (breakPoints.length == 0) {
      if (filteredPoints.length < 2) (new Array[LinkedHashMap[Point, Array[(String, Double, Point)]]](0), new Array[Array[Array[Array[Double]]]](0))
      else {
        var newPairs: LinkedHashMap[Point, Array[(String, Double, Point)]] = LinkedHashMap()
        for (p <- filteredPoints) {
          newPairs += (p -> pairs(p))
        }
        (Array(newPairs), newTransPorbArray)
      }
    }
    else {
      breakPoints = -1 +: breakPoints
      var newPairs = new Array[LinkedHashMap[Point, Array[(String, Double, main.scala.geometry.Point)]]](0)
      for (b <- 0 to breakPoints.length - 2) {
        filteredPoints = filteredPoints.drop(breakPoints(b) + 1)
        val subPoints = filteredPoints.take(breakPoints(b + 1) + 1)
        var newPair: LinkedHashMap[Point, Array[(String, Double, main.scala.geometry.Point)]] = LinkedHashMap()
        for (p <- subPoints) {
          newPair += (p -> pairs(p))
        }
        if (newPair.size >= 2) newPairs = newPairs :+ newPair
      }
      (newPairs, newTransPorbArray)
    }
  }

  def apply(p: LinkedHashMap[Point, Array[(String, Double, Point)]], roadDistArray: Array[Array[Array[Double]]], g: RoadGrid): (Array[Point], Array[String]) = {
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
    //val newRoadDistMatrix = Array(roadDistArray)

    if (cleanedPairs.size < 1) (p.keys.toArray, Array("-1"))
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
        ids = viterbi(eProbs, tProbs)
        //        try {
        //          ids = viterbi(eProbs, tProbs)
        //        } catch {
        //          case _: Throwable => {
        //            println("... Map matching failed ...")
        //            return (points, Array("-1"))
        //          }
        //        }
        if (timeCount) {
          println(".... Viterbi algorithm took: " + (nanoTime - t) / 1e9d + "s")
          t = nanoTime
        }
        var bestRoadsP = new Array[String](0)
        for (j <- 0 to ids.length - 1) {
          val candidates = pairs.values.toArray
          bestRoadsP = bestRoadsP :+ candidates(j)(ids(j))._1
        }
        bestRoads = concat(bestRoads, bestRoadsP)
      }
      var cleanedPoints = new Array[Point](0)
      for (p <- cleanedPairs) cleanedPoints = concat(cleanedPoints, p.keys.toArray)
      (cleanedPoints, bestRoads)
    }
  }
}
