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
    val states = Array.ofDim[List[Int]](eProbs.length, eProbs(0).length) // records the route getting to a candidate
    val probs = Array.ofDim[Double](eProbs.length, eProbs(0).length) // records the probability getting to a candidate
    eProbs(0).indices.foreach(i => states(0)(i) = List(i)) // set initial states
    probs(0) = eProbs(0)
    (1 until eProbs.length).foreach { t => //timestamp
      eProbs(t).indices.foreach { c => //candidate
        var candiProbs = new Array[Double](0) // to find the best way to get to this candidate
        eProbs(0).indices.foreach { last =>
          val newProb = probs(t - 1)(last) * tProbs(t - 1)(last)(c) * eProbs(t)(c)
          candiProbs = candiProbs :+ newProb
        }
        val index = max(0, candiProbs.indexOf(candiProbs.max))
        probs(t)(c) = candiProbs(index)
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

  def connectRoadsAndCalSpeed(idsWPoint: Array[(String, Point)], g: RoadGraph,rGrid: RoadGrid): Array[(String, Double, Int)] = {
    // return edgeID, speed and indicator(1 or 0 for matched or inferred)\

    // check consecutive points, if belong to the same road, use average value to replace
    val compressedIdsWPoints = idsWPoint.toList.foldLeft(List[(String, Point, Int)]()) { (x, y) =>
      if (x.isEmpty || x.last._1 != y._1) x ::: List((y._1, y._2, 1))
      else {
        val l = x.last
        val toAdd = List((l._1, (l._2 * l._3 + y._2) * (1 / l._3 + 1), l._3 + 1))
        x.dropRight(1) ::: toAdd
      }
    }.map(x => (x._1, x._2)).toArray
    if (compressedIdsWPoints(0)._1 == "-1") Array(("-1", -1, -1))
    else {
      val connectedSubEdgeArray = compressedIdsWPoints.sliding(2).toArray.flatMap(x => { // x: Array(e1,e2)
        val e1 = x(0)
        val e2 = x(1)
        if (e1._1.split("-")(1) == e2._1.split("-")(0)) Array((e1._1, e1._2, 1, -1: Double))
        else {
          val (shortestPath, len) = g.getShortestPathAndLength(e1._1, e2._1)
          val s = len / (e2._2.t - e1._2.t)
          val res = (e1._1, e1._2, 1, -1:Double) +: shortestPath.drop(1).dropRight(1).toArray.map(x => {
            val p = rGrid.id2vertex(x).point
            val t = (e2._2.t - e1._2.t) * (e1._2.geoDistance(p) / len)
            (x, p.assignTimeStamp(t.toLong), 0, s)
          })
          res
        }
      }) :+ (compressedIdsWPoints.last._1, compressedIdsWPoints.last._2, 1, -1: Double) //(roadID, Point, indicator, speed) if indicator =1, speed = -1, else Point = 0
      connectedSubEdgeArray.sliding(3).toArray.map(x => {
        val e1 = x(0)
        val e2 = x(1)
        val e3 = x(2)
        if (e2._4 != -1) (e2._1, e2._4, e2._3)
        else {
          val s1 = e1._2.geoDistance(e2._2) / (e2._2.t - e1._2.t)
          val s2 = e2._2.geoDistance(e3._2) / (e3._2.t - e2._2.t)
          val s = (s1 * e2._2.geoDistance(g.id2edge(e2._1).ls.points(0)) +
            s2 * e2._2.geoDistance(g.id2edge(e2._1).ls.points.last)) /
            g.id2edge(e2._1).length
          (e2._1, s, e2._3)
        }
      })
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

  def apply(p: LinkedHashMap[Point, Array[(String, Double, Point)]], roadDistArray: Array[Array[Array[Double]]], g: RoadGrid): (Array[Point], Array[(String, Point)]) = {
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

    if (cleanedPairs.length < 1) (p.keys.toArray, Array(("-1", Point(0, 0))))
    else {
      var bestRoads = new Array[(String, Point)](0)
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
        var bestRoadsP = new Array[(String, Point)](0)
        for (j <- ids.indices) {
          val candidates = pairs.values.toArray
          bestRoadsP = bestRoadsP :+ (candidates(j)(ids(j))._1, candidates(j)(ids(j))._3)
        }
        bestRoads = concat(bestRoads, bestRoadsP) //(roadID, projectionPoint(with timestamp))
      }
      var cleanedPoints = new Array[Point](0)
      for (p <- cleanedPairs) cleanedPoints = concat(cleanedPoints, p.keys.toArray)
      (cleanedPoints, bestRoads)
    }
  }
}
