package instances

import instances.GeometryImplicits.withExtraPointOps

import scala.reflect.ClassTag
import scala.math.{E, Pi, abs, max, pow, sqrt}
import geometry.road.RoadGrid
import org.locationtech.jts.geom.Coordinate

class MapMatching(fileDir: String) {

  case class RoadSeg(id: String, shape: LineString)

  type RoadNetwork = SpatialMap[LineString, String, Nothing]

  val roadGraph: RoadGrid = loadRoadGraph(fileDir)
  val roadNetwork: RoadNetwork = genRoadNetwork(roadGraph)

  // read osm files to generate a spatial map of road networks
  def loadRoadGraph(fileDir: String): RoadGrid = RoadGrid(fileDir)

  // generate the companion road network for candidate searching
  def genRoadNetwork(rg: RoadGrid): RoadNetwork = {
    val edges = rg.edges
    val coordinates = edges.map(_.ls.coordinates)
    val lineString = coordinates.map(x => LineString(x.map(c => new Coordinate(c(0), c(1)))))
    val entries = edges.map(edge => new Entry(lineString, Duration.empty, edge.id))
    new SpatialMap[LineString, String, Nothing](entries,None)
  }

  // find candidates of each point in a trajectory in a road network with a threshold,
  def findCandidate[T <: Trajectory[_, _] : ClassTag](traj: T, rn: RoadNetwork,
                                                      threshold: Double): Array[(Point, Array[RoadSeg])] = {
    val pArray = traj.entries.map(_.spatial)
    val candidates = roadNetwork.getSpatialIndexRTree(pArray, threshold)
    val roadSegs = rn.entries.map(x => RoadSeg(x.value, x.spatial))
    candidates.map(x => x.map(i => roadSegs(i)))
    pArray zip candidates.map(x => x.map(i => roadSegs(i)))
  }

  // calculate emission probability
  def calEmissionProb(p: Point, l: LineString, sigmaZ: Double): Double = {
    val d = p.greatCircle(l)
    1 / (sqrt(2 * Pi) * sigmaZ) * pow(E, -0.5 * pow(d / sigmaZ, 2))
  }

  //dijkstra shortest path, input road segment Ids and return distance in meter
  def findShortestPath(id1: String, id2: String): Double = ???

  //find transition probability of consecutive points
  def calTransitionProb(p1: Point, p2: Point, road1: RoadSeg, road2: RoadSeg, beta: Double): Double = {
    val roadDist = findShortestPath(road1.id, road2.id)
    if (road1.id == road2.id) 1 / beta * pow(E, 0)
    else {
      val dt = abs(p1.greatCircle(p2) - roadDist)
      if (dt > 2000) 0
      else 1 / beta * pow(E, -dt / beta)
    }
  }

  // calculate the emission matrix, using RoadNetwork (spatial map with rTree)
  def genEmissionMatrix[T <: Trajectory[_, _] : ClassTag](candidates: Array[(Point, Array[RoadSeg])],
                                                          sigmaZ: Double = 4.07): Array[Array[Double]] = {
    candidates.map { case (p, candidateArr) =>
      candidateArr.map(x => calEmissionProb(p, x.shape, sigmaZ))
    }
  }

  // calculate the transition matrix
  def genTransitionMatrix(candidates: Array[(Point, Array[RoadSeg])], beta: Double = 0.2): Array[Array[Array[Double]]] = ???

  // viterbi algo to find the best path, return a list of idx of each candidate group
  def viterbi(eProbs: Array[Array[Double]], tProbs: Array[Array[Array[Double]]]): Array[Int] = {
    val states = Array.ofDim[List[Int]](eProbs.length, eProbs(0).length)
    val probs = Array.ofDim[Double](eProbs.length, eProbs(0).length)
    eProbs(0).indices.foreach(i => states(0)(i) = List(i))
    probs(0) = eProbs(0)
    (1 until eProbs.length).foreach { t =>
      eProbs(t).indices.foreach { c =>
        var candiProbs = new Array[Double](0)
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

  def mapMatch[T <: Trajectory[_, _]](traj: T, candidateThresh: Double = 50,
                                      sigmaZ: Double = 4.07, beta: Double): Array[RoadSeg] = {
    val candidates = findCandidate(traj, roadNetwork, candidateThresh)
    val eMatrix = genEmissionMatrix(candidates, sigmaZ)
    val tMatrix = genTransitionMatrix(candidates, beta)
    val opimalPathIdx = viterbi(eMatrix, tMatrix)
    candidates.zip(opimalPathIdx).map(x => x._1._2(x._2))
  }
}
