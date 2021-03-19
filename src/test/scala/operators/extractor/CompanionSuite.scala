package operators.extractor

import operators.convertion.Converter
import operators.extraction.{PointCompanionExtractor, TrajCompanionExtractor}
import org.scalatest.funsuite.AnyFunSuite
import preprocessing.ReadTrajFile

import setup.SharedSparkSession

class CompanionSuite extends AnyFunSuite with SharedSparkSession {
  test("test companion") {
    val trajRDD = ReadTrajFile("preprocessing/traj_short.csv", 1000, limit = true)
    val queryRDD = ReadTrajFile("preprocessing/query.csv", 5)
      .zipWithUniqueId()
      .map{
        case(traj, id) => traj.setID(id.toString)
      }

    /** find companion by points */
    val converter = new Converter
    val pointRDD = converter.traj2Point(trajRDD.map((0, _)))
    val queryPointRDD = converter.traj2Point(queryRDD.map((0, _)))
    val extractor1 = new PointCompanionExtractor
    val queried1 = extractor1.queryWithIDs(500, 600)(pointRDD, queryPointRDD) // 500m and 10min
    val count1 = queried1.mapValues(_.distinct.length)
    val queried2 = extractor1.queryWithIDsFS(500, 600)(pointRDD, queryPointRDD)
    val count2 = queried2.mapValues(_.distinct.length)

    /** find companion by trajectory */
    val extractor2 = new TrajCompanionExtractor
    val queried3 = extractor2.queryWithIDs(500, 600)(trajRDD, queryRDD)
    val count3 = queried3.mapValues(_.distinct.length)
    val queried4 = extractor2.queryWithIDsFS(500, 600)(trajRDD, queryRDD)
    val count4 = queried4.mapValues(_.distinct.length)
    println(count1)
    println(count2)
    println(count3)
    println(count4)
    assert(count1 == count2 && count3 == count4 && count1 == count3)

  }
}