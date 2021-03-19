package fileReading

import org.scalatest.funsuite.AnyFunSuite
import preprocessing._
import setup.SharedSparkSession


class FileReadingSuite extends AnyFunSuite with SharedSparkSession {
  test("test reading point file") {
    val fileName = "datasets/cams.json"
    val res = ReadPointFile(fileName)
    println(res.take(5).deep)
  }

  test("test reading trajectory file") {
    val trajRDD = ReadTrajFile("preprocessing/traj_short.csv", 1000)
    trajRDD.take(5).foreach(println(_))
  }

  test("test reading trajectory json") {
    val trajRDD = ReadTrajJson("datasets/traj_100000_converted.json", 4)
    println(trajRDD.count)
    println(trajRDD.take(5).deep)
  }
}
