package utils

import java.net.InetAddress

object Config {

  var local: Map[String, String] = {
    Map(
      "master" -> "local[*]",
      "numPartitions" -> "8",
      "hzData" -> "datasets/traj_10000_converted.json",
      "portoData" -> "preprocessing/traj_short.csv",
    )
  }

  var distributed: Map[String, String] = {
    Map(
      "master" -> "spark://11.167.227.34:7077",
      "numPartitions" -> "64",
      "hzData" -> "/datasets/hz_traj/",
      "portoData" -> "/datasets/porto_traj.csv",
    )
  }

  val localhost: InetAddress = InetAddress.getLocalHost
  val localIpAddress: String = localhost.getHostAddress

  def get(key: String): String = {
    if (localIpAddress contains "11.167.227.34") {
      distributed(key)
    } else {
      local(key)
    }
  }

}