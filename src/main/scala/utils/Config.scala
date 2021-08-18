package utils

import java.net.InetAddress

object Config {

  var local: Map[String, String] = {
    Map(
      "master" -> "local[*]",
      "numPartitions" -> "16",
      "hzData" -> "datasets/traj_10000_converted.json",
      "portoData" -> "preprocessing/traj_short.csv",
      "resPath" -> "out/",
      "tPartition" -> "4",
      "samplingRate" -> "0.5",
      "queryFile" -> "datasets/queries.txt"
    )
  }

  var distributed: Map[String, String] = {
    Map(
      "master" -> "spark://11.167.227.34:7077",
      "numPartitions" -> "256",
      "hzData" -> "/datasets/hz_traj/",
      "portoData" -> "/datasets/porto_traj.csv",
      "resPath" -> "/datasets/out/",
      "tPartition" -> "16",
      "samplingRate" -> "0.2",
      "queryFile" -> "/home/kaiqi.liu/st-tool/datasets/queries.txt"
    )
  }

  var aws: Map[String, String] = {
    Map(
      "master" -> "spark://master:7077",
      "numPartitions" -> "256",
      "hzData" -> "/datasets/traj_10000_converted.json",
      "portoData" -> "/datasets/porto_traj.csv",
      "resPath" -> "/datasets/out/",
      "tPartition" -> "4",
      "samplingRate" -> "0.2",
    )
  }
  var server10: Map[String, String] = {
    Map(
      "master" -> "spark://master:7077",
      "numPartitions" -> "256",
      "hzData" -> "/datasets/traj_10000_converted.json",
      "portoData" -> "/datasets/porto_traj.csv",
      "resPath" -> "/datasets/out/",
      "tPartition" -> "4",
      "samplingRate" -> "0.2",
    )
  }
  val localhost: InetAddress = InetAddress.getLocalHost
  val localIpAddress: String = localhost.getHostAddress

  def get(key: String): String = {
    if (localIpAddress contains "11.167.227.34") {
      distributed(key)
    } else if (localIpAddress contains "172.31.8.79") {
      aws(key)
    }
    else if (localIpAddress contains "192.168.107.31") {
      server10(key)
    }
    else
      local(key)
  }
}
