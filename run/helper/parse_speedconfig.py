import json
import sys
import time

if(len(sys.argv) < 2):
 print("usage: \n  >> python parse_rqconfig.py <path to rqconfig,json>\n")
 sys.exit()
config = json.load(open(sys.argv[1]))

if (config["mode"] == "range"): c = "runRangeSpeedQuery"
elif (config["mode"] == "id"): c = "runRoadIDSpeedQuery"
else: 
  print('Wrong mode. Please use "range" or "id".')
  sys.exit()

s = ""
s += "hdfs dfs -rm -r %s\n" %(config["resultsdir"])
s += "export HADOOP_HOME=%s\n" % config["hadoopHome"]
s += "spark-submit --class %s --master %s --executor-memory %s --total-executor-cores %s --executor-cores %s\\\n" %(c, config["master"], config["executor-memory"], config["total-executor-cores"], config["executor-cores"])
s += " %s %s %s %s %s %s %s %s\n" %(config["jarpackage"], config["sparkmaster"], config["mmtrajfile"], config["numpartition"], str(config["query"]), config["mapfile"], config["speedrange"], config["resultsdir"])

s += "hdfs dfs -copyToLocal -f %s ./speedres_%s" %(config["resultsdir"], time.strftime("%Y%m%d%H%M", time.localtime()))

with open("submit_speed_job.tmp", "w") as f:
  f.write(s)
