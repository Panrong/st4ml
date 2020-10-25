import json
import sys
import time

if(len(sys.argv) < 2):
 print("usage: \n  >> python parse_rqconfig.py <path to rqconfig,json>\n")
 sys.exit()
config = json.load(open(sys.argv[1]))
s = ""
s += "hdfs dfs -rm -r %s\n" %(config["resultsdir"])
s += "export HADOOP_HOME=%s\n" % config["hadoopHome"]
s += "spark-submit --class runODQuery --master %s --executor-memory %s --total-executor-cores %s --executor-cores %s\\\n" %(config["master"], config["executor-memory"], config["total-executor-cores"], config["executor-cores"])
s += "  %s %s %s %s %s %s %s\n" %(config["jarpackage"], config["sparkmaster"], config["mapfile"], config["numpartition"], config["query"], config["mmtrajfile"], config["resultsdir"])

s += "hdfs dfs -copyToLocal -f %s ./odres_%s" %(config["resultsdir"], time.strftime("%Y%m%d%H%M", time.localtime()))

with open("submit_od_job.tmp", "w") as f:
  f.write(s)
