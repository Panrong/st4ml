import json
import sys
import time

if(len(sys.argv) < 2):
 print("usage: \n  >> python parse_rqconfig.py <path to rqconfig,json>\n")
 sys.exit()
config = json.load(open(sys.argv[1]))
s = ""

s += "export HADOOP_HOME=%s\n" % config["hadoopHome"]
s += "spark-submit --class RunMapMatching --master %s --executor-memory %s --total-executor-cores %s --executor-cores %s\\\n" %(config["master"], config["executor-memory"], config["total-executor-cores"], config["executor-cores"])
s += "  %s %s %s %s %s %s %s\n" %(config["jarpackage"], config["mmtrajfile"], config["numpartition"], config["rtreecapacity"], config["query"], config["mapfile"], config["gridsize"])


with open("submit_rq_job.tmp", "w") as f:
  f.write(s)
