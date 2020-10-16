import json
import sys
import time

if(len(sys.argv) < 2):
 print("usage: \n  >> python parse_mmconfig.py <path to mmconfig,json>\n")
 sys.exit()
config = json.load(open(sys.argv[1]))
s = ""
if(config["useHDFS"]): s += "hdfs dfs -rm -r %s\n" %(config["resultsdir"])

s += "export HADOOP_HOME=%s\n" % config["hadoopHome"]
s += "spark-submit --class RunMapMatching --master %s --executor-memory %s --total-executor-cores %s --executor-cores %s\\\n" %(config["master"], config["executor-memory"], config["total-executor-cores"], config["executor-cores"])
s += "  %s %s %s %s %s %s\n" %(config["jarpackage"], config["trajfile"], config["mapfile"], config["resultsdir"], config["sparkmaster"], config["numtraj"])

if(config["useHDFS"]): s += "hdfs dfs -copyToLocal -f %s ./mmres_%s" %(config["resultsdir"], time.strftime("%Y%m%d%H%M", time.localtime()))

with open("submit_mm_job.tmp", "w") as f:
  f.write(s)
